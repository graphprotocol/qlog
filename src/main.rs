extern crate clap;
extern crate regex;
#[macro_use]
extern crate lazy_static;
extern crate serde;
extern crate serde_json;
extern crate walkdir;

use clap::{App, AppSettings, ArgMatches, SubCommand};
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::{Duration, Instant};

use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

/// Queries that take longer than this (in ms) are considered slow
const SLOW_THRESHOLD: u64 = 1000;

/// When a log line contains this text, we know it's about a GraphQL
/// query
const GQL_MARKER: &str = "Query timing (GraphQL)";

/// When a log line contains this text, we know it's about a SQL
/// query
const SQL_MARKER: &str = "Query timing (SQL)";

/// StackDriver prefixes lines with this when they were too long, and then
/// shortens the line
const TRIMMED: &str = "[Trimmed]";

lazy_static! {
    /// The regexp we use to extract data about GraphQL queries from log files
    static ref GQL_QUERY_RE: Regex = Regex::new(
        " Query timing \\(GraphQL\\), query_time_ms: (?P<time>[0-9]+), \
        (?:variables: (?P<vars>\\{.*\\}|null), )?\
         query: (?P<query>.*) , \
         query_id: (?P<qid>[0-9a-f-]+), \
         subgraph_id: (?P<sid>[a-zA-Z0-9]*), \
         component: "
    )
    .unwrap();

    /// The regexp we use to extract data about SQL queries from the log files.
    /// This will only match lines that were emitted during GraphQL execution
    /// since it looks for `query_id` and `subgraph_id`; it will intentionally
    /// not match the lines emitted by things like `store.get` which don't
    /// have those two fields
    static ref SQL_QUERY_RE: Regex = Regex::new(
        " Query timing \\(SQL\\), \
          entity_count: [0-9]+, \
          time_ms: (?P<time>[0-9]+), \
          query: (?P<query>.*) -- \
          binds: (?P<binds>.*), \
          query_id: (?P<qid>[0-9a-f-]+), \
          subgraph_id: (?P<sid>[a-zA-Z0-9]*), \
          component: "
    )
    .unwrap();

    /// The regexp for finding arguments in GraphQL queries. This intentionally
    /// doesn't cover all possible GraphQL values, only numbers and strings
    static ref VAR_RE: Regex =
        Regex::new("([_A-Za-z][_0-9A-Za-z]*): *([0-9]+|\"[^\"]*\")").unwrap();
}

pub fn die(msg: &str) -> ! {
    println!("{}", msg);
    std::process::exit(1);
}

/// The statistics we maintain about each query; we keep queries unique
/// by `(query, subgraph)`
///
/// Changes to this data structure require that the summary files get
/// regenerated from the processed log files by running `qlog process`
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueryInfo {
    query: String,
    subgraph: String,
    /// The total time (in ms) spend on this query
    total_time: u64,
    /// The sum of query times squared, for computing standard deviation
    time_squared: u64,
    /// The longest a single instance of the query took
    max_time: u64,
    /// The UUID of the slowest query; this helps in finding that query
    /// in the logfile
    max_uuid: String,
    /// The variables used in the slowest query
    max_variables: Option<String>,
    /// The number of times this query took longer than `SLOW_THRESHOLD`
    slow_count: u64,
    /// The number of times the query has been run
    calls: u64,
    /// An ID to make it easier to refer to the query for the user
    id: usize,
}

impl QueryInfo {
    fn new(query: String, subgraph: String, id: usize) -> QueryInfo {
        QueryInfo {
            query,
            subgraph,
            id,
            total_time: 0,
            time_squared: 0,
            max_time: 0,
            max_uuid: "(none)".to_owned(),
            max_variables: None,
            slow_count: 0,
            calls: 0,
        }
    }

    fn add(&mut self, time: u64, query_id: &str, variables: Option<Cow<'_, str>>) {
        self.calls += 1;
        self.total_time += time;
        self.time_squared += time * time;
        if time > self.max_time {
            self.max_time = time;
            self.max_uuid = query_id.to_owned();
            self.max_variables = variables.map(|vars| vars.into_owned());
        }
        if time > SLOW_THRESHOLD {
            self.slow_count += 1;
        }
    }

    fn avg(&self) -> f64 {
        self.total_time as f64 / self.calls as f64
    }

    fn variance(&self) -> f64 {
        let avg = self.avg();
        let calls = self.calls as f64;
        let time_squared = self.time_squared as f64;
        time_squared / calls - avg * avg
    }

    fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }

    fn combine(&mut self, other: &QueryInfo) {
        self.calls += other.calls;
        self.total_time += other.total_time;
        self.time_squared += other.time_squared;
        if other.max_time > self.max_time {
            self.max_time = other.max_time;
            self.max_uuid = other.max_uuid.clone();
            self.max_variables = other.max_variables.clone();
        }
        self.slow_count += other.slow_count;
    }

    /// A hash value that can be calculated without constructing
    /// a `QueryInfo`
    fn hash(query: &str, subgraph: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        (query, subgraph).hash(&mut hasher);
        hasher.finish()
    }
}

fn field<'a>(caps: &'a Captures, group: &str) -> Option<&'a str> {
    caps.name(group).map(|field| field.as_str())
}

fn add_entry(
    queries: &mut BTreeMap<u64, QueryInfo>,
    query_time: &str,
    query_id: &str,
    query: Cow<'_, str>,
    variables: Option<Cow<'_, str>>,
    subgraph: &str,
) -> Result<(), ()> {
    let query_time: u64 = match query_time.parse() {
        Err(_) => return Err(()),
        Ok(qt) => qt,
    };
    let hsh = QueryInfo::hash(&query, &subgraph);
    let count = queries.len();
    let info = queries
        .entry(hsh)
        .or_insert_with(|| QueryInfo::new(query.into_owned(), subgraph.to_owned(), count + 1));
    info.add(query_time, &query_id, variables);
    Ok(())
}

/// The heart of the `process` subcommand. Expects a logfile containing
/// query logs on the command line.
fn process(print_extra: bool) -> Result<(Vec<QueryInfo>, Vec<QueryInfo>), std::io::Error> {
    // Read the file line by line using the lines() iterator from std::io::BufRead.
    let mut gql_queries: BTreeMap<u64, QueryInfo> = BTreeMap::default();
    let mut sql_queries: BTreeMap<u64, QueryInfo> = BTreeMap::default();

    /// Canonicalize queries so that queries that only differ in argument
    /// values are considered equal and summarized together. We do this by
    /// looking for arguments and turning something like
    /// `things(where: { color: "blue" })` into
    /// `things(where: { color: $color })`. This is mostly based on
    /// heuristics since we don't want to fully parse GraphQL to keep
    /// logfile processing reasonably fast.
    ///
    /// Returns the rewritten query and a string that contains the
    /// variable values in JSON form (`{ color: "blue" }`)
    fn canonicalize<'a>(query: &'a str, vars: Option<&str>) -> (Cow<'a, str>, Option<String>) {
        // If the query had explicit variables, just use those, don't try
        // to guess and extract them
        if let Some(vars) = vars {
            if vars.len() > 0 && vars != "{}" && vars != "null" {
                return (Cow::from(query), Some(vars.to_owned()));
            }
        }

        if VAR_RE.is_match(query) {
            let mut vars = String::new();
            write!(&mut vars, "{{ ").unwrap();
            let mut count = 0;
            let query = VAR_RE.replace_all(query, |caps: &Captures| {
                if count > 0 {
                    write!(&mut vars, ", ").unwrap();
                }

                count += 1;
                write!(vars, "{}{}: {}", &caps[1], count, &caps[2]).unwrap();
                format!("{}: ${}{}", &caps[1], &caps[1], count)
            });
            write!(vars, " }}").unwrap();
            (query, Some(vars))
        } else {
            (Cow::from(query), None)
        }
    }

    let start = Instant::now();
    let mut gql_lines: usize = 0;
    let mut sql_lines: usize = 0;
    let mut mtch = Duration::from_secs(0);
    for line in io::stdin().lock().lines() {
        let line = line?;

        let mtch_start = Instant::now();
        if let Some(caps) = GQL_QUERY_RE.captures(&line) {
            mtch += mtch_start.elapsed();
            gql_lines += 1;
            if let (Some(query_time), Some(query), Some(query_id), Some(subgraph)) = (
                field(&caps, "time"),
                field(&caps, "query"),
                field(&caps, "qid"),
                field(&caps, "sid"),
            ) {
                let (query, variables) = canonicalize(query, field(&caps, "vars"));
                add_entry(
                    &mut gql_queries,
                    query_time,
                    query_id,
                    query,
                    variables.map(|vars| Cow::from(vars)),
                    subgraph,
                )
                .unwrap_or_else(|_| eprintln!("not a query: {}", line));
            }
        } else if let Some(caps) = SQL_QUERY_RE.captures(&line) {
            mtch += mtch_start.elapsed();
            sql_lines += 1;
            if let (Some(time), Some(query), Some(binds), Some(qid), Some(sid)) = (
                field(&caps, "time"),
                field(&caps, "query"),
                field(&caps, "binds"),
                field(&caps, "qid"),
                field(&caps, "sid"),
            ) {
                add_entry(
                    &mut sql_queries,
                    time,
                    qid,
                    Cow::from(query),
                    Some(Cow::from(binds)),
                    sid,
                )
                .unwrap_or_else(|_| eprintln!("not a query: {}", line));
            }
        } else if print_extra {
            eprintln!("not a query: {}", line);
        }
    }
    eprintln!(
        "Processed {} GraphQL and {} SQL queries in {:.3}s (regexp match: {:.3}s)",
        gql_lines,
        sql_lines,
        start.elapsed().as_secs_f64(),
        mtch.as_secs_f64(),
    );
    Ok((
        gql_queries.values().cloned().collect(),
        sql_queries.values().cloned().collect(),
    ))
}

/// Read a list of summaries from `filename` The file must be in
/// 'JSON lines' format, i.e., with one JSON object per line
fn read_summaries(filename: &str) -> Result<Vec<QueryInfo>, std::io::Error> {
    let file = std::fs::File::open(filename)?;
    let reader = BufReader::new(file);
    let mut infos = vec![];
    for line in reader.lines() {
        infos.push(serde_json::from_str(&line?)?);
    }
    Ok(infos)
}

fn buf_writer(filename: &str) -> BufWriter<File> {
    match File::create(filename) {
        Ok(file) => BufWriter::new(file),
        Err(e) => die(&format!("failed to open `{}`: {}", filename, e)),
    }
}

/// Write a list of summaries to stdout; the list will be written in
/// 'JSON lines' format
fn write_summaries(writer: &mut dyn Write, infos: Vec<QueryInfo>) -> Result<(), std::io::Error> {
    for info in infos {
        let json = serde_json::to_string(&info).unwrap_or_else(|err| {
            die(&format!(
                "failed to convert summary to json: {}",
                err.to_string()
            ))
        });
        write!(writer, "{}\n", json)?;
    }
    Ok(())
}

/// The 'stats' subcommand
fn print_stats(mut queries: Vec<QueryInfo>, sort: &str) {
    let sort = sort.chars().next().unwrap_or('t');
    queries.sort_by(|a, b| {
        let ord = match sort {
            'c' => a.calls.cmp(&b.calls),
            'a' => a.avg().partial_cmp(&b.avg()).unwrap(),
            'm' => a.max_time.cmp(&b.max_time),
            's' => a.slow_count.cmp(&b.slow_count),
            'u' => a.max_uuid.cmp(&b.max_uuid),
            _ => a.total_time.cmp(&b.total_time),
        };
        ord.reverse()
    });
    // Use writeln! instead of println! so we do not get a panic on
    // SIGPIPE if the output is piped into e.g. head -n 1
    let mut stdout = io::stdout();
    #[allow(unused_must_use)]
    {
        writeln!(
            stdout,
            "| {:^7} | {:^8} | {:^12} | {:^6} | {:^6} | {:^6} | {:^6} |",
            "QID", "calls", "total", "avg", "stddev", "max", "slow"
        );
        writeln!(
            stdout,
            "|---------+----------+--------------+--------+--------+--------+--------|"
        );
    }
    for query in &queries {
        #[allow(unused_must_use)]
        {
            writeln!(
                stdout,
                "| Q{:0>6} | {:>8} | {:>12} | {:>6.0} | {:>6.0} | {:>6} | {:>6} |",
                query.id,
                query.calls,
                query.total_time,
                query.avg(),
                query.stddev(),
                query.max_time,
                query.slow_count
            );
        }
    }
}

/// The 'extract' subcommand turning a StackDriver logfile into a plain
/// textual logfile by pulling out the 'textPayload' for each entry
fn extract(
    dir: &str,
    gql: &mut dyn Write,
    sql: &mut dyn Write,
    verbose: bool,
) -> Result<(), std::io::Error> {
    let json_ext = OsStr::new("json");
    let mut stdout = io::stdout();
    let mut trimmed_count: usize = 0;
    let mut count: usize = 0;

    for entry in WalkDir::new(dir) {
        let entry = entry?;

        if entry.file_type().is_file() && entry.path().extension() == Some(&json_ext) {
            use serde_json::Value;

            if verbose {
                eprintln!("Reading {}", entry.path().to_string_lossy());
            }
            let file = File::open(entry.path())?;
            let reader = BufReader::new(file);

            // Going line by line is much faster than using
            // serde_json::Deserializer::from_reader(reader).into_iter();
            for line in reader.lines() {
                count += 1;
                if let Value::Object(map) = serde_json::from_str(&line?)? {
                    if let Some(Value::String(text)) = map.get("textPayload") {
                        let res = if text.contains(TRIMMED) {
                            trimmed_count += 1;
                            Ok(0)
                        } else if text.contains(SQL_MARKER) {
                            sql.write(text.as_bytes())
                        } else if text.contains(GQL_MARKER) {
                            gql.write(text.as_bytes())
                        } else {
                            stdout.write(text.as_bytes())
                        };
                        if let Err(e) = res {
                            if e.kind() == std::io::ErrorKind::BrokenPipe {
                                return Ok(());
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!(
        "Skipped {} trimmed lines out of {} lines",
        trimmed_count, count
    );
    Ok(())
}

/// The 'combine' subcommand. Reads summaries from 'filenames' and prints
/// the summary resulting from combining all those summaries
fn combine(filenames: Vec<&str>) -> Vec<QueryInfo> {
    let mut infos: BTreeMap<u64, QueryInfo> = BTreeMap::default();
    for filename in filenames {
        for info in read_summaries(filename).unwrap_or_else(|err| {
            die(&format!(
                "combine: could not read summaries from {}: {}",
                filename,
                err.to_string()
            ))
        }) {
            let hsh = QueryInfo::hash(&info.query, &info.subgraph);
            infos
                .entry(hsh)
                .and_modify(|existing| existing.combine(&info))
                .or_insert(info);
        }
    }
    for (indx, value) in infos.values_mut().enumerate() {
        value.id = indx;
    }
    infos.values().cloned().collect()
}

/// The 'queries' subcommand
fn print_queries(filename: &str, queries: Vec<&str>) -> Result<(), std::io::Error> {
    fn human_readable_time(time: u64) -> (f64, &'static str) {
        const SECS_PER_MINUTE: u64 = 60;
        const SECS_PER_HOUR: u64 = 60 * SECS_PER_MINUTE;
        const SECS_PER_DAY: u64 = 24 * SECS_PER_HOUR;

        let time = Duration::from_millis(time);
        if time > Duration::from_secs(SECS_PER_DAY) {
            (time.as_secs_f64() / SECS_PER_DAY as f64, "days")
        } else if time > Duration::from_secs(2 * SECS_PER_HOUR) {
            (time.as_secs_f64() / SECS_PER_HOUR as f64, "h")
        } else if time > Duration::from_secs(5 * SECS_PER_MINUTE) {
            (time.as_secs_f64() / SECS_PER_MINUTE as f64, "m")
        } else if time > Duration::from_secs(10) {
            (time.as_secs_f64(), "s")
        } else {
            (time.as_millis() as f64, "ms")
        }
    }

    let infos = read_summaries(filename)?;
    for (count, query) in queries.iter().enumerate() {
        if query.starts_with("Q") {
            let qid: usize = match query[1..].parse() {
                Err(_) => {
                    eprintln!("skipping invalid query identifier {}", query);
                    continue;
                }
                Ok(qid) => qid,
            };
            if let Some(info) = infos.iter().find(|info| info.id == qid) {
                if count > 0 {
                    println!("");
                }
                println!("{:=<32} Q{} {:=<32}", "", info.id, "");
                println!("# subgraph:      {}", info.subgraph);
                println!("# calls:           {:>12}", info.calls);
                println!("# slow_count:      {:>12}", info.slow_count);
                println!(
                    "# slow_percent:    {:>12.2} %",
                    info.slow_count as f64 * 100.0 / info.calls as f64
                );
                let (amount, unit) = human_readable_time(info.total_time);
                println!("# total_time:      {:>12.1} {}", amount, unit);
                println!("# avg_time:        {:>12.0} ms", info.avg());
                println!("# stddev_time:     {:>12.0} ms", info.stddev());
                println!("# max_time:        {:>12} ms", info.max_time);
                println!("# max_uuid:      {}", info.max_uuid);
                if let Some(max_vars) = &info.max_variables {
                    println!("# max_variables: {}", max_vars);
                }
                println!("\n{}", info.query);
            }
        }
    }
    Ok(())
}

fn main() {
    let args = App::new("qlog")
        .version("1.0")
        .about("Analyze graph-node GraphQL query logs ")
        .setting(AppSettings::InferSubcommands)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("extract")
                .about("Read StackDriver log files and print the textPayLoad to the SQL or GraphQL output file")
                .args_from_usage(
                    "-v, --verbose  'Print which files are being read on stderr'
                    graphql -g, --graphql=<FILE> Write GraphQL summary to this file
                    sql -s, --sql=<FILE> Write SQL summary to this file
                    <dir> The directory containing StackDriver files",
                ),
        )
        .subcommand(
            SubCommand::with_name("process")
                .about("Process a logfile produced by 'extract' and output a summary")
                .args_from_usage(
                    "-e, --extra 'Print lines that are not recognized as queries on stderr'
                     graphql -g, --graphql=<FILE> Write GraphQL summary to this file
                     sql -s, --sql=<FILE> Write SQL summary to this file",
                ),
        )
        .subcommand(
            SubCommand::with_name("stats")
                .about("Show statistics")
                .args_from_usage(
                    "-s, --sort=[SORT]  'Sort by this column (default: total_time)'
                     <summary>",
                ),
        )
        .subcommand(
            SubCommand::with_name("query")
                .about("Show details about a specific query")
                .args_from_usage(
                    "<summary>
                     <query>...",
                ),
        )
        .subcommand(
            SubCommand::with_name("combine")
                .about("Combine multiple summary files into one")
                .args_from_usage("<file>..."),
        )
        .get_matches();

    fn writer_for(args: &ArgMatches<'_>, name: &str) -> BufWriter<File> {
        args.value_of(name)
            .map(buf_writer)
            .expect(&format!("'{}' is mandatory", name))
    }

    match args.subcommand() {
        ("extract", Some(args)) => {
            let dir = args.value_of("dir").expect("'dir' is mandatory");
            let verbose = args.is_present("verbose");
            let mut gql = writer_for(args, "graphql");
            let mut sql = writer_for(args, "sql");
            extract(dir, &mut gql, &mut sql, verbose)
                .unwrap_or_else(|err| die(&format!("extract: {}", err.to_string())));
        }
        ("process", Some(args)) => {
            let extra = args.is_present("extra");
            let mut gql = writer_for(args, "graphql");
            let mut sql = writer_for(args, "sql");
            let (gql_infos, sql_infos) = process(extra).unwrap_or_else(|err| {
                die(&format!(
                    "process: failed to parse logfile: {}",
                    err.to_string()
                ))
            });
            write_summaries(&mut gql, gql_infos).unwrap_or_else(|err| {
                die(&format!(
                    "process: failed to write GraphQL logfile: {}",
                    err.to_string()
                ))
            });
            write_summaries(&mut sql, sql_infos).unwrap_or_else(|err| {
                die(&format!(
                    "process: failed to write SQL logfile: {}",
                    err.to_string()
                ))
            });
        }
        ("stats", args) => {
            let args = args.expect("arguments are mandatory for this command");

            let summary = args
                .value_of("summary")
                .unwrap_or_else(|| die("stats: missing summary file"));
            let sort = args.value_of("sort").unwrap_or("total_time");
            let queries = read_summaries(summary).unwrap_or_else(|err| {
                die(&format!(
                    "stats: could not read summaries: {}",
                    err.to_string()
                ))
            });
            print_stats(queries, sort);
        }
        ("query", args) => {
            let args = args.expect("arguments are mandatory for this command");
            let summary = args
                .value_of("summary")
                .unwrap_or_else(|| die("stats: missing summary file"));
            let queries = args
                .values_of("query")
                .expect("'query' is a mandatory argument")
                .collect();
            print_queries(summary, queries).unwrap_or_else(|err| {
                die(&format!(
                    "query: could not print queries: {}",
                    err.to_string()
                ))
            });
        }
        ("combine", args) => {
            let files = args
                .expect("arguments are mandatory for this command")
                .values_of("file")
                .expect("'file' is a mandatory argument")
                .collect();

            let infos = combine(files);
            write_summaries(&mut io::stdout(), infos).unwrap_or_else(|err| {
                die(&format!(
                    "combine: failed to write summary file: {}",
                    err.to_string()
                ))
            });
        }
        _ => die("internal error: no other subcommands exist"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gql_query_re() {
        const LINE1: &str = "Dec 30 20:55:13.071 INFO Query timing (GraphQL), \
                             query_time_ms: 160, \
                             query: query Stuff { things } , \
                             query_id: f-1-4-b-e4, \
                             subgraph_id: QmSuBgRaPh, \
                             component: GraphQlRunner\n";
        const LINE2: &str = "Dec 31 23:59:59.667 INFO Query timing (GraphQL), \
                             query_time_ms: 125, \
                             variables: {}, \
                             query: query { things(id:\"1\") { id }} , \
                             query_id: f2-6b-48-b6-6b, \
                             subgraph_id: QmSuBgRaPh, \
                             component: GraphQlRunner";
        const LINE3: &str = "Dec 31 23:59:59.739 INFO Query timing (GraphQL), \
                             query_time_ms: 14, \
                             variables: null, \
                             query: query TranscoderQuery { transcoders(first: 1) { id } } , \
                             query_id: c5-d3-4e-92-37, \
                             subgraph_id: QmeYBGccAwahY, \
                             component: GraphQlRunner";
        const LINE4: &str = "Dec 31 23:59:59.846 INFO Query timing (GraphQL), \
             query_time_ms: 12, \
             variables: {\"id\":\"0xdeadbeef\"}, \
             query: query exchange($id: String!) { exchange(id: $id) { id tokenAddress } } , \
             query_id: c8-1c-4c-98-65, \
             subgraph_id: QmSuBgRaPh, \
             component: GraphQlRunner";

        const LINE5: &str = "Dec 31 22:59:58.863 INFO Query timing (GraphQL), query_time_ms: 2657, variables: {\"_v1_first\":100,\"_v2_where\":{\"status\":\"Registered\"},\"_v0_skip\":0}, query: query TranscodersQuery($_v0_skip: Int, $_v1_first: Int, $_v2_where: Transcoder_filter) { transcoders(where: $_v2_where, skip: $_v0_skip, first: $_v1_first) { ...TranscoderFragment __typename } }  fragment TranscoderFragment on Transcoder { id active status lastRewardRound { id __typename } rewardCut feeShare pricePerSegment pendingRewardCut pendingFeeShare pendingPricePerSegment totalStake pools(orderBy: id, orderDirection: desc) { rewardTokens round { id __typename } __typename } __typename } , query_id: 2d-12-4b-a8-6b, subgraph_id: QmSuBgRaPh, component: GraphQlRunner";

        let caps = GQL_QUERY_RE.captures(LINE1).unwrap();
        assert_eq!(Some("160"), field(&caps, "time"));
        assert_eq!(Some("query Stuff { things }"), field(&caps, "query"));
        assert_eq!(Some("f-1-4-b-e4"), field(&caps, "qid"));
        assert_eq!(Some("QmSuBgRaPh"), field(&caps, "sid"));
        assert_eq!(None, field(&caps, "vars"));

        let caps = GQL_QUERY_RE.captures(LINE2).unwrap();
        assert_eq!(Some("125"), field(&caps, "time"));
        assert_eq!(
            Some("query { things(id:\"1\") { id }}"),
            field(&caps, "query")
        );
        assert_eq!(Some("f2-6b-48-b6-6b"), field(&caps, "qid"));
        assert_eq!(Some("QmSuBgRaPh"), field(&caps, "sid"));
        assert_eq!(Some("{}"), field(&caps, "vars"));

        let caps = GQL_QUERY_RE.captures(LINE3).unwrap();
        assert_eq!(Some("14"), field(&caps, "time"));
        assert_eq!(
            Some("query TranscoderQuery { transcoders(first: 1) { id } }"),
            field(&caps, "query")
        );
        assert_eq!(Some("c5-d3-4e-92-37"), field(&caps, "qid"));
        assert_eq!(Some("QmeYBGccAwahY"), field(&caps, "sid"));
        assert_eq!(Some("null"), field(&caps, "vars"));

        let caps = GQL_QUERY_RE.captures(LINE4).unwrap();
        assert_eq!(Some("12"), field(&caps, "time"));
        assert_eq!(
            Some("query exchange($id: String!) { exchange(id: $id) { id tokenAddress } }"),
            field(&caps, "query")
        );
        assert_eq!(Some("c8-1c-4c-98-65"), field(&caps, "qid"));
        assert_eq!(Some("QmSuBgRaPh"), field(&caps, "sid"));
        assert_eq!(Some("{\"id\":\"0xdeadbeef\"}"), field(&caps, "vars"));

        let caps = dbg!(GQL_QUERY_RE.captures(LINE5)).unwrap();
        assert_eq!(Some("2657"), field(&caps, "time"));
        // Skip the query, it's big
        assert_eq!(Some("2d-12-4b-a8-6b"), field(&caps, "qid"));
        assert_eq!(Some("QmSuBgRaPh"), field(&caps, "sid"));
        assert_eq!(
            Some("{\"_v1_first\":100,\"_v2_where\":{\"status\":\"Registered\"},\"_v0_skip\":0}"),
            field(&caps, "vars")
        );
    }

    #[test]
    fn test_sql_query_re() {
        const LINE1:&str = "Jan 22 11:22:33.573 TRCE Query timing (SQL), \
        entity_count: 7, \
        time_ms: 6, \
        query: select 'Beneficiary' as entity, to_jsonb(c.*) as data   from \"sgd1\".\"beneficiary\" c  where c.\"block_range\" @> $1 order by \"id\"  limit 100 \
        -- binds: [2147483647], \
        query_id: 1d8bc664-41dd-4cf2-8ad6-997e459b322f, \
        subgraph_id: QmZawMfSrDUr1rYAW9b5rSckGoRCw8tN77WXFbLNEKXPGz, \
        component: GraphQlRunner";

        let caps = SQL_QUERY_RE.captures(LINE1).unwrap();

        assert_eq!(Some("6"), field(&caps, "time"));
        let query = field(&caps, "query").unwrap();
        assert!(query.starts_with("select 'Beneficiary' as entity"));
        assert!(query.ends_with("limit 100"));
        assert_eq!(Some("[2147483647]"), field(&caps, "binds"));
        assert_eq!(
            Some("1d8bc664-41dd-4cf2-8ad6-997e459b322f"),
            field(&caps, "qid")
        );
        assert_eq!(
            Some("QmZawMfSrDUr1rYAW9b5rSckGoRCw8tN77WXFbLNEKXPGz"),
            field(&caps, "sid")
        );
    }
}
