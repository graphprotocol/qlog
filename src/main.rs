#[macro_use]
extern crate lazy_static;

use clap::{App, AppSettings, ArgMatches, SubCommand};
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::{Duration, Instant};

mod common;
mod extract;
mod sampler;

use sampler::Sampler;

/// Queries that take longer than this (in ms) are considered slow
const SLOW_THRESHOLD: u64 = 1000;

lazy_static! {
    /// The regexp we use to extract data about GraphQL queries from log files
    static ref GQL_QUERY_RE: Regex = Regex::new(
        " Query timing \\(GraphQL\\), \
          (?:complexity: (?P<complexity>[0-9]+), )?\
          (?:block: (?P<block>[0-9]+), )?\
          (cached: (?P<cached>[a-zA-Z0-9_-]+), )?\
         query_time_ms: (?P<time>[0-9]+), \
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
        Regex::new("([_A-Za-z][_0-9A-Za-z]*): *([0-9]+|\"([^\"]|\\\\\")*\"|\\[[^]]*\\])").unwrap();

    static ref QUERY_NAME_RE: Regex =
    Regex::new("query GraphQL__Client__OperationDefinition(?P<delete>_[0-9]+)").unwrap();
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
    /// The complexity of the slowest query
    #[serde(default = "zero")]
    max_complexity: u64,
    /// The number of times this query took longer than `SLOW_THRESHOLD`
    slow_count: u64,
    /// The number of times the query has been run
    calls: u64,
    /// An ID to make it easier to refer to the query for the user
    id: usize,
    /// The number of query executions that were served from cache
    #[serde(default = "zero")]
    cached_count: u64,
    /// The total time we spent serving cached queries
    #[serde(default = "zero")]
    cached_time: u64,
    /// The maximum time we spent serving a cached query
    #[serde(default = "zero")]
    cached_max_time: u64,
    /// The hash value for this query; two `QueryInfo` instances with the
    /// same `hash` are assumed to refer to the same logical query
    #[serde(default = "zero")]
    hash: u64,
}

fn zero() -> u64 {
    0
}

impl QueryInfo {
    fn new(query: String, subgraph: String, id: usize, hash: u64) -> QueryInfo {
        QueryInfo {
            query,
            subgraph,
            id,
            total_time: 0,
            time_squared: 0,
            max_time: 0,
            max_uuid: "(none)".to_owned(),
            max_variables: None,
            max_complexity: 0,
            slow_count: 0,
            calls: 0,
            cached_count: 0,
            cached_time: 0,
            cached_max_time: 0,
            hash,
        }
    }

    fn add(
        &mut self,
        time: u64,
        query_id: &str,
        variables: Option<Cow<'_, str>>,
        cached: bool,
        complexity: u64,
    ) {
        if cached {
            self.cached_count += 1;
            self.cached_time += time;
            if time > self.cached_max_time {
                self.cached_max_time = time;
            }
        } else {
            self.calls += 1;
            self.total_time += time;
            self.time_squared += time * time;
            if time > self.max_time {
                self.max_time = time;
                self.max_uuid = query_id.to_owned();
                self.max_variables = variables.map(|vars| vars.into_owned());
                self.max_complexity = complexity;
            }
            if time > SLOW_THRESHOLD {
                self.slow_count += 1;
            }
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

    fn cached_avg(&self) -> f64 {
        self.cached_time as f64 / self.cached_count as f64
    }

    fn combine(&mut self, other: &QueryInfo) {
        self.calls += other.calls;
        self.total_time += other.total_time;
        self.time_squared += other.time_squared;
        if other.max_time > self.max_time {
            self.max_time = other.max_time;
            self.max_uuid = other.max_uuid.clone();
            self.max_variables = other.max_variables.clone();
            self.max_complexity = other.max_complexity.clone();
        }
        self.slow_count += other.slow_count;

        self.cached_count += other.cached_count;
        self.cached_time += other.cached_time;
        if other.cached_max_time > self.cached_max_time {
            self.cached_max_time = other.cached_max_time;
        }
    }

    /// A hash value that can be calculated without constructing
    /// a `QueryInfo`
    fn hash(query: &str, subgraph: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        (query, subgraph).hash(&mut hasher);
        hasher.finish()
    }

    fn read(line: &str) -> Result<QueryInfo, serde_json::Error> {
        serde_json::from_str(line).map(|mut info: QueryInfo| {
            if info.hash == 0 {
                info.hash = QueryInfo::hash(&info.query, &info.subgraph);
            }
            info
        })
    }
}

fn field<'a>(caps: &'a Captures, group: &str) -> Option<&'a str> {
    caps.name(group).map(|field| field.as_str())
}

fn add_entry(
    queries: &mut BTreeMap<u64, QueryInfo>,
    query_time: u64,
    complexity: u64,
    query_id: &str,
    query: &str,
    variables: Option<&str>,
    cached: bool,
    subgraph: &str,
) {
    let (query, variables) = canonicalize(query, variables);
    let variables = variables.map(|vars| Cow::from(vars));

    let hsh = QueryInfo::hash(&query, &subgraph);
    let count = queries.len();
    let info = queries
        .entry(hsh)
        .or_insert_with(|| QueryInfo::new(query.into_owned(), subgraph.to_owned(), count + 1, hsh));
    info.add(query_time, &query_id, variables, cached, complexity);
}

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

    let (mut query, vars) = if VAR_RE.is_match(query) {
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
    };

    if let Some(caps) = QUERY_NAME_RE.captures(query.as_ref()) {
        let delete = caps.name("delete").unwrap();
        let range = delete.start()..delete.end();
        query.to_mut().replace_range(range, "");
    }
    (query, vars)
}

/// The heart of the `process` subcommand. Expects a logfile containing
/// query logs on the command line.
fn process(
    sampler: &mut Sampler,
    print_extra: bool,
) -> Result<(Vec<QueryInfo>, Vec<QueryInfo>), std::io::Error> {
    // Read the file line by line using the lines() iterator from std::io::BufRead.
    let mut gql_queries: BTreeMap<u64, QueryInfo> = BTreeMap::default();
    let mut sql_queries: BTreeMap<u64, QueryInfo> = BTreeMap::default();

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
            let cached = field(&caps, "cached").map(|v| v == "true").unwrap_or(false);
            if let (
                Some(query_time),
                Some(complexity),
                Some(query),
                Some(query_id),
                Some(subgraph),
            ) = (
                field(&caps, "time"),
                field(&caps, "complexity"),
                field(&caps, "query"),
                field(&caps, "qid"),
                field(&caps, "sid"),
            ) {
                let variables = field(&caps, "vars");
                let query_time: u64 = query_time.parse().unwrap_or_else(|_| {
                    eprintln!("invalid query_time: {}", line);
                    0
                });
                sampler.sample(&query, &variables, &subgraph);
                add_entry(
                    &mut gql_queries,
                    query_time,
                    complexity.parse::<u64>().unwrap(),
                    query_id,
                    query,
                    variables,
                    cached,
                    subgraph,
                );
            }
        } else if let Some(caps) = SQL_QUERY_RE.captures(&line) {
            mtch += mtch_start.elapsed();
            sql_lines += 1;
            if let (Some(time), Some(complexity), Some(query), Some(binds), Some(qid), Some(sid)) = (
                field(&caps, "time"),
                field(&caps, "complexity"),
                field(&caps, "query"),
                field(&caps, "binds"),
                field(&caps, "qid"),
                field(&caps, "sid"),
            ) {
                let time: u64 = time.parse().unwrap_or_else(|_| {
                    eprintln!("invalid query_time: {}", line);
                    0
                });
                add_entry(
                    &mut sql_queries,
                    time,
                    complexity.parse::<u64>().unwrap(),
                    qid,
                    query,
                    Some(binds),
                    false,
                    sid,
                );
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
        infos.push(QueryInfo::read(&line?)?);
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

fn sort_queries(queries: &mut Vec<QueryInfo>, sort: &str) {
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
}

/// The 'stats' subcommand
fn print_stats(queries: Vec<QueryInfo>) {
    // Use writeln! instead of println! so we do not get a panic on
    // SIGPIPE if the output is piped into e.g. head -n 1
    let mut stdout = io::stdout();
    #[allow(unused_must_use)]
    {
        writeln!(
            stdout,
            "| {:^7} | {:^8} | {:^8} | {:^12} | {:^6} | {:^6} | {:^6} | {:^6} |",
            "QID", "calls", "complexity", "total", "avg", "stddev", "max", "slow"
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
                "| Q{:0>6} | {:>8} | {:>8} | {:>12} | {:>6.0} | {:>6.0} | {:>6} | {:>6} |",
                query.id,
                query.calls,
                query.max_complexity,
                query.total_time,
                query.avg(),
                query.stddev(),
                query.max_time,
                query.slow_count
            );
        }
    }
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
            infos
                .entry(info.hash)
                .and_modify(|existing| existing.combine(&info))
                .or_insert(info);
        }
    }
    for (indx, value) in infos.values_mut().enumerate() {
        value.id = indx;
    }
    infos.values().cloned().collect()
}

fn print_full_query(info: &QueryInfo) {
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

    let mut stdout = io::stdout();
    #[allow(unused_must_use)]
    {
        writeln!(stdout, "{:=<32} Q{} {:=<32}", "", info.id, "");
        writeln!(stdout, "# subgraph:      {}", info.subgraph);
        writeln!(stdout, "# calls:           {:>12}", info.calls);
        writeln!(stdout, "# complexity:      {:>12}", info.max_time);
        writeln!(stdout, "# slow_count:      {:>12}", info.slow_count);
        writeln!(
            stdout,
            "# slow_percent:    {:>12.2} %",
            info.slow_count as f64 * 100.0 / info.calls as f64
        );
        let (amount, unit) = human_readable_time(info.total_time);
        writeln!(stdout, "# total_time:      {:>12.1} {}", amount, unit);
        writeln!(stdout, "# avg_time:        {:>12.0} ms", info.avg());
        writeln!(stdout, "# stddev_time:     {:>12.0} ms", info.stddev());
        if info.cached_count > 0 {
            writeln!(stdout, "# cached calls:    {:>12}", info.cached_count);
            let (amount, unit) = human_readable_time(info.cached_time);
            writeln!(stdout, "# cached time:     {:>12.1} {}", amount, unit);
            writeln!(stdout, "# cached avg_time: {:>12.0} ms", info.cached_avg());
            writeln!(stdout, "# cached max_time: {:>12} ms", info.cached_max_time);
        }
        writeln!(stdout, "# max_time:        {:>12} ms", info.max_time);
        writeln!(stdout, "# max_uuid:      {}", info.max_uuid);
        if let Some(max_vars) = &info.max_variables {
            writeln!(stdout, "# max_variables: {}", max_vars);
        }
        writeln!(stdout, "\n{}", info.query);
    }
}

/// The 'queries' subcommand
fn print_queries(filename: &str, queries: Vec<&str>) -> Result<(), std::io::Error> {
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
                print_full_query(info);
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
                    graphql -g, --graphql=<FILE> 'Write GraphQL summary to this file'
                    sql -s, --sql=<FILE> 'Write SQL summary to this file'
                    <dir> 'The directory containing StackDriver files'",
                ),
        )
        .subcommand(
            SubCommand::with_name("process")
                .about("Process a logfile produced by 'extract' and output a summary")
                .args_from_usage(
                    "-e, --extra 'Print lines that are not recognized as queries on stderr'
                     [graphql] -g, --graphql=<FILE> Write GraphQL summary to this file
                     [sql] -s, --sql=<FILE> Write SQL summary to this file
                     [samples] --samples=<NUMBER> 'Number of samples to take'
                     [sample-file] --sample-file=<FILE> 'Where to write samples'
                     [sample-subgraphs] --sample-subgraphs=<LIST> 'Which subgraphs to sample'",
                ),
        )
        .subcommand(
            SubCommand::with_name("stats")
                .about("Show statistics")
                .after_help("For an explanation of the full output format, see the help for 'stats'")
                .args_from_usage(
                    "-s, --sort=[SORT]  'Sort by this column (default: total_time)'
                     -f, --full         'Print full query details'
                     <summary>",
                ),
        )
        .subcommand(
            SubCommand::with_name("query")
                .about("Show details about a specific query")
                .after_help(QUERY_HELP_TEXT)
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
        buf_writer(args.value_of(name).unwrap_or("/dev/null"))
    }

    match args.subcommand() {
        ("extract", Some(args)) => {
            let dir = args.value_of("dir").expect("'dir' is mandatory");
            let verbose = args.is_present("verbose");
            let mut gql = writer_for(args, "graphql");
            let mut sql = writer_for(args, "sql");
            extract::run(dir, &mut gql, &mut sql, verbose)
                .unwrap_or_else(|err| die(&format!("extract: {}", err.to_string())));
        }
        ("process", Some(args)) => {
            let extra = args.is_present("extra");
            let mut gql = writer_for(args, "graphql");
            let mut sql = writer_for(args, "sql");

            let samples = args
                .value_of("samples")
                .map(|s| s.parse::<usize>().expect("'samples' is a number"))
                .unwrap_or(0);
            let samples_file = args
                .value_of("sample-file")
                .unwrap_or("/var/tmp/samples.jsonl");
            let samples_subgraphs = args
                .value_of("sample-subgraphs")
                .map(|s| {
                    s.split(",")
                        .map(|t| t.to_owned())
                        .collect::<HashSet<String>>()
                })
                .unwrap_or(HashSet::new());
            if samples > 0 {
                println!(
                    "Taking {} samples and writing them to {}",
                    samples, samples_file
                );
                if samples_subgraphs.is_empty() {
                    println!("  sampling all subgraphs");
                } else {
                    println!("  sampling these subgraphs");
                    for subgraph in &samples_subgraphs {
                        println!("    {}", subgraph);
                    }
                }
            }
            let mut sampler = Sampler::new(samples, samples_subgraphs);
            let (gql_infos, sql_infos) = process(&mut sampler, extra).unwrap_or_else(|err| {
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
            if samples > 0 {
                sampler
                    .write(buf_writer(samples_file))
                    .unwrap_or_else(|err| {
                        die(&format!(
                            "process: failed to write samples to {}: {}",
                            samples_file,
                            err.to_string()
                        ))
                    });
            }
        }
        ("stats", args) => {
            let args = args.expect("arguments are mandatory for this command");

            let summary = args
                .value_of("summary")
                .unwrap_or_else(|| die("stats: missing summary file"));
            let sort = args.value_of("sort").unwrap_or("total_time");
            let full = args.is_present("full");
            let mut queries = read_summaries(summary).unwrap_or_else(|err| {
                die(&format!(
                    "stats: could not read summaries: {}",
                    err.to_string()
                ))
            });
            sort_queries(&mut queries, sort);
            if full {
                for query in queries {
                    print_full_query(&query);
                }
            } else {
                print_stats(queries);
            }
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

// Help text for the 'query' subcommand
const QUERY_HELP_TEXT: &str =
    "For each query, print summary statistics of the query. The output has the\
\nfollowing format:

================================ QNNNN ================================
# subgraph:        subgraph id
# calls:           number of times the query was run against
#                  the database
# slow_count:      number of times a query took longer than 1s
# slow_percent:    slow_count / calls * 100
# total_time:      total time the queries took
# avg_time:        total_time / calls
# stddev_time:     standard deviation of the time queries took
# cached calls:    number of query executions that were served
#                  from cache. Cached executions do not enter
#                  into the statistics for non-cached executions
# cached time:     total time spent serving queries from cache
# cached avg_time: cached time / cached calls
# cached max_time: maximum time it took to serve a query from cache
# max_time:        maximum time it took to serve a query from
#                  the database
# max_uuid:        query_id of a query that took max_time
# max_variables:   variables that were passed to the invocation
#                  that took max_time

graphql query processed so that most values in filters etc. are
extracted into variables
";

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

        const LINE6: &str = "Jun 26 22:12:02.295 INFO Query timing (GraphQL), \
                             complexity: 4711, \
                             block: 10344025, \
                             cached: false, \
                             query_time_ms: 10, \
                             variables: null, \
                             query: { rateUpdates(orderBy: timestamp, orderDirection: desc, where: {synth: \"sEUR\", timestamp_gte: 1593123133, timestamp_lte: 1593209533}, first: 1000, skip: 0) { id synth rate block timestamp } } , \
                             query_id: cb9af68f-ae60-4dba-b9b3-89aee6fe8eca, \
                             subgraph_id: QmaSubgraph, component: GraphQlRunner";

        const LINE7: &str = "Jun 26 22:12:02.295 INFO Query timing (GraphQL), \
                             complexity: 0, \
                             block: 10344025, \
                             cached: herd-hit, \
                             query_time_ms: 10, \
                             variables: null, \
                             query: { rateUpdates(orderBy: timestamp, orderDirection: desc, where: {synth: \"sEUR\", timestamp_gte: 1593123133, timestamp_lte: 1593209533}, first: 1000, skip: 0) { id synth rate block timestamp } } , \
                             query_id: cb9af68f-ae60-4dba-b9b3-89aee6fe8eca, \
                             subgraph_id: QmaSubgraph, component: GraphQlRunner";

        let caps = GQL_QUERY_RE.captures(LINE1).unwrap();
        assert_eq!(Some("160"), field(&caps, "time"));
        assert_eq!(Some("query Stuff { things }"), field(&caps, "query"));
        assert_eq!(Some("f-1-4-b-e4"), field(&caps, "qid"));
        assert_eq!(Some("QmSuBgRaPh"), field(&caps, "sid"));
        assert_eq!(None, field(&caps, "vars"));
        assert_eq!(None, field(&caps, "cached"));

        let caps = GQL_QUERY_RE.captures(LINE2).unwrap();
        assert_eq!(None, field(&caps, "complexity"));
        assert_eq!(None, field(&caps, "block"));
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

        let caps = GQL_QUERY_RE.captures(LINE5).unwrap();
        assert_eq!(Some("2657"), field(&caps, "time"));
        // Skip the query, it's big
        assert_eq!(Some("2d-12-4b-a8-6b"), field(&caps, "qid"));
        assert_eq!(Some("QmSuBgRaPh"), field(&caps, "sid"));
        assert_eq!(
            Some("{\"_v1_first\":100,\"_v2_where\":{\"status\":\"Registered\"},\"_v0_skip\":0}"),
            field(&caps, "vars")
        );

        let caps = GQL_QUERY_RE.captures(LINE6).unwrap();
        assert_eq!(Some("4711"), field(&caps, "complexity"));
        assert_eq!(Some("10344025"), field(&caps, "block"));
        assert_eq!(Some("10"), field(&caps, "time"));
        assert_eq!(Some("false"), field(&caps, "cached"));
        // Skip the query, it's big
        assert_eq!(
            Some("cb9af68f-ae60-4dba-b9b3-89aee6fe8eca"),
            field(&caps, "qid")
        );
        assert_eq!(
            Some("{ rateUpdates(orderBy: timestamp, orderDirection: desc, where: {synth: \"sEUR\", timestamp_gte: 1593123133, timestamp_lte: 1593209533}, first: 1000, skip: 0) { id synth rate block timestamp } }"),
            field(&caps, "query"));
        assert_eq!(Some("QmaSubgraph"), field(&caps, "sid"));
        assert_eq!(Some("null"), field(&caps, "vars"));

        let caps = GQL_QUERY_RE.captures(LINE7).unwrap();
        assert_eq!(Some("0"), field(&caps, "complexity"));
        assert_eq!(Some("10344025"), field(&caps, "block"));
        assert_eq!(Some("10"), field(&caps, "time"));
        assert_eq!(Some("herd-hit"), field(&caps, "cached"));
        // Skip the query, it's big
        assert_eq!(
            Some("cb9af68f-ae60-4dba-b9b3-89aee6fe8eca"),
            field(&caps, "qid")
        );
        assert_eq!(Some("QmaSubgraph"), field(&caps, "sid"));
        assert_eq!(Some("null"), field(&caps, "vars"));
    }

    #[test]
    fn test_gql_query_re_with_cache() {
        const LINE1: &str = "INFO Query timing (GraphQL), complexity: 0, \
          block: 21458574, \
          cached: true, \
          query_time_ms: 23, variables: null, \
          query: { things { id timestamp } } , \
          query_id: ed3d5fd7-9c86-4a68-8957-657d84c24aec, \
          subgraph_id: Qmsubgraph, \
          component: GraphQlRunner";

        let caps = GQL_QUERY_RE.captures(LINE1).unwrap();
        assert_eq!(Some("true"), field(&caps, "cached"));
        assert_eq!(Some("Qmsubgraph"), field(&caps, "sid"));
        assert_eq!(Some("0"), field(&caps, "complexity"));
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

    #[test]
    fn test_gql_query_name_fix() {
        const QUERY: &str =
            "query GraphQL__Client__OperationDefinition_70073834585800 { tokens { symbol } }";
        const CANONICAL: &str = "query GraphQL__Client__OperationDefinition { tokens { symbol } }";
        let (query, vars) = canonicalize(QUERY, None);
        assert_eq!(None, vars);
        assert_eq!(CANONICAL, query);
    }
}
