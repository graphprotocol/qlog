extern crate clap;
extern crate regex;
#[macro_use]
extern crate lazy_static;
extern crate serde;
extern crate serde_json;
extern crate walkdir;

use clap::{App, AppSettings, SubCommand};
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, Write};
use std::time::{Duration, Instant};

use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

/// Queries that take longer than this (in ms) are considered slow
const SLOW_THRESHOLD: u64 = 1000;
const PRINT_TIMING: bool = false;

lazy_static! {
    /// The regexp we use to extract data about GraphQL queries from log files
    static ref QUERY_RE: Regex = Regex::new(
        " Execute query, query_time_ms: ([0-9]+), \
         query: (.*) , \
         query_id: ([0-9a-f-]+), \
         subgraph_id: ([a-zA-Z0-9]*), \
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
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueryInfo {
    query: String,
    subgraph: String,
    /// The total time (in ms) spend on this query
    total_time: u64,
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
            max_time: 0,
            max_uuid: "(none)".to_owned(),
            max_variables: None,
            slow_count: 0,
            calls: 0,
        }
    }

    fn add(&mut self, time: u64, query_id: &str, variables: Option<String>) {
        self.calls += 1;
        self.total_time += time;
        if time > self.max_time {
            self.max_time = time;
            self.max_uuid = query_id.to_owned();
            self.max_variables = variables;
        }
        if time > SLOW_THRESHOLD {
            self.slow_count += 1;
        }
    }

    fn avg(&self) -> f64 {
        self.total_time as f64 / self.calls as f64
    }

    fn combine(&mut self, other: &QueryInfo) {
        self.calls += other.calls;
        self.total_time += other.total_time;
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

/// The heart of the `process` subcommand. Expects a logfile containing
/// query logs on the command line.
fn process(print_extra: bool) -> Result<Vec<QueryInfo>, std::io::Error> {
    // Read the file line by line using the lines() iterator from std::io::BufRead.
    let mut queries: BTreeMap<u64, QueryInfo> = BTreeMap::default();
    let mut count: usize = 0;

    fn field<'a>(caps: &'a Captures, i: usize) -> Option<&'a str> {
        caps.get(i).map(|field| field.as_str())
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
    fn canonicalize(query: &str) -> (Cow<'_, str>, Option<String>) {
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
    let mut lines: usize = 0;
    let mut mtch = Duration::from_secs(0);
    for line in io::stdin().lock().lines() {
        let line = line?;

        let mtch_start = Instant::now();
        if let Some(caps) = QUERY_RE.captures(&line) {
            mtch += mtch_start.elapsed();
            lines += 1;
            if let (Some(query_time), Some(query), Some(query_id), Some(subgraph)) = (
                field(&caps, 1),
                field(&caps, 2),
                field(&caps, 3),
                field(&caps, 4),
            ) {
                let query_time: u64 = match query_time.parse() {
                    Err(_) => {
                        if print_extra {
                            eprintln!("not a query: {}", line)
                        }
                        continue;
                    }
                    Ok(qt) => qt,
                };
                let (query, variables) = canonicalize(query);
                let hsh = QueryInfo::hash(&query, &subgraph);
                let info = queries.entry(hsh).or_insert_with(|| {
                    count += 1;
                    QueryInfo::new(query.into_owned(), subgraph.to_owned(), count)
                });
                info.add(query_time, &query_id, variables);
            }
        } else if print_extra {
            eprintln!("not a query: {}", line);
        }
    }
    if PRINT_TIMING {
        eprintln!(
            "parse: {:.3}s\nmatch: {:.3}s\ncount: {}",
            start.elapsed().as_secs_f64(),
            mtch.as_secs_f64(),
            lines
        );
    }
    Ok(queries.values().cloned().collect())
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

/// Write a list of summaries to stdout; the list will be written in
/// 'JSON lines' format
fn write_summaries(infos: Vec<QueryInfo>) {
    for info in infos {
        let json = serde_json::to_string(&info).unwrap_or_else(|err| {
            die(&format!(
                "process: failed to convert to json: {}",
                err.to_string()
            ))
        });
        println!("{}", json);
    }
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
            "| {:^7} | {:^8} | {:^12} | {:^6} | {:^6} | {:^6} | {:^8} |",
            "QID", "calls", "total", "avg", "max", "slow", "uuid"
        );
        writeln!(
            stdout,
            "|---------+----------+--------------+--------+--------+--------+----------|"
        );
    }
    for query in &queries {
        #[allow(unused_must_use)]
        {
            writeln!(
                stdout,
                "| Q{:0>6} | {:>8} | {:>12} | {:>6.0} | {:>6} | {:>6} | {:<8} |",
                query.id,
                query.calls,
                query.total_time,
                query.avg(),
                query.max_time,
                query.slow_count,
                &query.max_uuid[..8]
            );
        }
    }
}

/// The 'extract' subcommand turning a StackDriver logfile into a plain
/// textual logfile by pulling out the 'textPayload' for each entry
fn extract(dir: &str, verbose: bool) -> Result<(), std::io::Error> {
    let json_ext = OsStr::new("json");
    let mut stdout = io::stdout();

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
                if let Value::Object(map) = serde_json::from_str(&line?)? {
                    if let Some(Value::String(text)) = map.get("textPayload") {
                        if let Err(e) = stdout.write(text.as_bytes()) {
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
                .about("Read StackDriver log files and print the textPayLoad on stdout")
                .args_from_usage(
                    "-v, --verbose  'Print which files are being read on stderr'
                    <dir> The directory containing StackDriver files",
                ),
        )
        .subcommand(
            SubCommand::with_name("process")
                .about("Process a logfile produced by 'extract' and output a summary")
                .args_from_usage(
                    "-e, --extra 'Print lines that are not recognized as queries on stderr'",
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

    match args.subcommand() {
        ("extract", Some(args)) => {
            let dir = args.value_of("dir").expect("'dir' is mandatory");
            let verbose = args.is_present("verbose");
            extract(dir, verbose)
                .unwrap_or_else(|err| die(&format!("extract: {}", err.to_string())));
        }
        ("process", args) => {
            let extra = args.map(|args| args.is_present("extra")).unwrap_or(false);
            let infos = process(extra).unwrap_or_else(|err| {
                die(&format!(
                    "ingest: failed to parse logfile: {}",
                    err.to_string()
                ))
            });
            write_summaries(infos);
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
            write_summaries(infos);
        }
        _ => die("internal error: no other subcommands exist"),
    }
}
