use clap::{App, AppSettings, ArgMatches, SubCommand};
use graphql_parser::parse_query;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::time::{Duration, Instant};

mod common;
mod entry;
mod extract;
mod sampler;
mod shape_hash;

use entry::Entry;
use sampler::Sampler;

/// Queries that take longer than this (in ms) are considered slow
const SLOW_THRESHOLD: u64 = 1000;

pub fn die(msg: &str) -> ! {
    eprintln!("{}", msg);
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
    max_variables: String,
    /// The complexity of the slowest query
    #[serde(default = "zero")]
    max_complexity: u64,
    /// The number of times this query took longer than `SLOW_THRESHOLD`
    slow_count: u64,
    /// The number of times the query has been run
    calls: u64,
    /// An ID to make it easier to refer to the query for the user
    id: usize,
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
            max_variables: "null".to_owned(),
            max_complexity: 0,
            slow_count: 0,
            calls: 0,
            hash,
        }
    }

    fn add(&mut self, time: u64, query_id: &str, query: &str, variables: &str, complexity: u64) {
        self.calls += 1;
        self.total_time += time;
        self.time_squared += time * time;
        if time > self.max_time {
            self.max_time = time;
            self.max_uuid = query_id.to_owned();
            self.max_variables = variables.to_owned();
            self.max_complexity = complexity;
            self.query = query.to_owned();
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
            self.max_complexity = other.max_complexity.clone();
        }
        self.slow_count += other.slow_count;
    }

    /// A hash value that can be calculated without constructing
    /// a `QueryInfo`
    fn hash(query_id: &str, query: &str, subgraph: &str) -> u64 {
        let mut hasher = DefaultHasher::new();

        if query_id.matches("-").count() == 1 {
            // A new style query id in the format {shape_hash}-{hash}
            let shape_hash = query_id.split("-").next().unwrap();
            u64::from_str_radix(shape_hash, 16).map_err(|e| {
                eprintln!(
                    "query_id looks like it has the shape_hash, but apparently not: {}: {}",
                    shape_hash,
                    e.to_string()
                );
            })
        } else {
            parse_query(query)
                .map_err(|e| {
                    eprintln!(
                        "Failed to parse GraphQL query: {}: {}",
                        e.to_string(),
                        query
                    )
                })
                .map(|doc| shape_hash::shape_hash(&doc))
        }
        .map(|shape_hash|
            // We have a shape_hash
            (shape_hash, subgraph).hash(&mut hasher))
        .unwrap_or_else(|_|
            // Fall back to the old way of computing hashes
            (query, subgraph).hash(&mut hasher));

        hasher.finish()
    }

    fn read(line: &str) -> Result<QueryInfo, serde_json::Error> {
        serde_json::from_str(line).map(|mut info: QueryInfo| {
            if info.hash == 0 {
                info.hash = QueryInfo::hash("ignore", &info.query, &info.subgraph);
            }
            info
        })
    }
}

fn add_entry(
    queries: &mut BTreeMap<u64, QueryInfo>,
    query_time: u64,
    complexity: u64,
    query_id: &str,
    query: &str,
    variables: &str,
    subgraph: &str,
) {
    let hsh = QueryInfo::hash(query_id, &query, &subgraph);
    let count = queries.len();
    let info = queries
        .entry(hsh)
        .or_insert_with(|| QueryInfo::new(query.to_owned(), subgraph.to_owned(), count + 1, hsh));
    info.add(query_time, &query_id, query, variables, complexity);
}

/// The heart of the `process` subcommand. Expects a logfile containing
/// query logs on the command line.
fn process(sampler: &mut Sampler, print_extra: bool) -> Result<Vec<QueryInfo>, std::io::Error> {
    // Read the file line by line using the lines() iterator from std::io::BufRead.
    let mut gql_queries: BTreeMap<u64, QueryInfo> = BTreeMap::default();

    let start = Instant::now();
    let mut gql_lines: usize = 0;
    let mut mtch = Duration::from_secs(0);
    for line in io::stdin().lock().lines() {
        let line = line?;

        let mtch_start = Instant::now();
        if let Some(entry) = Entry::parse(&line, None) {
            mtch += mtch_start.elapsed();
            gql_lines += 1;
            sampler.sample(&entry.query, &entry.variables, &entry.subgraph);
            add_entry(
                &mut gql_queries,
                entry.time,
                0,
                entry.query_id,
                entry.query,
                entry.variables,
                entry.subgraph,
            );
        } else if print_extra {
            eprintln!("not a query: {}", line);
        }
    }
    eprintln!(
        "Processed {} GraphQL queries in {:.3}s (regexp match: {:.3}s)",
        gql_lines,
        start.elapsed().as_secs_f64(),
        mtch.as_secs_f64(),
    );
    Ok(gql_queries.values().cloned().collect())
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
        writeln!(stdout, "# max_time:        {:>12} ms", info.max_time);
        writeln!(stdout, "# max_uuid:      {}", info.max_uuid);
        writeln!(stdout, "# max_variables: {}", info.max_variables);
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
                    <dir> 'The directory containing StackDriver files'",
                ),
        )
        .subcommand(
            SubCommand::with_name("process")
                .about("Process a logfile produced by 'extract' and output a summary")
                .args_from_usage(
                    "-e, --extra 'Print lines that are not recognized as queries on stderr'
                     [graphql] -g, --graphql=<FILE> Write GraphQL summary to this file
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
            extract::run(dir, &mut gql, verbose)
                .unwrap_or_else(|err| die(&format!("extract: {}", err.to_string())));
        }
        ("process", Some(args)) => {
            let extra = args.is_present("extra");
            let mut gql = writer_for(args, "graphql");

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
            let gql_infos = process(&mut sampler, extra).unwrap_or_else(|err| {
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
# max_time:        maximum time it took to serve a query from
#                  the database
# max_uuid:        query_id of a query that took max_time
# max_variables:   variables that were passed to the invocation
#                  that took max_time

graphql query processed so that most values in filters etc. are
extracted into variables
";
