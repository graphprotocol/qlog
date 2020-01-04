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

const SLOW_THRESHOLD: u64 = 1000;
const PRINT_TIMING: bool = false;

lazy_static! {
    static ref QUERY_RE: Regex = Regex::new(
        " Execute query, query_time_ms: ([0-9]+), \
         query: (.*) , \
         query_id: ([0-9a-f-]+), \
         subgraph_id: ([a-zA-Z0-9]*), \
         component: "
    )
    .unwrap();
    static ref VAR_RE: Regex =
        Regex::new("([_A-Za-z][_0-9A-Za-z]*): *([0-9]+|\"[^\"]*\")").unwrap();
}

pub fn die(msg: &str) -> ! {
    println!("{}", msg);
    std::process::exit(1);
}

pub fn die_with<T, E: std::fmt::Display>(res: Result<T, E>, msg: &str) -> T {
    match res {
        Err(e) => die(&format!("{}: {}", msg, e.to_string())),
        Ok(t) => t,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueryInfo {
    query: String,
    subgraph: String,
    total_time: u64,
    max_time: u64,
    max_uuid: String,
    max_variables: Option<String>,
    slow_count: u64,
    calls: u64,
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

    fn hash(query: &str, subgraph: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        (query, subgraph).hash(&mut hasher);
        hasher.finish()
    }
}

fn parse_logfile(print_extra: bool) -> Result<Vec<QueryInfo>, std::io::Error> {
    // Read the file line by line using the lines() iterator from std::io::BufRead.
    let mut queries: BTreeMap<u64, QueryInfo> = BTreeMap::default();
    let mut count: usize = 0;

    fn field<'a>(caps: &'a Captures, i: usize) -> Option<&'a str> {
        caps.get(i).map(|field| field.as_str())
    }

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
                    Err(_) => continue,
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

fn read_summaries(filename: &str) -> Result<Vec<QueryInfo>, std::io::Error> {
    let file = std::fs::File::open(filename)?;
    let reader = BufReader::new(file);
    let mut infos = vec![];
    for line in reader.lines() {
        let line = line.unwrap();

        let value = serde_json::from_str(&line)?;
        infos.push(value);
    }
    Ok(infos)
}

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

fn prepare(dir: &str, verbose: bool) -> Result<(), std::io::Error> {
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
                let line = line?;

                let value = serde_json::from_str(&line)?;
                if let Value::Object(map) = value {
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
                    "-s, --sort=[SORT]  'Sort by this column'
                     <summary>",
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
            let dir = args.value_of("dir").unwrap();
            let verbose = args.is_present("verbose");
            prepare(dir, verbose)
                .unwrap_or_else(|err| die(&format!("prepare: {}", err.to_string())));
        }
        ("process", args) => {
            let extra = match args {
                None => false,
                Some(args) => args.is_present("extra"),
            };
            let infos = parse_logfile(extra).unwrap_or_else(|err| {
                die(&format!(
                    "ingest: failed to parse logfile: {}",
                    err.to_string()
                ))
            });
            write_summaries(infos);
        }
        ("stats", args) => match args {
            None => die("stats: missing arguments"),
            Some(args) => {
                let input = args
                    .value_of("summary")
                    .unwrap_or_else(|| die("stats: missing input file"));
                let sort = args.value_of("sort").unwrap_or("total_time");
                let queries = read_summaries(input).unwrap_or_else(|err| {
                    die(&format!(
                        "stats: could not read summaries: {}",
                        err.to_string()
                    ))
                });
                print_stats(queries, sort);
            }
        },
        ("combine", args) => {
            let infos = combine(args.unwrap().values_of("file").unwrap().collect());
            write_summaries(infos);
        }
        _ => die("internal error: no other subcommands exist"),
    }
}
