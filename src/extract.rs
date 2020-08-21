use serde_json::Value;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use walkdir::WalkDir;

use crate::common::{GQL_MARKER, SQL_MARKER, TRIMMED};

fn extract<T: Read>(
    source: T,
    gql: &mut dyn Write,
    sql: &mut dyn Write,
) -> Result<(usize, usize), std::io::Error> {
    let mut count: usize = 0;
    let mut trimmed_count: usize = 0;
    let mut stderr = io::stderr();

    let reader = BufReader::new(source);

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
                    stderr.write(text.as_bytes())
                };
                if let Err(e) = res {
                    if e.kind() == std::io::ErrorKind::BrokenPipe {
                        return Ok((count, trimmed_count));
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    Ok((count, trimmed_count))
}

/// The 'extract' subcommand turning a StackDriver logfile into a plain
/// textual logfile by pulling out the 'textPayload' for each entry
pub fn run(
    dir: &str,
    gql: &mut dyn Write,
    sql: &mut dyn Write,
    verbose: bool,
) -> Result<(), std::io::Error> {
    let json_ext = OsStr::new("json");
    let mut trimmed_count: usize = 0;
    let mut count: usize = 0;

    if dir == "-" {
        let stdin = io::stdin();
        let (cur_count, cur_trimmed_count) = extract(stdin, gql, sql)?;
        count += cur_count;
        trimmed_count += cur_trimmed_count;
    } else {
        for entry in WalkDir::new(dir) {
            let entry = entry?;

            if entry.file_type().is_file() && entry.path().extension() == Some(&json_ext) {
                if verbose {
                    eprintln!("Reading {}", entry.path().to_string_lossy());
                }
                let file = File::open(entry.path())?;

                let (cur_count, cur_trimmed_count) = extract(file, gql, sql)?;
                count += cur_count;
                trimmed_count += cur_trimmed_count;
            }
        }
    }
    eprintln!(
        "Skipped {} trimmed lines out of {} lines",
        trimmed_count, count
    );
    Ok(())
}
