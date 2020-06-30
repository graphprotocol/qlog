use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use walkdir::WalkDir;

use crate::common::{GQL_MARKER, SQL_MARKER, TRIMMED};

/// The 'extract' subcommand turning a StackDriver logfile into a plain
/// textual logfile by pulling out the 'textPayload' for each entry
pub fn run(
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
