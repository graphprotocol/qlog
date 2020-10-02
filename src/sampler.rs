use rand::{prelude::Rng, rngs::SmallRng, SeedableRng};
use serde::Serialize;
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};

use crate::common::{INDEX_NODE_SUBGRAPH, SUBGRAPHS_SUBGRAPH};
use crate::Entry;

#[derive(Serialize)]
struct Sample {
    query: String,
    variables: String,
    query_id: String,
    block: u64,
    time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
}

impl<'a> From<&Entry<'a>> for Sample {
    fn from(entry: &Entry) -> Self {
        Self {
            query: entry.query.to_string(),
            variables: entry.variables.to_string(),
            query_id: entry.query_id.to_string(),
            block: entry.block,
            time: entry.time,
            timestamp: entry.timestamp.as_ref().map(|s| s.to_string()),
        }
    }
}

/// A collection of query samples; we use one of these for each subgraph.
struct SampleDomain {
    /// The total number of unique queries we have seen
    seen_count: usize,
    /// The hashes of unique `(query, variables)` combinations
    seen: HashSet<u64>,
    /// Up to `Sampler.size` distinct samples
    samples: Vec<Sample>,
}

impl Default for SampleDomain {
    fn default() -> Self {
        SampleDomain {
            seen_count: 0,
            seen: HashSet::default(),
            samples: Vec::default(),
        }
    }
}

impl SampleDomain {
    /// If we have not seen `(query, variables)` before, add them to our samples
    /// so that in the end the probability that any unique query is in our
    /// final sample is `size / N` where `N` is the number of distinct queries
    fn sample(&mut self, size: usize, rng: &mut SmallRng, entry: &Entry) {
        let hash = {
            let mut hasher = DefaultHasher::new();
            (&entry.query, &entry.variables).hash(&mut hasher);
            hasher.finish()
        };

        // We sample distinct queries
        if !self.seen.contains(&hash) {
            // Sample uniformly, i.e. if there are N distinct queries for a
            // subgraph in the file we are processing, the probabilty that any
            // one query winds up in the sample is `size/N`
            if self.seen_count < size {
                self.samples.push(Sample::from(entry));
            } else {
                let k = rng.gen_range(0, self.seen_count + 1);
                if k < size {
                    let samples = Sample::from(entry);
                    if let Some(entry) = self.samples.get_mut(k) {
                        *entry = samples;
                    }
                }
            }
            self.seen_count += 1;
            self.seen.insert(hash);
        }
    }
}

pub struct Sampler {
    size: usize,
    rng: SmallRng,
    samples: BTreeMap<String, SampleDomain>,
    subgraphs: HashSet<String>,
    out: BufWriter<File>,
}

impl Sampler {
    pub fn new(size: usize, subgraphs: HashSet<String>, out: BufWriter<File>) -> Self {
        Sampler {
            size,
            rng: SmallRng::from_entropy(),
            samples: BTreeMap::new(),
            subgraphs,
            out,
        }
    }

    pub fn sample<'b>(&mut self, entry: &Entry) {
        if self.size == 0
            || entry.subgraph == INDEX_NODE_SUBGRAPH
            || entry.subgraph == SUBGRAPHS_SUBGRAPH
            || (!self.subgraphs.is_empty() && !self.subgraphs.contains(entry.subgraph.as_ref()))
        {
            return;
        }

        let domain = {
            match self.samples.get_mut(entry.subgraph.as_ref()) {
                Some(samples) => samples,
                None => self.samples.entry(entry.subgraph.to_string()).or_default(),
            }
        };

        domain.sample(self.size, &mut self.rng, entry);
    }

    pub fn write(&mut self) -> Result<(), std::io::Error> {
        if self.size <= 0 {
            return Ok(());
        }

        for (subgraph, domain) in &self.samples {
            for sample in &domain.samples {
                let subgraph = Cow::from(subgraph);
                let entry = Entry {
                    subgraph,
                    query_id: Cow::from(&sample.query_id),
                    block: sample.block,
                    time: sample.time,
                    query: Cow::from(&sample.query),
                    variables: Cow::from(&sample.variables),
                    timestamp: sample.timestamp.as_ref().map(|s| Cow::from(s)),
                };
                writeln!(self.out, "{}", serde_json::to_string(&entry)?)?;
            }
        }
        Ok(())
    }
}
