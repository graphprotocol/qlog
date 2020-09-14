use rand::{prelude::Rng, rngs::SmallRng, SeedableRng};
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};

use crate::common::{INDEX_NODE_SUBGRAPH, SUBGRAPHS_SUBGRAPH};

struct Sample {
    query: String,
    variables: String,
}

impl Sample {
    fn new(query: &str, variables: &str) -> Self {
        Sample {
            query: query.to_owned(),
            variables: variables.to_owned(),
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
    fn sample(&mut self, size: usize, rng: &mut SmallRng, query: &str, variables: &str) {
        let hash = {
            let mut hasher = DefaultHasher::new();
            (query, variables).hash(&mut hasher);
            hasher.finish()
        };

        // We sample distinct queries
        if !self.seen.contains(&hash) {
            // Sample uniformly, i.e. if there are N distinct queries for a
            // subgraph in the file we are processing, the probabilty that any
            // one query winds up in the sample is `size/N`
            if self.seen_count < size {
                self.samples.push(Sample::new(query, variables));
            } else {
                let k = rng.gen_range(0, self.seen_count + 1);
                if k < size {
                    let samples = Sample::new(query, variables);
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
}

impl Sampler {
    pub fn new(size: usize, subgraphs: HashSet<String>) -> Self {
        Sampler {
            size,
            rng: SmallRng::from_entropy(),
            samples: BTreeMap::new(),
            subgraphs,
        }
    }

    pub fn sample<'b>(&mut self, query: &str, variables: &str, subgraph: &'b str) {
        if self.size == 0
            || subgraph == INDEX_NODE_SUBGRAPH
            || subgraph == SUBGRAPHS_SUBGRAPH
            || (!self.subgraphs.is_empty() && !self.subgraphs.contains(subgraph))
        {
            return;
        }

        let domain = {
            match self.samples.get_mut(subgraph) {
                Some(samples) => samples,
                None => self.samples.entry(subgraph.to_owned()).or_default(),
            }
        };

        domain.sample(self.size, &mut self.rng, query, variables);
    }

    pub fn write(&self, mut out: BufWriter<File>) -> Result<(), std::io::Error> {
        #[derive(Serialize)]
        struct SampleOutput<'a> {
            subgraph: &'a String,
            query: &'a String,
            variables: &'a String,
        }
        for (subgraph, domain) in &self.samples {
            for sample in &domain.samples {
                let v = SampleOutput {
                    subgraph,
                    query: &sample.query,
                    variables: &sample.variables,
                };
                writeln!(out, "{}", serde_json::to_string(&v)?)?;
            }
        }
        Ok(())
    }
}
