This repo contains a tool to process and summarize the query logs that
[graph-node](https://github.com/graphprotocol/graph-node) generates. It can
be used to

* transform the textual log files that `graph-node` poduces into JSONL
  files with one JSON object for each query. The input can either be the
  raw log from `graph-node` or the log entries that Google Cloud's
  Stackdriver emits. The JSONL files can then be used as input to
  [agora](https://github.com/graphprotocol/agora/)
* summarize query performance for similar queries to calculate basic
  statistics like average and maximum query duration
* sample query logs to generate a random sample of a larger logfile

## Installation

You will need to have a [Rust toolchain installed ](https://rustup.rs/)
Check out this repository, `cd` into the checkout and run

```
cargo install --bins --path .
```

The resulting binary will be placed into `~/.cargo/bin`. If you want to
update an existing installation, you will also have to pass `--force` to
`cargo`.


## Gathering query logs

When you run `graph-node`, set `GRAPH_LOG_QUERY_TIMING` to `gql`. That will
make `graph-node` log lines of the form

```
Sep 22 17:01:52.521 INFO Query timing (GraphQL), block: 1234567, query_time_ms: 489, variables: null, query: query things { things(first: 10) { id name } } , query_id: f3e751f1852e62e6-cf1efbcd35771aeb, subgraph_id: Qmsubgraph, component: GraphQlRunner
```

`qlog` can then be used to transform and summarize these logs in a number
of ways

## Processing query logs

In its simplest form, `qlog process` reads a textfile containing log
entries and transforms them into line-separated JSON (JSONL). To do that,
run
```
grep 'Query timing' queries.log | qlog process --text --output queries.jsonl
```

In addition, `qlog` can also summarize queries by grouping similar queries
by their 'shape hash' by passing `--graphql summary.jsonl`, for example to
just summarize queries, run

```
grep 'Query timing' queries.log | qlog process --text --graphql summary.jsonl
```

If there is already a file with queries in JSONL form, `qlog process` can
summarize this with the following command. Note that we do not pass
`--text` to `qlog process` since the input file is already in JSONL form:

```
cat queries.jsonl | qlog process --graphql summary.jsonl
```

Finally, `qlog process` can also be used to take fixed-size samples of a
logfile; running `qlog process` like this will produce a file
`samples.jsonl` that contains 1,000 samples per subgraph taken
uniformly from `queries.jsonl`:

```
cat queries.jsonl | qlog process --samples 1000 --sample-file samples.jsonl
```
Sampling can also be restricted to certain subgraphs with the
`--sample-subgraphs` option which expects a comma-separated list of
subgraph identifiers, i.e. identifiers in the form `Qmsubgraph`.

The options for converting a logfile to JSONL, to summarize a logfile, and
to sample a logfile can be combined so that `qlog process` only needs to be
run over a logfile once, which saves a significant amount of time for large
logfiles:

```
grep 'Query timing' queries.log | \
  qlog process --text --graphql summary.jsonl \
     --output queries.jsonl \
     --samples 1000 --sample-file samples.jsonl
```

### Using `qlog` with Google Cloud

Google Cloud's logging infrastructure, Stackdriver, wraps log messages in
JSON objects. It is possible to have these JSON objects delivered into
files in Google Cloud storage by defining a sink in Google Cloud Logging.

The command `qlog extract` can be used to parse such files and turn them
into JSONL log files, similar to what `qlog process` with the `--output`
option produces. Run

```
cat stackdriver.log | qlog extract --graphql queries.jsonl -
```
to turn a StackDriver logfile into a JSONL logfile that `qlog process` can
summarize and sample.


## Combining query summaries

The command `qlog combine` can be used to combine multiple summary files
into one by running `qlog combine summary1.jsonl summary2.jsonl ... >
summary.jsonl`


## Analysing query logs

With a summary file `summary.jsonl` produced by `qlog process` in hand, it
is possible to perform simple analysis tasks with `qlog stats` and `qlog
query`. For example, the command
```
qlog stats -s total -f summary.jsonl

```
will list query details sorted by their total time.

`qlog stats` can also provide an overview of queries in tabular form, for
example by running `qlog stats summary.json | head -n 10`:

```console
|   QID   |  calls   |    total     |  avg   | stddev |  max   |  slow  |
|---------+----------+--------------+--------+--------+--------+--------|
| Q000019 |   933741 |    430758440 |    461 |    284 |   7998 |  30440 |
| Q000511 |  3153187 |    412610852 |    131 |    137 |   3991 |  13506 |
| Q000534 |   360948 |    152649245 |    423 |    282 |  10689 |   9162 |
| Q000343 |  1291109 |     35364900 |     27 |     40 |   2918 |     19 |
| Q000829 |     4269 |     33130650 |   7761 |   1116 |  17533 |   4269 |
| Q000527 |   785074 |     32840740 |     42 |     45 |   1974 |     27 |
| Q001673 |    24366 |     27497870 |   1129 |    492 |   7168 |  10180 |
| Q001453 |   106500 |     24405334 |    229 |    361 |   6884 |   2827 |
```

The `query` subcommand can be used to print more details about a specific
query (the leading `0` in the `QNNN` identifier are optional):

```console
host:qlog>qlog query summary.json Q558 Q333
================================ Q558 ================================
# subgraph:      QmaTK1m8VszFp7iijbWrX65iN8e5zogvJYvUAck7HEvAtQ
# calls:                      1
# slow_count:                 1
# slow_percent:          100.00 %
# total_time:              11.5 m
# avg_time:              692850 ms
# stddev_time:                0 ms
# max_time:              692850 ms
# max_uuid:      160e5f6b-f230-428a-bd3b-8cae6cbb5d89

query getTokens($pageSize: Int = 50) { tokens(first: $pageSize) { address name symbol decimals events { ... on TransferEvent { amount sender destination } } } }

================================ Q333 ================================
# subgraph:      QmVEoWSQ8eNnVkL1uw2toDSc1xXKi1ADYUm36p961Q1EzT
# calls:                    692
# slow_count:                10
# slow_percent:            1.45 %
# total_time:              18.7 m
# avg_time:                1625 ms
# stddev_time:            14798 ms
# max_time:              158087 ms
# max_uuid:      e956ea45-807d-43bb-ad4b-4adcd04bc281

query getSubdomains($id: ID!) { domain(id: $id) { id labelName subdomains { id labelName labelhash name owner { id __typename } __typename } __typename } }
```

### Using `jq` for simple analysis

If we have a directory with summary files `YYYY-MM-DD.jsonl` for each day,
we can list the total number of queries per day using `jq`:

```bash
for f in *.jsonl
do
  day=$(basename $f .jsonl)
  gql=$(jq -s 'map(.calls) | add ' < $f)
  printf "%s %10s\n" "$day" "$gql"
done
```

This saves the number of queries per subgraph per day to a csv:
```bash
summaries=""
for f in *.jsonl
do
  day=$(basename $f .json)
  day_summary=$(jq -s 'group_by(.subgraph) |
    map({"subgraph":(.[0].subgraph), "calls":(reduce .[].calls as $calls (0; . + $calls)), "date":("'$day'") }) |
    map([.subgraph, .calls, .date] | join(", ")) | join(" \r\n ")' < $f)
  summaries+="${day_summary//\"} \r\n"
done
printf "${summaries//\"}" > subgraph_daily_summary.csv
```

### Format of the JSONL file

The JSONL files that `qlog process --graphql` produces contain a list of
JSON objects, one for each query that `graph-node` responded to with the
following entries:

* `subgraph`: the IPFS hash of the subgraph against which the query was run
* `query_id`: the entry is in the form `<shape hash>-<query hash>` where
  the shape hash is the hash of the query text when disregarding concrete
  filter values in the query, so that a query with `first: 10` and one with
  `first: 100` produce the same shape hash. THe `query hash` is the hash of
  the query text and the variables passed to the query.
* `block`: the number of the block against which the query was executed
* `time`: how long processing the query took in ms
* `query`: the text of the GraphQL query
* `variables`: the variables used in the query as a string that is a JSON
  object in its own right
* `timestamp`: the server time when the query was run

The summary JSONL files produced with `qlog process --summary` contain JSON
objects with the following entries:

* `query`: an example of the query being summarized. Queries are summarized
  by their shape hash, i.e. the summary file contains one entry for each
  unique shape in the original log file
* `subgraph`: the IPFS hash of the subgraph
* `calls`: the number of times this query shape was executed
* `slow_count`: the number of executions of this query shape that took more
  than 1s
* `total_time`: the sum of the execution time of all queries being
  summarized in ms
* `time_squared`: the sum of the square of the execution time of queries in
  ms^2
* `max_time`: the time that the slowest execution of this query shape took
  in ms
* `max_uuid`: the `query_id` of a query that took `max_time` (it's called
  `uuid` for historical reasons)
* `max_variables`: the variables that were passed to the query when it took
  `max_time`
* `max_complexity`: meaningless; only there for historical reasons
* `id`, `hash`: used by `qlog` for internal bookkeeping

Average query execution time can be calculated from this data as
`total_time / calls` and the standard deviation as `sqrt(time_squared /
calls - (total_time/calls)^2)`


### Write to Postgres

For more complex analysis, it is best to load summaries into a dedicated
Postgres database. In the following, we assume that the environment is set
up in such a way that running `psql` will connect to that dedicated
database. The command `./bin/load -c` will create a database schema `qlog`
for that purpose.

Assuming there is a directory `daily-summaries` with one summary file
called `YYYY-MM-DD.jsonl` for each day, simply run
```
./bin/load daily-summaries/*.jsonl
```

Subsequent days can be added to the database with `./bin/load
daily-summaries/2020-11-01.jsonl`

#### View data


Once the summary data is loaded into Postgres, we can run even more complex
queries, for example, to find the subgraphs causing the most work:
```console
host:qlog> psql -c '
> select subgraph,
>        sum(total_time::numeric)/(24*60*60*1000) as total_time_days,
>        sum(calls) as calls
>   from qlog.data
>  group by subgraph
>  order by 2 desc
>  limit 5;'
```

# Copyright

Copyright &copy; 2021 The Graph Foundation

Licensed under the [MIT license](LICENSE).
