This repo contains a tool to process and summarize the query logs that
[graph-node](https://github.com/graphprotocol/graph-node) generates.

## Installation

```
cargo install --bins --path .
```

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
