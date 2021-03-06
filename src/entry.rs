//! Representation of a single log entry
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry<'a> {
    pub subgraph: Cow<'a, str>,
    pub query_id: Cow<'a, str>,
    pub block: u64,
    pub time: u64,
    pub query: Cow<'a, str>,
    pub variables: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Cow<'a, str>>,
}

// Return the part of the line between `prefix` and `suffix`, with
// both of them not appearing in the result
fn field<'a>(line: &'a str, prefix: &str, suffix: &str) -> Option<Cow<'a, str>> {
    if let Some(start) = line.find(prefix) {
        if let Some(field) = line.get(start + prefix.len()..) {
            if let Some(end) = field.find(suffix) {
                return field.get(..end).map(|s| Cow::from(s));
            }
        }
    }
    None
}

// Same as `field`, but we search for `suffix` from the right
fn rfield<'a>(line: &'a str, prefix: &str, suffix: &str) -> Option<Cow<'a, str>> {
    if let Some(start) = line.find(prefix) {
        if let Some(field) = line.get(start + prefix.len()..) {
            if let Some(end) = field.rfind(suffix) {
                return field.get(..end).map(|s| Cow::from(s));
            }
        }
    }
    None
}

impl<'a> Entry<'a> {
    pub fn parse(line: &'a str, timestamp: Option<&'a str>) -> Option<Entry<'a>> {
        let block = field(line, "block: ", ",");
        let time = field(line, "query_time_ms: ", ",");
        let subgraph = field(line, "subgraph_id: ", ", component: ");
        let query_id = field(line, "query_id: ", ",");
        // This is unambiguous since formatted GraphQL queries do not
        // contain commas surrounded by whitespace. Since we search
        // the suffix from the right, we won't get confused by strings
        // in the query containing the suffix
        let query = rfield(line, "query: ", " , query_id:");
        // This is unambiguous since 'variables' is a JSON object and any
        // object key therefore is enclosed in quotes
        let variables = field(line, "variables: ", ", query: ");

        if let (
            Some(block),
            Some(query_time),
            Some(query),
            Some(variables),
            Some(query_id),
            Some(subgraph),
        ) = (block, time, query, variables, query_id, subgraph)
        {
            let block = block.parse().unwrap_or_else(|_| {
                eprintln!("invalid block: {}", block);
                0
            });
            let time: u64 = query_time.parse().unwrap_or_else(|_| {
                eprintln!("invalid query_time: {}", line);
                0
            });
            let timestamp = timestamp.map(|ts| Cow::from(ts));

            let entry = Entry {
                subgraph,
                query_id,
                block,
                time,
                query,
                variables,
                timestamp,
            };
            Some(entry)
        } else {
            None
        }
    }
}

pub trait EntryParser {
    fn parse<'a>(&self, line: &'a str) -> Option<Entry<'a>>;
}

pub struct TextEntryParser {}

impl EntryParser for TextEntryParser {
    fn parse<'a>(&self, line: &'a str) -> Option<Entry<'a>> {
        Entry::parse(line, None)
    }
}

pub struct JsonlEntryParser {}

impl EntryParser for JsonlEntryParser {
    fn parse<'a>(&self, line: &'a str) -> Option<Entry<'a>> {
        serde_json::from_str(line).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gql_query() {
        const LINE1: &str = "Dec 30 20:55:13.071 INFO Query timing (GraphQL), \
                             block: 10344025, \
                             query_time_ms: 160, \
                             variables: null, \
                             query: query Stuff { things } , \
                             query_id: f-1-4-b-e4, \
                             subgraph_id: QmSuBgRaPh, \
                             component: GraphQlRunner\n";
        const LINE2: &str = "Dec 31 23:59:59.667 INFO Query timing (GraphQL), \
                             block: 10344025, \
                             query_time_ms: 125, \
                             variables: {}, \
                             query: query { things(id:\"1\") { id }} , \
                             query_id: f2-6b-48-b6-6b, \
                             subgraph_id: QmSuBgRaPh, \
                             component: GraphQlRunner";
        const LINE3: &str = "Dec 31 23:59:59.739 INFO Query timing (GraphQL), \
                             block: 10344025, \
                             query_time_ms: 14, \
                             variables: null, \
                             query: query TranscoderQuery { transcoders(first: 1) { id } } , \
                             query_id: c5-d3-4e-92-37, \
                             subgraph_id: QmeYBGccAwahY, \
                             component: GraphQlRunner";
        const LINE4: &str = "Dec 31 23:59:59.846 INFO Query timing (GraphQL), \
             block: 10344025, \
             query_time_ms: 12, \
             variables: {\"id\":\"0xdeadbeef\"}, \
             query: query exchange($id: String!) { exchange(id: $id) { id tokenAddress } } , \
             query_id: c8-1c-4c-98-65, \
             subgraph_id: QmSuBgRaPh, \
             component: GraphQlRunner";

        const LINE5: &str = "Dec 31 22:59:58.863 INFO Query timing (GraphQL), \
                             block: 1234, \
                             query_time_ms: 2657, \
                             variables: {\"_v1_first\":100,\"_v2_where\":{\"status\":\"Registered\"},\"_v0_skip\":0}, \
                             query: query TranscodersQuery($_v0_skip: Int, $_v1_first: Int, $_v2_where: Transcoder_filter) { transcoders(where: $_v2_where, skip: $_v0_skip, first: $_v1_first) { ...TranscoderFragment __typename } }  fragment TranscoderFragment on Transcoder { id active status lastRewardRound { id __typename } rewardCut feeShare pricePerSegment pendingRewardCut pendingFeeShare pendingPricePerSegment totalStake pools(orderBy: id, orderDirection: desc) { rewardTokens round { id __typename } __typename } __typename } , \
                             query_id: 2d-12-4b-a8-6b, \
                             subgraph_id: QmSuBgRaPh, \
                             component: GraphQlRunner";

        const LINE6: &str = "Jun 26 22:12:02.295 INFO Query timing (GraphQL), \
                             complexity: 4711, \
                             block: 10344025, \
                             query_time_ms: 10, \
                             variables: null, \
                             query: { rateUpdates(orderBy: timestamp, orderDirection: desc, where: {synth: \"sEUR\", timestamp_gte: 1593123133, timestamp_lte: 1593209533}, first: 1000, skip: 0) { id synth rate block timestamp } } , \
                             query_id: cb9af68f-ae60-4dba-b9b3-89aee6fe8eca, \
                             subgraph_id: QmaSubgraph, component: GraphQlRunner";

        // Ignore this; it only differs in complexity from LINE6, and we don't
        // process complexity
        const _LINE7: &str = "Jun 26 22:12:02.295 INFO Query timing (GraphQL), \
                             complexity: 0, \
                             block: 10344025, \
                             query_time_ms: 10, \
                             variables: null, \
                             query: { rateUpdates(orderBy: timestamp, orderDirection: desc, where: {synth: \"sEUR\", timestamp_gte: 1593123133, timestamp_lte: 1593209533}, first: 1000, skip: 0) { id synth rate block timestamp } } , \
                             query_id: cb9af68f-ae60-4dba-b9b3-89aee6fe8eca, \
                             subgraph_id: QmaSubgraph, component: GraphQlRunner";

        // Ignore this; it only differs in complexity from LINE6, and we don't
        // process complexity
        const _LINE8: &str = "Jun 25 10:00:00.074 INFO Query timing (GraphQL), \
                             block: 10334284, \
                             query_time_ms: 7, \
                             variables: null, \
                             query: { rateUpdates(orderBy: timestamp, orderDirection: desc, where: {synth: \"sUSD\", timestamp_gte: 1592992799, timestamp_lte: 1593079199}, first: 1000, skip: 0) { id synth rate block timestamp } } , \
                             query_id: e020b60e-478f-41ce-b555-82d1ad88050b, \
                             subgraph_id: QmaSubgraph, component: GraphQlRunner";

        let exp = Entry {
            subgraph: "QmSuBgRaPh".into(),
            block: 10344025,
            time: 160,
            query: "query Stuff { things }".into(),
            variables: "null".into(),
            query_id: "f-1-4-b-e4".into(),
            timestamp: None,
        };
        let entry = Entry::parse(LINE1, None);
        assert_eq!(Some(exp), entry);

        let exp = Entry {
            subgraph: "QmSuBgRaPh".into(),
            block: 10344025,
            time: 125,
            query: "query { things(id:\"1\") { id }}".into(),
            variables: "{}".into(),
            query_id: "f2-6b-48-b6-6b".into(),
            timestamp: None,
        };
        let entry = Entry::parse(LINE2, None);
        assert_eq!(Some(exp), entry);

        let exp = Entry {
            subgraph: "QmeYBGccAwahY".into(),
            block: 10344025,
            time: 14,
            query: "query TranscoderQuery { transcoders(first: 1) { id } }".into(),
            variables: "null".into(),
            query_id: "c5-d3-4e-92-37".into(),
            timestamp: None,
        };
        let entry = Entry::parse(LINE3, None);
        assert_eq!(Some(exp), entry);

        let exp = Entry {
            subgraph: "QmSuBgRaPh".into(),
            block: 10344025,
            time: 12,
            query: "query exchange($id: String!) { exchange(id: $id) { id tokenAddress } }".into(),
            variables: "{\"id\":\"0xdeadbeef\"}".into(),
            query_id: "c8-1c-4c-98-65".into(),
            timestamp: None,
        };
        let entry = Entry::parse(LINE4, None);
        assert_eq!(Some(exp), entry);

        let exp = Entry {
            subgraph: "QmSuBgRaPh".into(),
            block: 1234,
            time: 2657,
            query: "query TranscodersQuery($_v0_skip: Int, $_v1_first: Int, $_v2_where: Transcoder_filter) { transcoders(where: $_v2_where, skip: $_v0_skip, first: $_v1_first) { ...TranscoderFragment __typename } }  fragment TranscoderFragment on Transcoder { id active status lastRewardRound { id __typename } rewardCut feeShare pricePerSegment pendingRewardCut pendingFeeShare pendingPricePerSegment totalStake pools(orderBy: id, orderDirection: desc) { rewardTokens round { id __typename } __typename } __typename }".into(),
            variables: "{\"_v1_first\":100,\"_v2_where\":{\"status\":\"Registered\"},\"_v0_skip\":0}".into(),
            query_id: "2d-12-4b-a8-6b".into(),
            timestamp: None
        };
        let entry = Entry::parse(LINE5, None);
        assert_eq!(Some(exp), entry);

        let exp = Entry {
            subgraph: "QmaSubgraph".into(),
            block: 10344025,
            time: 10,
            query: "{ rateUpdates(orderBy: timestamp, orderDirection: desc, where: {synth: \"sEUR\", timestamp_gte: 1593123133, timestamp_lte: 1593209533}, first: 1000, skip: 0) { id synth rate block timestamp } }".into(),
            variables: "null".into(),
            query_id: "cb9af68f-ae60-4dba-b9b3-89aee6fe8eca".into(),
            timestamp: None
        };
        let entry = Entry::parse(LINE6, None);
        assert_eq!(Some(exp), entry);
    }
}
