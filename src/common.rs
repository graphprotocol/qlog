/// The index-node status API and the API for the subgraph of subgraphs logs
/// requests under these subgraph names. We ignore them when we sample queries
pub const INDEX_NODE_SUBGRAPH: &str = "indexnode";
pub const SUBGRAPHS_SUBGRAPH: &str = "subgraphs";

/// When a log line contains this text, we know it's about a GraphQL
/// query
pub const GQL_MARKER: &str = "Query timing (GraphQL)";

/// When a log line contains this text, we know it's about a SQL
/// query
pub const SQL_MARKER: &str = "Query timing (SQL)";

/// StackDriver prefixes lines with this when they were too long, and then
/// shortens the line
pub const TRIMMED: &str = "[Trimmed]";
