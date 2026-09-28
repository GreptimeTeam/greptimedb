// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// For the given format: `x-greptime-hints: auto_create_table=true, ttl=7d`
pub const HINTS_KEY: &str = "x-greptime-hints";
/// Deprecated, use `HINTS_KEY` instead. Notes if "x-greptime-hints" is set, keys with this prefix will be ignored.
pub const HINTS_KEY_PREFIX: &str = "x-greptime-hint-";
pub const REMOTE_QUERY_ID_EXTENSION_KEY: &str = "remote_query_id";
pub const INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY: &str =
    "initial_remote_dyn_filter_registrations";
pub const SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY: &str =
    "query.support_flight_metrics_before_batch";
pub const LIVE_ANALYZE_METRICS_EXTENSION_KEY: &str = "query.live_analyze_metrics";

/// Skip WAL for this insert only; never persisted as a table option.
pub const INSERT_SKIP_WAL_HINT: &str = "insert_skip_wal";

pub const READ_PREFERENCE_HINT: &str = "read_preference";
/// The extension key of the marker that a query is an execution stage of another query, instead of
/// an independent query.
///
/// A datanode executes the `MergeScan` nodes of the plan it received by querying the regions of
/// those nodes from the datanodes hosting them. Such an inner region query is a stage of the outer
/// query: the outer query waits for it while holding its own concurrency permit, so the inner query
/// must not acquire a permit of its own, otherwise the outer query waits for the inner stage while
/// the inner stage waits for the permit held by the outer query. The marker travels to the peer
/// datanode as an extension of the query context of the `RegionRequestHeader` of the request.
///
/// The key is reserved (see [`RESERVED_EXTENSION_KEYS`]), so the hint path (`x-greptime-hints`)
/// can't set it: a SQL/PromQL client of the frontend can't make its own query bypass the
/// concurrency limiter of a datanode. The marker is trusted on the region query port of a datanode,
/// like the other extensions of the header and the plan itself.
///
/// Keep in sync with `common_query::request::QUERY_INTERNAL_STAGE_EXTENSION_KEY`
/// (`session` can't depend on `common-query`).
pub const QUERY_INTERNAL_STAGE_EXTENSION_KEY: &str = "query.internal_stage";
/// The value of [`QUERY_INTERNAL_STAGE_EXTENSION_KEY`] that marks an execution stage of another
/// query. Any other value, including an absent one, leaves the query subject to the concurrency
/// limiter.
///
/// Keep in sync with `common_query::request::QUERY_INTERNAL_STAGE_EXTENSION_VALUE`.
pub const QUERY_INTERNAL_STAGE_EXTENSION_VALUE: &str = "true";
pub const RESERVED_EXTENSION_KEYS: [&str; 5] = [
    REMOTE_QUERY_ID_EXTENSION_KEY,
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
    SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
    LIVE_ANALYZE_METRICS_EXTENSION_KEY,
    QUERY_INTERNAL_STAGE_EXTENSION_KEY,
];

/// Deprecated, use `HINTS_KEY` instead.
pub const HINT_KEYS: [&str; 7] = [
    "x-greptime-hint-auto_create_table",
    "x-greptime-hint-ttl",
    "x-greptime-hint-append_mode",
    "x-greptime-hint-merge_mode",
    "x-greptime-hint-physical_table",
    "x-greptime-hint-skip_wal",
    "x-greptime-hint-read_preference",
];

pub fn is_reserved_extension_key(key: &str) -> bool {
    RESERVED_EXTENSION_KEYS.contains(&key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_reserved_extension_key() {
        assert!(is_reserved_extension_key(REMOTE_QUERY_ID_EXTENSION_KEY));
        assert!(is_reserved_extension_key(
            INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY
        ));
        assert!(is_reserved_extension_key(
            SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY
        ));
        assert!(is_reserved_extension_key(
            LIVE_ANALYZE_METRICS_EXTENSION_KEY
        ));
        assert!(is_reserved_extension_key(
            QUERY_INTERNAL_STAGE_EXTENSION_KEY
        ));
        assert!(!is_reserved_extension_key(READ_PREFERENCE_HINT));
    }
}
