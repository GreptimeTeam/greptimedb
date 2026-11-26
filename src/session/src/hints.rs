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
pub const READ_PREFERENCE_EXTENSION_KEY: &str = "query.read_preference";
pub const RESERVED_EXTENSION_KEYS: [&str; 5] = [
    REMOTE_QUERY_ID_EXTENSION_KEY,
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
    SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
    LIVE_ANALYZE_METRICS_EXTENSION_KEY,
    READ_PREFERENCE_EXTENSION_KEY,
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

/// Prefix of the query-context extension keys that carry a series-sampling hint.
///
/// One key per sampled table: `greptime.series_sample.<table_id>` with the value
/// `"<num>/<den>"` (see [`series_sample_extension_key`]). The fraction has to
/// reach the region scan the frontend planned, and the query context is the
/// only part of a region request that carries per-query hints, so the key is
/// reserved instead of being validated as a query option.
pub const SERIES_SAMPLE_EXTENSION_KEY_PREFIX: &str = "greptime.series_sample";

/// Returns the query-context extension key that carries the series sample of `table_id`.
pub fn series_sample_extension_key(table_id: u32) -> String {
    format!("{SERIES_SAMPLE_EXTENSION_KEY_PREFIX}.{table_id}")
}

/// Parses the value of a series sample extension key (`"<num>/<den>"`).
pub fn parse_series_sample_fraction(value: &str) -> Option<(u32, u32)> {
    let (num, den) = value.trim().split_once('/')?;
    Some((num.trim().parse().ok()?, den.trim().parse().ok()?))
}

pub fn is_reserved_extension_key(key: &str) -> bool {
    RESERVED_EXTENSION_KEYS.contains(&key) || is_series_sample_extension_key(key)
}

fn is_series_sample_extension_key(key: &str) -> bool {
    key.strip_prefix(SERIES_SAMPLE_EXTENSION_KEY_PREFIX)
        .and_then(|rest| rest.strip_prefix('.'))
        .is_some_and(|table_id| table_id.parse::<u32>().is_ok())
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
        assert!(is_reserved_extension_key(READ_PREFERENCE_EXTENSION_KEY));
        assert!(!is_reserved_extension_key(READ_PREFERENCE_HINT));
        // Series-sampling hints are reserved per table, and only for a numeric id.
        assert!(is_reserved_extension_key(&series_sample_extension_key(42)));
        assert!(!is_reserved_extension_key(
            SERIES_SAMPLE_EXTENSION_KEY_PREFIX
        ));
        assert!(!is_reserved_extension_key(&format!(
            "{SERIES_SAMPLE_EXTENSION_KEY_PREFIX}.abc"
        )));
    }

    #[test]
    fn test_series_sample_fraction_roundtrip() {
        assert_eq!(Some((1, 16)), parse_series_sample_fraction("1/16"));
        assert_eq!(Some((1, 16)), parse_series_sample_fraction(" 1 / 16 "));
        assert_eq!(None, parse_series_sample_fraction("1"));
        assert_eq!(None, parse_series_sample_fraction("1/x"));
        assert_eq!(series_sample_extension_key(42), "greptime.series_sample.42");
    }
}
