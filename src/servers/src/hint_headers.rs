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

use http::HeaderMap;
use session::hints::HINTS_KEY;
use tonic::metadata::MetadataMap;

pub(crate) fn extract_hints<T: ToHeaderMap>(headers: &T) -> Vec<(String, String)> {
    let mut hints = Vec::new();
    if let Some(value_str) = headers.get(HINTS_KEY) {
        for hint in value_str
            .split(',')
            .map(str::trim)
            .filter(|hint| !hint.is_empty())
        {
            let (name, value) = hint.split_once('=').unwrap_or((hint, ""));
            hints.push((name.trim().to_string(), value.trim().to_string()));
        }
        // If hints are provided in the `x-greptime-hints` header, ignore the rest of the headers
        return hints;
    }
    for (header, name) in crate::hints::compatibility::legacy_headers(T::HTTP) {
        if let Some(value) = headers.get(header) {
            hints.push((name.to_string(), value.trim().to_string()));
        }
    }
    hints
}

/// Filters reserved keys and the legacy server-default sentinel before parsing.
pub(crate) fn filter_public_hints(
    hints: impl IntoIterator<Item = (String, String)>,
) -> impl Iterator<Item = (String, String)> {
    hints.into_iter().filter(|(name, value)| {
        !session::hints::is_reserved_extension_key(name)
            && !(name.eq_ignore_ascii_case("query_parallelism") && value == "0")
    })
}

pub(crate) trait ToHeaderMap {
    const HTTP: bool;
    fn get(&self, key: &str) -> Option<&str>;
}

impl ToHeaderMap for MetadataMap {
    const HTTP: bool = false;
    fn get(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.to_str().ok())
    }
}

impl ToHeaderMap for HeaderMap {
    const HTTP: bool = true;
    fn get(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.to_str().ok())
    }
}
#[cfg(test)]
mod tests {
    use http::header::{HeaderMap, HeaderValue};
    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::*;

    fn assert_legacy_hint_values(hints: Vec<(String, String)>) {
        let expected = [
            ("auto_create_table", "true"),
            ("ttl", "3600d"),
            ("append_mode", "true"),
            ("merge_mode", "false"),
            ("physical_table", "table1"),
            ("read_preference", "leader"),
        ];
        assert_eq!(hints.len(), expected.len());
        let values: std::collections::BTreeMap<_, _> = hints
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(values, expected.into_iter().collect());
    }

    #[test]
    fn test_extract_missing_values_is_independent_of_hint_names() {
        let value = ", query.parallelism, insert_skip_wal, read_preference, custom.option, , custom.payload=a=b, ";
        let mut headers = HeaderMap::new();
        headers.insert(HINTS_KEY, HeaderValue::from_static(value));
        let mut metadata = MetadataMap::new();
        metadata.insert(HINTS_KEY, MetadataValue::from_static(value));
        let expected: Vec<_> = [
            ("query.parallelism", ""),
            ("insert_skip_wal", ""),
            ("read_preference", ""),
            ("custom.option", ""),
            ("custom.payload", "a=b"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        assert_eq!(extract_hints(&headers), expected);
        assert_eq!(extract_hints(&metadata), expected);
    }

    #[test]
    fn test_extract_skip_wal_hint() {
        use session::hints::INSERT_SKIP_WAL_HINT;

        let mut headers = HeaderMap::new();
        headers.insert(HINTS_KEY, HeaderValue::from_static("insert_skip_wal=true"));
        let mut metadata = MetadataMap::new();
        metadata.insert(
            HINTS_KEY,
            MetadataValue::from_static("insert_skip_wal=true"),
        );
        let expected = vec![(INSERT_SKIP_WAL_HINT.to_string(), "true".to_string())];
        assert_eq!(extract_hints(&headers), expected);
        assert_eq!(extract_hints(&metadata), expected);
    }

    #[test]
    fn test_extract_hints_with_full_header_map() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-greptime-hint-auto_create_table",
            HeaderValue::from_static("true"),
        );
        headers.insert("x-greptime-hint-ttl", HeaderValue::from_static("3600d"));
        headers.insert(
            "x-greptime-hint-append_mode",
            HeaderValue::from_static("true"),
        );
        headers.insert(
            "x-greptime-hint-merge_mode",
            HeaderValue::from_static("false"),
        );
        headers.insert(
            "x-greptime-hint-physical_table",
            HeaderValue::from_static("table1"),
        );
        headers.insert(
            "x-greptime-hint-read_preference",
            HeaderValue::from_static("leader"),
        );

        let hints = extract_hints(&headers);

        assert_legacy_hint_values(hints);
    }

    #[test]
    fn test_extract_hints_with_missing_keys() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-greptime-hint-auto_create_table",
            HeaderValue::from_static("true"),
        );
        headers.insert("x-greptime-hint-ttl", HeaderValue::from_static("3600d"));

        let hints = extract_hints(&headers);

        assert_eq!(hints.len(), 2);
        assert_eq!(
            hints[0],
            ("auto_create_table".to_string(), "true".to_string())
        );
        assert_eq!(hints[1], ("ttl".to_string(), "3600d".to_string()));
    }

    #[test]
    fn test_extract_hints_all_in_one() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-greptime-hints",
            HeaderValue::from_static(" auto_create_table=true, ttl =3600d, append_mode=true , merge_mode=false , physical_table= table1,\
            read_preference=leader, compaction.twcs.time_window=1h"),
        );

        let hints = extract_hints(&headers);

        assert_eq!(hints.len(), 7);
        assert_eq!(
            hints[0],
            ("auto_create_table".to_string(), "true".to_string())
        );
        assert_eq!(hints[1], ("ttl".to_string(), "3600d".to_string()));
        assert_eq!(hints[2], ("append_mode".to_string(), "true".to_string()));
        assert_eq!(hints[3], ("merge_mode".to_string(), "false".to_string()));
        assert_eq!(
            hints[4],
            ("physical_table".to_string(), "table1".to_string())
        );
        assert_eq!(
            hints[5],
            ("read_preference".to_string(), "leader".to_string())
        );
        assert_eq!(
            hints[6],
            ("compaction.twcs.time_window".to_string(), "1h".to_string())
        );
    }

    #[test]
    fn test_extract_hints_with_metadata_map() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "x-greptime-hint-auto_create_table",
            MetadataValue::from_static("true"),
        );
        metadata.insert("x-greptime-hint-ttl", MetadataValue::from_static("3600d"));
        metadata.insert(
            "x-greptime-hint-append_mode",
            MetadataValue::from_static("true"),
        );
        metadata.insert(
            "x-greptime-hint-merge_mode",
            MetadataValue::from_static("false"),
        );
        metadata.insert(
            "x-greptime-hint-physical_table",
            MetadataValue::from_static("table1"),
        );
        metadata.insert(
            "x-greptime-hint-read_preference",
            MetadataValue::from_static("leader"),
        );

        let hints = extract_hints(&metadata);

        assert_legacy_hint_values(hints);
    }

    #[test]
    fn test_extract_hints_with_partial_metadata_map() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "x-greptime-hint-auto_create_table",
            MetadataValue::from_static("true"),
        );
        metadata.insert("x-greptime-hint-ttl", MetadataValue::from_static("3600d"));

        let hints = extract_hints(&metadata);

        assert_eq!(hints.len(), 2);
        assert_eq!(
            hints[0],
            ("auto_create_table".to_string(), "true".to_string())
        );
        assert_eq!(hints[1], ("ttl".to_string(), "3600d".to_string()));
    }
}
