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
use tonic::metadata::MetadataMap;

// For the given format: `x-greptime-hints: auto_create_table=true, ttl=7d`
pub const HINTS_KEY: &str = "x-greptime-hints";

pub const READ_PREFERENCE_HINT: &str = "read_preference";

const HINT_KEYS: [&str; 7] = [
    "x-greptime-hint-auto_create_table",
    "x-greptime-hint-ttl",
    "x-greptime-hint-append_mode",
    "x-greptime-hint-merge_mode",
    "x-greptime-hint-physical_table",
    "x-greptime-hint-skip_wal",
    "x-greptime-hint-read_preference",
];

pub(crate) fn extract_hints<T: ToHeaderMap>(headers: &T) -> Vec<(String, String)> {
    let mut hints = Vec::new();
    if let Some(value_str) = headers.get(HINTS_KEY) {
        value_str.split(',').for_each(|hint| {
            let mut parts = hint.splitn(2, '=');
            if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                hints.push((key.trim().to_string(), value.trim().to_string()));
            }
        });
        // If hints are provided in the `x-greptime-hints` header, ignore the rest of the headers
        return hints;
    }
    for key in HINT_KEYS.iter() {
        if let Some(value) = headers.get(key) {
            let new_key = key.replace("x-greptime-hint-", "");
            hints.push((new_key, value.trim().to_string()));
        }
    }
    hints
}

pub(crate) trait ToHeaderMap {
    fn get(&self, key: &str) -> Option<&str>;
}

impl ToHeaderMap for MetadataMap {
    fn get(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.to_str().ok())
    }
}

impl ToHeaderMap for HeaderMap {
    fn get(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.to_str().ok())
    }
}
#[cfg(test)]
mod tests {
    use http::header::{HeaderMap, HeaderValue};
    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::*;

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

        assert_eq!(hints.len(), 6);
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
            read_preference=leader"),
        );

        let hints = extract_hints(&headers);

        assert_eq!(hints.len(), 6);
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

        assert_eq!(hints.len(), 6);
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
