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
use session::hints::{HINT_KEYS, HINTS_KEY, HINTS_KEY_PREFIX};
use tonic::metadata::MetadataMap;

use crate::error::{InvalidParameterSnafu, Result};

pub(crate) fn extract_hints<T: ToHeaderMap>(headers: &T) -> Vec<(String, String)> {
    let mut hints = Vec::new();
    if let Some(value_str) = headers.get(HINTS_KEY) {
        value_str.split(',').for_each(|hint| {
            let mut parts = hint.splitn(2, '=');
            if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                hints.push((key.trim().to_string(), value.trim().to_string()));
            } else {
                let key = hint.trim();
                if owns_hint_namespace(key) {
                    hints.push((key.to_string(), String::new()));
                }
            }
        });
        return hints;
    }
    for key in HINT_KEYS.iter() {
        if let Some(value) = headers.get(key) {
            let new_key = key.replace(HINTS_KEY_PREFIX, "");
            hints.push((new_key, value.trim().to_string()));
        }
    }
    hints
}

fn owns_hint_namespace(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.starts_with("query.")
        || key.starts_with("datafusion.")
        || key.starts_with("flow.")
        || key == "query_parallelism"
        || key == "query_fallback"
        || key == "allow_query_fallback"
}

/// Normalize public hints before any QueryContext mutation.
pub(crate) fn validate_public_hints(hints: Vec<(String, String)>) -> Result<Vec<(String, String)>> {
    use std::collections::HashMap;

    let mut normalized = Vec::with_capacity(hints.len());
    let mut seen = HashMap::<String, String>::new();
    for (key, value) in hints {
        if session::hints::is_reserved_extension_key(&key) {
            continue;
        }
        let is_read_preference = key.eq_ignore_ascii_case(session::hints::READ_PREFERENCE_HINT);
        if is_read_preference && value.parse::<session::ReadPreference>().is_err() {
            return InvalidParameterSnafu {
                reason: format!("Invalid read preference `{value}`"),
            }
            .fail();
        }
        if key.to_ascii_lowercase().starts_with("flow.") {
            return InvalidParameterSnafu {
                reason: format!("Public hint `{key}` is reserved for internal use"),
            }
            .fail();
        }
        let pair = if is_read_preference {
            let preference = value.parse::<session::ReadPreference>().unwrap();
            (
                session::hints::READ_PREFERENCE_HINT.to_string(),
                preference.to_string().to_ascii_lowercase(),
            )
        } else {
            match session::query_options::parse_query_option(&key, &value) {
                Ok(Some(pair)) => pair,
                Ok(None) => (key, value),
                Err(error) => {
                    return InvalidParameterSnafu {
                        reason: error.to_string(),
                    }
                    .fail();
                }
            }
        };
        let canonical = pair.0.to_ascii_lowercase();
        if canonical.starts_with("query.")
            || canonical.starts_with("datafusion.")
            || canonical == "query_parallelism"
            || canonical == "query_fallback"
            || canonical == "allow_query_fallback"
            || canonical == session::hints::READ_PREFERENCE_HINT
        {
            if let Some(existing) = seen.get(&pair.0) {
                if existing != &pair.1 {
                    return InvalidParameterSnafu {
                        reason: format!("Conflicting values for hint `{}`", pair.0),
                    }
                    .fail();
                }
                continue;
            }
            seen.insert(pair.0.clone(), pair.1.clone());
        }
        normalized.push(pair);
    }
    Ok(normalized)
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
    use common_error::ext::ErrorExt;
    use http::header::{HeaderMap, HeaderValue};
    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::*;

    #[test]
    fn test_query_style_missing_equals_is_preserved() {
        let mut headers = HeaderMap::new();
        headers.insert(HINTS_KEY, HeaderValue::from_static("query.parallelism"));
        assert_eq!(
            extract_hints(&headers),
            vec![("query.parallelism".to_string(), String::new())]
        );
    }

    #[test]
    fn test_query_option_aliases_normalize_and_conflicts_fail() {
        let hints = validate_public_hints(vec![
            ("query_parallelism".into(), "04".into()),
            ("query.parallelism".into(), "4".into()),
            ("query_fallback".into(), "TRUE".into()),
        ])
        .unwrap();
        assert_eq!(
            hints,
            vec![
                ("query.parallelism".into(), "4".into()),
                ("query.allow_query_fallback".into(), "true".into()),
            ]
        );
        assert!(
            validate_public_hints(vec![
                ("query_parallelism".into(), "4".into()),
                ("query.parallelism".into(), "5".into()),
            ])
            .is_err()
        );
    }

    #[test]
    fn test_read_preference_is_canonicalized_and_validated() {
        assert_eq!(
            validate_public_hints(vec![("READ_PREFERENCE".into(), "leader".into())]).unwrap(),
            vec![("read_preference".into(), "leader".into())]
        );
        assert!(
            validate_public_hints(vec![("read_preference".into(), "follower".into())]).is_err()
        );
    }

    #[test]
    fn test_unknown_or_malformed_query_options_fail() {
        for (key, value) in [
            ("query.parallelism", "0"),
            ("query.unknown_option", "true"),
            ("datafusion.execution.batch_size", "12"),
            ("query.enable_remote_dynamic_filter_pushdown", "yes"),
        ] {
            assert!(validate_public_hints(vec![(key.into(), value.into())]).is_err());
        }
    }

    #[test]
    fn test_unrelated_duplicate_hints_remain_last_write_wins() {
        let hints = validate_public_hints(vec![
            ("ttl".into(), "1d".into()),
            ("ttl".into(), "2d".into()),
        ])
        .unwrap();
        assert_eq!(hints.len(), 2);
    }

    #[test]
    fn test_public_query_options_are_validated_before_apply() {
        let err = validate_public_hints(vec![("flow.scheduled_time_millis".into(), "1".into())])
            .unwrap_err();
        assert_eq!(
            err.status_code(),
            common_error::status_code::StatusCode::InvalidArguments
        );
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
