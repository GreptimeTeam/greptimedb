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

//! Selects HTTP hint sources using compatibility declared by each registration.

use std::sync::OnceLock;

use http::HeaderMap;
use session::query_options::{self, QueryParseOptions};

use crate::error::{InvalidParameterSnafu, Result};
use crate::hints::registry;

pub(crate) use session::hints::{
    APPEND_MODE_HEADER, AUTO_CREATE_TABLE_HEADER, MERGE_MODE_HEADER, PHYSICAL_TABLE_HEADER,
    READ_PREFERENCE_HEADER, SKIP_WAL_HEADER, TTL_HEADER,
};
pub(crate) const INSERT_SKIP_WAL_HEADER: &str = "x-greptime-hint-insert_skip_wal";

/// A retired individual hint header retained at the transport boundary.
pub(crate) struct LegacyHeader {
    pub(crate) name: &'static str,
    pub(crate) http_only: bool,
}

/// Historical invalid-value handling for a dedicated header.
pub(crate) enum InvalidHeaderPolicy {
    Ignore,
    Reject(&'static str),
}

/// A dedicated header overrides the generic input whenever it is present.
pub(crate) struct HeaderCompatibility {
    pub(crate) name: &'static str,
    pub(crate) invalid: InvalidHeaderPolicy,
}

/// Selected public inputs and validated dedicated-header overrides.
#[derive(Clone)]
pub(crate) struct SelectedHints {
    pub(crate) hints: Vec<(String, String)>,
    pub(crate) dedicated: Vec<session::hint_registry::ParsedHint<session::context::QueryContext>>,
}

/// Enumerates legacy inputs from registrations without expanding gRPC compatibility.
pub(crate) fn legacy_headers(http: bool) -> impl Iterator<Item = (&'static str, &'static str)> {
    static LEGACY_HINTS: OnceLock<Vec<&'static registry::ProtocolHint>> = OnceLock::new();
    LEGACY_HINTS
        .get_or_init(|| {
            registry::registrations()
                .iter()
                .filter(|hint| hint.legacy_header.is_some())
                .collect()
        })
        .iter()
        .filter_map(move |hint| {
            hint.legacy_header
                .as_ref()
                .filter(|header| http || !header.http_only)
                .map(|header| (header.name, hint.definition.metadata.name.as_str()))
        })
}

/// Selects dedicated sources before parsing generic values, preserving presence semantics.
pub(crate) fn select_http_inputs(
    mut hints: Vec<(String, String)>,
    headers: &HeaderMap,
) -> Result<SelectedHints> {
    let mut dedicated = Vec::new();
    static DEDICATED_HINTS: OnceLock<Vec<&'static registry::ProtocolHint>> = OnceLock::new();
    let registrations = DEDICATED_HINTS.get_or_init(|| {
        registry::registrations()
            .iter()
            .filter(|hint| hint.dedicated_http_header.is_some())
            .collect()
    });
    for registration in registrations {
        let Some(compatibility) = &registration.dedicated_http_header else {
            continue;
        };
        let Some(value) = headers.get(compatibility.name) else {
            continue;
        };
        let definition = &registration.definition;
        hints.retain(|(name, _)| {
            !name.eq_ignore_ascii_case(&definition.metadata.name)
                && !definition
                    .metadata
                    .aliases
                    .iter()
                    .any(|alias| name.eq_ignore_ascii_case(alias))
        });
        let parsed = value.to_str().ok().and_then(|value| {
            registry::dedicated_http_registry()
                .parse(
                    &definition.metadata.name,
                    value,
                    QueryParseOptions {
                        max_parallelism: query_options::MAX_PUBLIC_PARALLELISM,
                    },
                )
                .ok()
                .flatten()
        });
        match parsed {
            Some(value) => dedicated.push(value),
            None => match compatibility.invalid {
                InvalidHeaderPolicy::Ignore => {}
                InvalidHeaderPolicy::Reject(reason) => {
                    return InvalidParameterSnafu {
                        reason: format!("{} {reason}", compatibility.name),
                    }
                    .fail();
                }
            },
        }
    }
    Ok(SelectedHints { hints, dedicated })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::header::constants::{
        GREPTIME_DB_HEADER_READ_PREFERENCE, GREPTIME_INSERT_SKIP_WAL_HEADER_NAME,
    };
    use http::HeaderValue;
    use session::context::QueryContext;
    use session::hints::{INSERT_SKIP_WAL_HINT, READ_PREFERENCE_HINT};

    fn hints(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn test_legacy_bindings_keep_transport_scope() {
        let http: Vec<_> = legacy_headers(true).collect();
        let grpc: Vec<_> = legacy_headers(false).collect();
        assert_eq!(http.len(), 8);
        assert_eq!(grpc.len(), 7);
        assert!(http.contains(&(INSERT_SKIP_WAL_HEADER, INSERT_SKIP_WAL_HINT)));
        assert!(!grpc.contains(&(INSERT_SKIP_WAL_HEADER, INSERT_SKIP_WAL_HINT)));
        for header in session::hints::HINT_KEYS {
            assert!(grpc.iter().any(|(registered, _)| *registered == header));
        }
    }

    #[test]
    fn test_dedicated_wal_overrides_generic_input() {
        for value in ["true", "false"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                GREPTIME_INSERT_SKIP_WAL_HEADER_NAME,
                HeaderValue::from_static(value),
            );
            let selected = select_http_inputs(
                hints(&[(INSERT_SKIP_WAL_HINT, "invalid"), ("ttl", "1d")]),
                &headers,
            )
            .unwrap();
            assert_eq!(selected.hints, hints(&[("ttl", "1d")]));
            let mut context = QueryContext::with("greptime", "public");
            for parsed in selected.dedicated {
                parsed.apply(&mut context);
            }
            assert_eq!(context.skip_wal(), value == "true");
        }
    }

    #[test]
    fn test_invalid_dedicated_wal_fails_source_selection() {
        for value in [
            HeaderValue::from_static("TRUE"),
            HeaderValue::from_static(" true "),
            HeaderValue::from_bytes(&[0xff]).unwrap(),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(GREPTIME_INSERT_SKIP_WAL_HEADER_NAME, value);
            let result = select_http_inputs(
                hints(&[
                    ("query.parallelism", "invalid"),
                    (INSERT_SKIP_WAL_HINT, "false"),
                ]),
                &headers,
            );
            let error = result.err().expect("invalid dedicated WAL");
            assert_eq!(
                error.to_string(),
                InvalidParameterSnafu {
                    reason: format!("{GREPTIME_INSERT_SKIP_WAL_HEADER_NAME} must be true or false")
                }
                .build()
                .to_string()
            );
        }
    }

    #[test]
    fn test_invalid_dedicated_read_preference_suppresses_generic_input() {
        for value in [
            HeaderValue::from_static("invalid"),
            HeaderValue::from_bytes(&[0xff]).unwrap(),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(GREPTIME_DB_HEADER_READ_PREFERENCE, value);
            let selected = select_http_inputs(
                hints(&[("READ_PREFERENCE", "invalid"), ("ttl", "1d")]),
                &headers,
            )
            .unwrap();
            assert_eq!(selected.hints, hints(&[("ttl", "1d")]));
            assert!(selected.dedicated.is_empty());
        }
    }

    #[test]
    fn test_absent_dedicated_headers_preserve_input_order() {
        let input = hints(&[
            ("ttl", "1d"),
            (READ_PREFERENCE_HINT, "leader"),
            ("ttl", "2d"),
        ]);
        let selected = select_http_inputs(input.clone(), &HeaderMap::new()).unwrap();
        assert_eq!(selected.hints, input);
        assert!(selected.dedicated.is_empty());
    }
}
