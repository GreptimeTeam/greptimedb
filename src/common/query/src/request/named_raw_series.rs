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

//! Named raw-series preflight contract.
//!
//! The contract is the JSON body of the [`PREPARE_RAW_SERIES_ACTION`] action.
//! It carries the already-bound raw-series request and the ordering the
//! approving node can guarantee for it, or an explicit unsupported marker.

use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datatypes::series::SeriesOrdering;
use serde::{Deserialize, Deserializer, Serialize};

use crate::request::RawSeriesRequest;

/// Action name of the named raw-series preflight contract.
pub const PREPARE_RAW_SERIES_ACTION: &str = "greptime.prepare_raw_series.v1";

/// A named raw-series request approved for preflight.
#[derive(Clone, Debug)]
pub struct PreparedNamedRawSeries {
    /// The bound raw-series request approved by the preparing node.
    pub binding: RawSeriesRequest,
    /// The ordering guaranteed by the preparing node for the binding.
    pub ordering: SeriesOrdering,
}

/// Encodes a preflight response body for [`PREPARE_RAW_SERIES_ACTION`].
///
/// `Some` approves the request after revalidating its binding through the
/// existing [`RawSeriesRequest`] validation, while `None` encodes the frozen
/// unsupported shape. Not every [`SeriesOrdering`] can be reported: interleaved
/// orderings are rejected.
pub fn encode_prepare_raw_series_response(
    supported: Option<&PreparedNamedRawSeries>,
) -> DataFusionResult<Vec<u8>> {
    let supported = match supported {
        Some(prepared) => {
            prepared.binding.to_extension_value()?;
            Some(Supported {
                binding: prepared.binding.clone(),
                ordering: WireOrdering::from_series_ordering(prepared.ordering)?,
            })
        }
        None => None,
    };
    let response = PrepareRawSeriesResponse {
        version: PREPARE_RAW_SERIES_VERSION,
        supported,
    };
    serde_json::to_vec(&response).map_err(|_| invalid_response_error())
}

/// Decodes a preflight response body for [`PREPARE_RAW_SERIES_ACTION`].
///
/// Returns `Some` for an approving body and `None` for the frozen unsupported
/// shape. The body is decoded directly as a typed JSON envelope, so duplicate
/// keys, unknown fields, unknown versions, and malformed bindings are rejected.
pub fn decode_prepare_raw_series_response(
    body: &[u8],
) -> DataFusionResult<Option<PreparedNamedRawSeries>> {
    let response: PrepareRawSeriesResponse =
        serde_json::from_slice(body).map_err(|_| invalid_response_error())?;
    if response.version != PREPARE_RAW_SERIES_VERSION {
        return Err(unsupported_version_error());
    }
    let Some(supported) = response.supported else {
        return Ok(None);
    };
    supported.binding.to_extension_value()?;
    Ok(Some(PreparedNamedRawSeries {
        binding: supported.binding,
        ordering: supported.ordering.into_series_ordering(),
    }))
}

/// Wire-format version of the named raw-series preflight contract.
const PREPARE_RAW_SERIES_VERSION: u32 = 1;

/// Private JSON envelope of [`PREPARE_RAW_SERIES_ACTION`].
///
/// `supported` is required but nullable: a missing field is rejected instead of
/// defaulting, while an explicit `null` is the unsupported encoding.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareRawSeriesResponse {
    version: u32,
    #[serde(deserialize_with = "deserialize_required_supported")]
    supported: Option<Supported>,
}

/// Private shape of an approving response.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Supported {
    binding: RawSeriesRequest,
    ordering: WireOrdering,
}

/// Private mirror of the orderings this contract can report.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireOrdering {
    Contiguous,
    ReadKeyThenTime,
}

impl WireOrdering {
    /// Rejects orderings the contract cannot report.
    fn from_series_ordering(ordering: SeriesOrdering) -> DataFusionResult<Self> {
        match ordering {
            SeriesOrdering::Contiguous => Ok(Self::Contiguous),
            SeriesOrdering::KeyThenTime => Ok(Self::ReadKeyThenTime),
            SeriesOrdering::Interleaved => Err(unsupported_ordering_error()),
        }
    }

    fn into_series_ordering(self) -> SeriesOrdering {
        match self {
            Self::Contiguous => SeriesOrdering::Contiguous,
            Self::ReadKeyThenTime => SeriesOrdering::KeyThenTime,
        }
    }
}

/// Requires the `supported` field while still accepting an explicit `null`.
fn deserialize_required_supported<'de, D>(deserializer: D) -> Result<Option<Supported>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Supported>::deserialize(deserializer)
}

fn invalid_response_error() -> DataFusionError {
    DataFusionError::Plan("Invalid prepare raw-series response".to_string())
}

fn unsupported_version_error() -> DataFusionError {
    DataFusionError::Plan("Unsupported prepare raw-series response version".to_string())
}

fn unsupported_ordering_error() -> DataFusionError {
    DataFusionError::Plan("Unsupported prepare raw-series response ordering".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use store_api::storage::RegionId;

    use super::*;

    const SUPPORTED_FIXTURE: &str = include_str!(
        "../../../../../tests/compatibility/wire/named_raw_series_prepare/v1_supported.json"
    );
    const UNSUPPORTED_FIXTURE: &str = include_str!(
        "../../../../../tests/compatibility/wire/named_raw_series_prepare/v1_unsupported.json"
    );
    const BINDING_FIXTURE: &str =
        include_str!("../../../../../tests/compatibility/wire/raw_series_request/v1_request.json");

    fn fixture_binding() -> RawSeriesRequest {
        RawSeriesRequest::from_extension_value(BINDING_FIXTURE).unwrap()
    }

    fn fixture_body() -> String {
        format!(
            r#"{{"version":1,"supported":{{"binding":{},"ordering":"contiguous"}}}}"#,
            BINDING_FIXTURE.trim()
        )
    }

    #[test]
    fn frozen_fixtures_decode_supported_and_unsupported() {
        let prepared = decode_prepare_raw_series_response(SUPPORTED_FIXTURE.as_bytes())
            .unwrap()
            .expect("supported fixture must decode as approved");
        assert_eq!(prepared.ordering, SeriesOrdering::Contiguous);
        assert_eq!(
            prepared.binding.physical_metadata.region_id,
            RegionId::new(1, 1)
        );
        assert_eq!(
            prepared.binding.to_extension_value().unwrap(),
            fixture_binding().to_extension_value().unwrap()
        );

        let (schema, _) = prepared
            .binding
            .bind(Arc::new(prepared.binding.physical_metadata.clone()))
            .unwrap();
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>(),
            ["aux", "value"]
        );

        assert!(
            decode_prepare_raw_series_response(UNSUPPORTED_FIXTURE.as_bytes())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn prepare_response_round_trips_both_orderings() {
        for (ordering, wire_name) in [
            (SeriesOrdering::Contiguous, "contiguous"),
            (SeriesOrdering::KeyThenTime, "read_key_then_time"),
        ] {
            let prepared = PreparedNamedRawSeries {
                binding: fixture_binding(),
                ordering,
            };
            let encoded = encode_prepare_raw_series_response(Some(&prepared)).unwrap();
            let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(json["version"].as_u64(), Some(1));
            assert_eq!(json["supported"]["ordering"].as_str(), Some(wire_name));
            assert!(json["supported"]["binding"].is_object());

            let decoded = decode_prepare_raw_series_response(&encoded)
                .unwrap()
                .expect("approved response must decode as supported");
            assert_eq!(decoded.ordering, ordering);
            assert_eq!(
                decoded.binding.to_extension_value().unwrap(),
                prepared.binding.to_extension_value().unwrap()
            );
        }
    }

    #[test]
    fn unsupported_prepare_response_has_frozen_shape() {
        let encoded = encode_prepare_raw_series_response(None).unwrap();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            r#"{"version":1,"supported":null}"#
        );
        assert!(
            decode_prepare_raw_series_response(&encoded)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn prepare_response_encoding_rejects_interleaved_and_malformed_binding() {
        let interleaved = PreparedNamedRawSeries {
            binding: fixture_binding(),
            ordering: SeriesOrdering::Interleaved,
        };
        assert!(encode_prepare_raw_series_response(Some(&interleaved)).is_err());

        let mut malformed = fixture_binding();
        malformed.field_ids.push(3);
        let malformed = PreparedNamedRawSeries {
            binding: malformed,
            ordering: SeriesOrdering::Contiguous,
        };
        assert!(encode_prepare_raw_series_response(Some(&malformed)).is_err());
    }

    #[test]
    fn prepare_response_rejects_malformed_bodies() {
        let body = fixture_body();
        let cases = [
            ("empty body", String::new()),
            ("not json", "{".to_string()),
            ("top-level array", "[]".to_string()),
            ("missing version", r#"{"supported":null}"#.to_string()),
            (
                "zero version",
                r#"{"version":0,"supported":null}"#.to_string(),
            ),
            (
                "unknown version",
                r#"{"version":2,"supported":null}"#.to_string(),
            ),
            (
                "unknown envelope field",
                r#"{"version":1,"supported":null,"extra":true}"#.to_string(),
            ),
            ("missing supported", r#"{"version":1}"#.to_string()),
            (
                "duplicate version",
                r#"{"version":1,"version":1,"supported":null}"#.to_string(),
            ),
            (
                "duplicate supported",
                r#"{"version":1,"supported":null,"supported":null}"#.to_string(),
            ),
            (
                "supported missing binding",
                r#"{"version":1,"supported":{"ordering":"contiguous"}}"#.to_string(),
            ),
            (
                "supported missing ordering",
                body.replace(r#","ordering":"contiguous""#, ""),
            ),
            (
                "supported unknown field",
                body.replace(
                    r#""ordering":"contiguous""#,
                    r#""ordering":"contiguous","extra":true"#,
                ),
            ),
            (
                "interleaved ordering",
                body.replace(r#""ordering":"contiguous""#, r#""ordering":"interleaved""#),
            ),
            (
                "unknown ordering",
                body.replace(r#""ordering":"contiguous""#, r#""ordering":"future""#),
            ),
            (
                "non-string ordering",
                body.replace(r#""ordering":"contiguous""#, r#""ordering":1"#),
            ),
            (
                "unknown nested binding field",
                body.replace(
                    r#""metric_name":"cpu""#,
                    r#""metric_name":"cpu","extra":true"#,
                ),
            ),
            (
                "duplicate nested binding field",
                body.replace(
                    r#""metric_name":"cpu""#,
                    r#""metric_name":"cpu","metric_name":"cpu""#,
                ),
            ),
            (
                "unknown nested tag field",
                body.replace(
                    r#"{"column_id":1,"name":"host"}"#,
                    r#"{"column_id":1,"name":"host","extra":true}"#,
                ),
            ),
            (
                "duplicate nested tag field",
                body.replace(
                    r#"{"column_id":1,"name":"host"}"#,
                    r#"{"column_id":1,"name":"host","name":"host"}"#,
                ),
            ),
            (
                "malformed binding version",
                body.replace(
                    r#"{"version":1,"physical_metadata""#,
                    r#"{"version":2,"physical_metadata""#,
                ),
            ),
            (
                "duplicate binding field id",
                body.replace(r#""field_ids":[3,2]"#, r#""field_ids":[3,3]"#),
            ),
            (
                "non-field binding id",
                body.replace(r#""field_ids":[3,2]"#, r#""field_ids":[1]"#),
            ),
        ];

        for (name, candidate) in cases {
            assert!(
                decode_prepare_raw_series_response(candidate.as_bytes()).is_err(),
                "must reject {name}"
            );
        }
        assert!(decode_prepare_raw_series_response(body.as_bytes()).is_ok());
    }
}
