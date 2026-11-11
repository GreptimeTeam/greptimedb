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

//! Conversion at the region RPC boundary. Invalid requests never reach file selection.

use api::helper;
use api::v1::{self, region as pb};
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datatypes::value::Value;

use crate::sketch::{
    ColumnSketchResult, SketchBucket, SketchGap, SketchGapReason, SketchQuery, SketchRequest,
    SketchResponse, SketchTimeRange, SketchValue,
};
use crate::storage::{FileId, RegionId};

impl TryFrom<pb::SketchTimeRange> for SketchTimeRange {
    type Error = BoxedError;

    fn try_from(range: pb::SketchTimeRange) -> Result<Self, Self::Error> {
        let unit =
            v1::TimeUnit::try_from(range.time_unit).map_err(|_| invalid("unknown time unit"))?;
        let unit = helper::from_pb_time_unit(unit);
        if range.start >= range.end {
            return Err(invalid("sketch time range must have start before end"));
        }
        Ok(Self {
            start: Timestamp::new(range.start, unit),
            end: Timestamp::new(range.end, unit),
        })
    }
}

impl TryFrom<SketchTimeRange> for pb::SketchTimeRange {
    type Error = BoxedError;

    fn try_from(range: SketchTimeRange) -> Result<Self, Self::Error> {
        let units = [range.start.unit(), range.end.unit()];
        let unit = if units.contains(&TimeUnit::Nanosecond) {
            TimeUnit::Nanosecond
        } else if units.contains(&TimeUnit::Microsecond) {
            TimeUnit::Microsecond
        } else if units.contains(&TimeUnit::Millisecond) {
            TimeUnit::Millisecond
        } else {
            TimeUnit::Second
        };
        let start = range
            .start
            .convert_to(unit)
            .ok_or_else(|| invalid("start timestamp overflows wire unit"))?;
        let end = range
            .end
            .convert_to(unit)
            .ok_or_else(|| invalid("end timestamp overflows wire unit"))?;
        if start >= end {
            return Err(invalid("sketch time range must have start before end"));
        }
        Ok(Self {
            start: start.value(),
            end: end.value(),
            time_unit: helper::to_pb_time_unit(unit) as i32,
        })
    }
}

impl TryFrom<pb::SketchQuery> for SketchQuery {
    type Error = BoxedError;

    fn try_from(query: pb::SketchQuery) -> Result<Self, Self::Error> {
        use pb::sketch_query::Query;
        match query
            .query
            .ok_or_else(|| invalid("missing sketch operation"))?
        {
            Query::DistinctCount(_) => Ok(Self::DistinctCount),
            Query::Quantile(query) => {
                if !query.fraction.is_finite() || !(0.0..=1.0).contains(&query.fraction) {
                    return Err(invalid("quantile fraction must be finite and in [0, 1]"));
                }
                Ok(Self::Quantile {
                    fraction: query.fraction,
                })
            }
            Query::Frequency(query) => Ok(Self::Frequency {
                value: decode_value(query.value)?,
            }),
            Query::Rank(query) => {
                let value = decode_value(query.value)?;
                if value.is_null() {
                    return Err(invalid("rank requires a non-null value"));
                }
                Ok(Self::Rank { value })
            }
        }
    }
}

impl From<SketchQuery> for pb::SketchQuery {
    fn from(query: SketchQuery) -> Self {
        use pb::sketch_query::Query;
        let query = match query {
            SketchQuery::DistinctCount => Query::DistinctCount(pb::SketchDistinctCountQuery {}),
            SketchQuery::Quantile { fraction } => {
                Query::Quantile(pb::SketchQuantileQuery { fraction })
            }
            SketchQuery::Frequency { value } => Query::Frequency(pb::SketchFrequencyQuery {
                value: Some(helper::to_grpc_value(value)),
            }),
            SketchQuery::Rank { value } => Query::Rank(pb::SketchRankQuery {
                value: Some(helper::to_grpc_value(value)),
            }),
        };
        Self { query: Some(query) }
    }
}

impl TryFrom<pb::SketchRequest> for SketchRequest {
    type Error = BoxedError;

    fn try_from(request: pb::SketchRequest) -> Result<Self, Self::Error> {
        let request = Self {
            time_range: request
                .time_range
                .ok_or_else(|| invalid("missing sketch time range"))?
                .try_into()?,
            columns: request.columns,
            queries: request
                .queries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        };
        request.validate()?;
        Ok(request)
    }
}

impl SketchRequest {
    /// Converts a validated request to the wire representation for a region.
    pub fn into_proto(self, region_id: RegionId) -> Result<pb::SketchRequest, BoxedError> {
        self.validate()?;
        Ok(pb::SketchRequest {
            region_id: region_id.as_u64(),
            time_range: Some(self.time_range.try_into()?),
            columns: self.columns,
            queries: self.queries.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<SketchValue> for pb::SketchValue {
    fn from(value: SketchValue) -> Self {
        use pb::sketch_value::Value;
        Self {
            value: Some(match value {
                SketchValue::DistinctCount {
                    estimate,
                    relative_standard_error,
                } => Value::DistinctCount(pb::SketchDistinctCount {
                    estimate,
                    relative_standard_error,
                }),
                SketchValue::Quantile { value } => Value::Quantile(pb::SketchQuantile {
                    value: Some(helper::to_grpc_value(value)),
                }),
                SketchValue::Frequency { lower, upper } => {
                    Value::Frequency(pb::SketchFrequency { lower, upper })
                }
                SketchValue::Rank { fraction } => Value::Rank(pb::SketchRank { fraction }),
            }),
        }
    }
}

impl TryFrom<pb::SketchValue> for SketchValue {
    type Error = BoxedError;

    fn try_from(value: pb::SketchValue) -> Result<Self, Self::Error> {
        use pb::sketch_value::Value;
        Ok(
            match value
                .value
                .ok_or_else(|| invalid("missing sketch result value"))?
            {
                Value::DistinctCount(value) => {
                    if !value.estimate.is_finite()
                        || value.estimate < 0.0
                        || !value.relative_standard_error.is_finite()
                        || value.relative_standard_error < 0.0
                    {
                        return Err(invalid("invalid distinct count result"));
                    }
                    Self::DistinctCount {
                        estimate: value.estimate,
                        relative_standard_error: value.relative_standard_error,
                    }
                }
                Value::Quantile(value) => Self::Quantile {
                    value: decode_value(value.value)?,
                },
                Value::Frequency(value) => {
                    if value.lower > value.upper {
                        return Err(invalid("invalid frequency bounds"));
                    }
                    Self::Frequency {
                        lower: value.lower,
                        upper: value.upper,
                    }
                }
                Value::Rank(value) => {
                    if !value.fraction.is_finite() || !(0.0..=1.0).contains(&value.fraction) {
                        return Err(invalid("invalid rank fraction"));
                    }
                    Self::Rank {
                        fraction: value.fraction,
                    }
                }
            },
        )
    }
}

impl TryFrom<SketchResponse> for pb::SketchResponse {
    type Error = BoxedError;

    fn try_from(response: SketchResponse) -> Result<Self, Self::Error> {
        let results = response
            .results
            .into_iter()
            .map(|result| {
                let buckets = result
                    .buckets
                    .into_iter()
                    .map(|bucket| {
                        Ok(pb::SketchBucket {
                            time_range: Some(bucket.time_range.try_into()?),
                            covered_files: bucket.covered_files,
                            missing_files: bucket.missing_files,
                            represented_rows: bucket.represented_rows,
                        })
                    })
                    .collect::<Result<_, BoxedError>>()?;
                let gaps = result
                    .gaps
                    .into_iter()
                    .map(|gap| pb::SketchGap {
                        source_region_id: gap.source_region_id.as_u64(),
                        file_id: gap.file_id.as_bytes().to_vec(),
                        index_version: gap.index_version,
                        reason: match gap.reason {
                            SketchGapReason::NotConfigured => {
                                pb::SketchGapReason::NotConfigured as i32
                            }
                            SketchGapReason::MissingIndex => {
                                pb::SketchGapReason::MissingIndex as i32
                            }
                            SketchGapReason::LegacyFormat => {
                                pb::SketchGapReason::LegacyFormat as i32
                            }
                            SketchGapReason::ScopeMismatch => {
                                pb::SketchGapReason::ScopeMismatch as i32
                            }
                            SketchGapReason::Incompatible => {
                                pb::SketchGapReason::Incompatible as i32
                            }
                        },
                    })
                    .collect();
                Ok(pb::ColumnSketchResult {
                    column_name: result.column_name,
                    column_id: result.column_id,
                    query: Some(result.query.into()),
                    buckets,
                    gaps,
                    value: result.value.map(Into::into),
                })
            })
            .collect::<Result<_, BoxedError>>()?;
        Ok(Self { results })
    }
}

impl TryFrom<pb::SketchResponse> for SketchResponse {
    type Error = BoxedError;

    fn try_from(response: pb::SketchResponse) -> Result<Self, Self::Error> {
        let results = response
            .results
            .into_iter()
            .map(|result| {
                let buckets = result
                    .buckets
                    .into_iter()
                    .map(|bucket| {
                        Ok(SketchBucket {
                            time_range: bucket
                                .time_range
                                .ok_or_else(|| invalid("missing bucket range"))?
                                .try_into()?,
                            covered_files: bucket.covered_files,
                            missing_files: bucket.missing_files,
                            represented_rows: bucket.represented_rows,
                        })
                    })
                    .collect::<Result<_, BoxedError>>()?;
                let gaps = result
                    .gaps
                    .into_iter()
                    .map(|gap| {
                        let reason = match pb::SketchGapReason::try_from(gap.reason)
                            .map_err(|_| invalid("unknown gap reason"))?
                        {
                            pb::SketchGapReason::NotConfigured => SketchGapReason::NotConfigured,
                            pb::SketchGapReason::MissingIndex => SketchGapReason::MissingIndex,
                            pb::SketchGapReason::LegacyFormat => SketchGapReason::LegacyFormat,
                            pb::SketchGapReason::ScopeMismatch => SketchGapReason::ScopeMismatch,
                            pb::SketchGapReason::Incompatible => SketchGapReason::Incompatible,
                            pb::SketchGapReason::Unspecified => {
                                return Err(invalid("unspecified gap reason"));
                            }
                        };
                        Ok(SketchGap {
                            source_region_id: RegionId::from_u64(gap.source_region_id),
                            file_id: FileId::from_bytes(
                                gap.file_id
                                    .try_into()
                                    .map_err(|_| invalid("file UUID must be 16 bytes"))?,
                            ),
                            index_version: gap.index_version,
                            reason,
                        })
                    })
                    .collect::<Result<_, BoxedError>>()?;
                Ok(ColumnSketchResult {
                    column_name: result.column_name,
                    column_id: result.column_id,
                    query: result
                        .query
                        .ok_or_else(|| invalid("missing result query"))?
                        .try_into()?,
                    buckets,
                    gaps,
                    value: result.value.map(TryInto::try_into).transpose()?,
                })
            })
            .collect::<Result<_, BoxedError>>()?;
        Ok(Self { results })
    }
}

fn decode_value(value: Option<v1::Value>) -> Result<Value, BoxedError> {
    use v1::value::ValueData;
    let value = value.ok_or_else(|| invalid("missing query value"))?;
    // Composite and decimal values need type extensions, which this protocol does not carry.
    match value.value_data.as_ref() {
        Some(
            ValueData::ListValue(_) | ValueData::StructValue(_) | ValueData::Decimal128Value(_),
        ) => return Err(invalid("query value requires unsupported type extensions")),
        Some(ValueData::I8Value(v)) if i8::try_from(*v).is_err() => {
            return Err(invalid("i8 value out of range"));
        }
        Some(ValueData::I16Value(v)) if i16::try_from(*v).is_err() => {
            return Err(invalid("i16 value out of range"));
        }
        Some(ValueData::U8Value(v)) if u8::try_from(*v).is_err() => {
            return Err(invalid("u8 value out of range"));
        }
        Some(ValueData::U16Value(v)) if u16::try_from(*v).is_err() => {
            return Err(invalid("u16 value out of range"));
        }
        _ => {}
    }
    Ok(helper::pb_value_to_value_ref(&value, None).into())
}

fn invalid(reason: &str) -> BoxedError {
    BoxedError::new(PlainError::new(
        reason.to_string(),
        StatusCode::InvalidArguments,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_precision_and_invalid_units() {
        let range = SketchTimeRange {
            start: Timestamp::new_microsecond(1001),
            end: Timestamp::new_nanosecond(1_001_001),
        };
        let wire = pb::SketchTimeRange::try_from(range).unwrap();
        assert_eq!(
            (wire.start, wire.end, wire.time_unit),
            (1_001_000, 1_001_001, v1::TimeUnit::Nanosecond as i32)
        );
        assert_eq!(SketchTimeRange::try_from(wire).unwrap(), range);
        assert!(
            SketchTimeRange::try_from(pb::SketchTimeRange {
                start: 1,
                end: 2,
                time_unit: 999
            })
            .is_err()
        );
    }

    #[test]
    fn query_roundtrip_and_invalid_arguments() {
        for query in [
            SketchQuery::DistinctCount,
            SketchQuery::Quantile { fraction: 0.5 },
            SketchQuery::Frequency {
                value: Value::UInt32(42),
            },
            SketchQuery::Rank {
                value: Value::UInt32(42),
            },
        ] {
            assert_eq!(
                SketchQuery::try_from(pb::SketchQuery::from(query.clone())).unwrap(),
                query
            );
        }
        assert!(SketchQuery::try_from(pb::SketchQuery::default()).is_err());
        assert!(
            SketchQuery::try_from(pb::SketchQuery::from(SketchQuery::Quantile {
                fraction: f64::NAN
            }))
            .is_err()
        );
        assert!(
            SketchQuery::try_from(pb::SketchQuery::from(SketchQuery::Rank {
                value: Value::Null
            }))
            .is_err()
        );
        assert!(SketchRequest::try_from(pb::SketchRequest::default()).is_err());
    }

    #[test]
    fn test_request_roundtrip() {
        let region_id = RegionId::new(42, 7);
        let request = SketchRequest {
            time_range: SketchTimeRange {
                start: Timestamp::new_microsecond(1001),
                end: Timestamp::new_microsecond(2003),
            },
            columns: vec!["__table_id".to_string(), "value".to_string()],
            queries: vec![
                SketchQuery::DistinctCount,
                SketchQuery::Quantile { fraction: 0.95 },
                SketchQuery::Frequency {
                    value: Value::UInt32(42),
                },
                SketchQuery::Rank {
                    value: Value::UInt32(42),
                },
            ],
        };
        let wire = request.clone().into_proto(region_id).unwrap();
        assert_eq!(wire.region_id, region_id.as_u64());
        assert_eq!(
            wire.time_range,
            Some(pb::SketchTimeRange {
                start: 1001,
                end: 2003,
                time_unit: v1::TimeUnit::Microsecond as i32,
            })
        );
        assert_eq!(SketchRequest::try_from(wire).unwrap(), request);

        let mut invalid = request;
        invalid.queries = vec![SketchQuery::Rank { value: Value::Null }];
        assert!(invalid.into_proto(region_id).is_err());
    }

    fn response() -> SketchResponse {
        SketchResponse {
            results: vec![ColumnSketchResult {
                column_name: "__table_id".to_string(),
                column_id: 3,
                query: SketchQuery::Rank {
                    value: Value::UInt32(42),
                },
                buckets: vec![SketchBucket {
                    time_range: SketchTimeRange {
                        start: Timestamp::new_nanosecond(1001),
                        end: Timestamp::new_nanosecond(2003),
                    },
                    covered_files: 2,
                    missing_files: 1,
                    represented_rows: 102400,
                }],
                gaps: vec![SketchGap {
                    source_region_id: RegionId::new(42, 7),
                    file_id: FileId::from_bytes([17; 16]),
                    index_version: 9,
                    reason: SketchGapReason::MissingIndex,
                }],
                value: Some(SketchValue::Rank { fraction: 0.5 }),
            }],
        }
    }

    #[test]
    fn test_response_roundtrip() {
        for reason in [
            SketchGapReason::NotConfigured,
            SketchGapReason::MissingIndex,
            SketchGapReason::LegacyFormat,
            SketchGapReason::ScopeMismatch,
            SketchGapReason::Incompatible,
        ] {
            let mut expected = response();
            expected.results[0].gaps[0].reason = reason;
            let wire = pb::SketchResponse::try_from(expected.clone()).unwrap();
            assert_eq!(wire.results[0].gaps[0].file_id, vec![17; 16]);
            assert_eq!(SketchResponse::try_from(wire).unwrap(), expected);
        }
        let mut expected = response();
        let mut stat_result = expected.results[0].clone();
        stat_result.column_name = "other".to_string();
        stat_result.column_id = 4;
        stat_result.query = SketchQuery::DistinctCount;
        stat_result.value = None;
        expected.results.push(stat_result);
        let wire = pb::SketchResponse::try_from(expected.clone()).unwrap();
        assert_eq!(SketchResponse::try_from(wire).unwrap(), expected);
    }

    #[test]
    fn test_response_rejects_invalid_identity_and_missing_fields() {
        let valid = pb::SketchResponse::try_from(response()).unwrap();
        for length in [0, 15, 17] {
            let mut invalid = valid.clone();
            invalid.results[0].gaps[0].file_id = vec![17; length];
            assert!(SketchResponse::try_from(invalid).is_err());
        }
        for reason in [pb::SketchGapReason::Unspecified as i32, 999] {
            let mut invalid = valid.clone();
            invalid.results[0].gaps[0].reason = reason;
            assert!(SketchResponse::try_from(invalid).is_err());
        }
        let mut missing_query = valid.clone();
        missing_query.results[0].query = None;
        assert!(SketchResponse::try_from(missing_query).is_err());
        let mut missing_range = valid;
        missing_range.results[0].buckets[0].time_range = None;
        assert!(SketchResponse::try_from(missing_range).is_err());
    }

    #[test]
    fn test_result_values_roundtrip_and_invalid_values() {
        for value in [
            SketchValue::DistinctCount {
                estimate: 42.5,
                relative_standard_error: 0.0325,
            },
            SketchValue::Quantile {
                value: Value::UInt32(42),
            },
            SketchValue::Frequency {
                lower: 40,
                upper: 45,
            },
            SketchValue::Rank { fraction: 0.5 },
        ] {
            assert_eq!(
                SketchValue::try_from(pb::SketchValue::from(value.clone())).unwrap(),
                value
            );
        }
        for value in [
            SketchValue::DistinctCount {
                estimate: -1.0,
                relative_standard_error: 0.03,
            },
            SketchValue::DistinctCount {
                estimate: f64::NAN,
                relative_standard_error: 0.03,
            },
            SketchValue::DistinctCount {
                estimate: 42.0,
                relative_standard_error: f64::INFINITY,
            },
            SketchValue::DistinctCount {
                estimate: 42.0,
                relative_standard_error: -0.01,
            },
            SketchValue::Frequency {
                lower: 45,
                upper: 40,
            },
            SketchValue::Rank { fraction: -0.01 },
            SketchValue::Rank { fraction: 1.01 },
            SketchValue::Rank { fraction: f64::NAN },
            SketchValue::Rank {
                fraction: f64::INFINITY,
            },
        ] {
            let mut invalid = pb::SketchResponse::try_from(response()).unwrap();
            invalid.results[0].value = Some(value.into());
            assert!(SketchResponse::try_from(invalid).is_err());
        }
        assert!(SketchValue::try_from(pb::SketchValue::default()).is_err());
        assert!(
            SketchValue::try_from(pb::SketchValue {
                value: Some(pb::sketch_value::Value::Quantile(pb::SketchQuantile {
                    value: None
                })),
            })
            .is_err()
        );
    }
}
