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

use api::v1::Row;
use common_macro::stack_trace_debug;
use common_time::ttl::TimeToLive;
use common_time::Timestamp;
use snafu::{Location, ResultExt, Snafu};

/// Extracts the timestamp value from a row at the given column index.
/// Returns None if the index is invalid or the value is not a timestamp type.
pub fn extract_timestamp_from_row(row: &Row, timestamp_index: usize) -> Option<Timestamp> {
    if timestamp_index >= row.values.len() {
        return None;
    }

    let value = &row.values[timestamp_index];
    let value_data = value.value_data.as_ref()?;

    use api::v1::value::ValueData;
    match value_data {
        ValueData::TimestampSecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Second))
        }
        ValueData::TimestampMillisecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Millisecond))
        }
        ValueData::TimestampMicrosecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Microsecond))
        }
        ValueData::TimestampNanosecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Nanosecond))
        }
        _ => None,
    }
}

/// Checks if a row is expired based on TTL settings.
/// Returns true if the row should be filtered out (expired), false otherwise.
pub fn is_row_expired(
    row: &Row,
    timestamp_index: usize,
    ttl: &Option<TimeToLive>,
    now: &Timestamp,
) -> Result<bool, Error> {
    // No TTL means data never expires
    let Some(ttl_value) = ttl else {
        return Ok(false);
    };

    // Extract timestamp from row
    let Some(row_timestamp) = extract_timestamp_from_row(row, timestamp_index) else {
        // If we can't extract timestamp, don't filter it out
        // This maintains backwards compatibility and safety
        return Ok(false);
    };

    // Check if expired using TTL's is_expired method
    ttl_value
        .is_expired(&row_timestamp, now)
        .context(CheckTtlSnafu)
}

/// Filters expired rows from a vec of rows based on TTL.
/// Returns the filtered rows and the count of filtered (removed) rows.
///
/// Note: This function should NOT be called for instant TTL tables.
/// Instant TTL is designed for flow tasks and should be handled separately.
pub fn filter_expired_rows(
    rows: Vec<Row>,
    timestamp_index: usize,
    ttl: &Option<TimeToLive>,
    now: &Timestamp,
) -> Result<(Vec<Row>, usize), Error> {
    // If no TTL, return all rows unchanged
    if ttl.is_none() {
        return Ok((rows, 0));
    }

    // Instant TTL should be handled separately (not by expiration logic)
    if matches!(ttl, Some(TimeToLive::Instant)) {
        return Ok((rows, 0));
    }

    let original_count = rows.len();

    // Filter rows based on TTL
    let filtered_rows: Vec<Row> = rows
        .into_iter()
        .filter(|row| {
            match is_row_expired(row, timestamp_index, ttl, now) {
                Ok(expired) => !expired, // Keep non-expired rows
                Err(_) => true,          // Keep rows we can't validate (safe default)
            }
        })
        .collect();

    let filtered_count = original_count - filtered_rows.len();

    Ok((filtered_rows, filtered_count))
}

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to check TTL expiration"))]
    CheckTtl {
        source: common_time::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl common_error::ext::ErrorExt for Error {
    fn status_code(&self) -> common_error::status_code::StatusCode {
        match self {
            Error::CheckTtl { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::v1::value::ValueData;
    use api::v1::Value;
    use common_time::timestamp::TimeUnit;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use std::sync::Arc;

    #[test]
    fn test_extract_timestamp_from_row() {
        let _schema = build_test_schema();
        let timestamp_index = 1; // Second column is timestamp

        // Create a row with timestamp 1000ms
        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                },
            ],
        };

        let result = extract_timestamp_from_row(&row, timestamp_index);
        assert!(result.is_some());
        let ts = result.unwrap();
        assert_eq!(ts.value(), 1000);
        assert_eq!(ts.unit(), TimeUnit::Millisecond);
    }

    #[test]
    fn test_extract_timestamp_invalid_index() {
        let _schema = build_test_schema();
        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
            ],
        };

        // Index out of bounds
        let result = extract_timestamp_from_row(&row, 10);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_timestamp_wrong_type() {
        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::StringValue("not a timestamp".to_string())),
                },
            ],
        };

        let result = extract_timestamp_from_row(&row, 1);
        assert!(result.is_none());
    }

    fn build_test_schema() -> Arc<Schema> {
        let columns = vec![
            ColumnSchema::new("id", ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millisecond_datatype(), false),
        ];
        Arc::new(Schema::new(columns))
    }

    #[test]
    fn test_is_row_expired_with_duration_ttl() {
        use common_time::ttl::TimeToLive;
        use std::time::Duration as StdDuration;

        // TTL of 1 hour (3600 seconds)
        let ttl = Some(TimeToLive::Duration(StdDuration::from_secs(3600)));

        // Current time: 2 hours (7200 seconds)
        let now = Timestamp::new(7200, TimeUnit::Second);

        // Row with timestamp 1 hour ago (3600s) - EXPIRED (boundary)
        let expired_row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::TimestampSecondValue(3599)),
                },
            ],
        };

        // Row with timestamp 30 minutes ago (5400s) - NOT EXPIRED
        let valid_row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(2)),
                },
                Value {
                    value_data: Some(ValueData::TimestampSecondValue(5400)),
                },
            ],
        };

        let timestamp_index = 1;

        assert!(is_row_expired(&expired_row, timestamp_index, &ttl, &now).unwrap());
        assert!(!is_row_expired(&valid_row, timestamp_index, &ttl, &now).unwrap());
    }

    #[test]
    fn test_is_row_expired_no_ttl() {
        let ttl = None;
        let now = Timestamp::new(7200, TimeUnit::Second);

        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::TimestampSecondValue(100)),
                },
            ],
        };

        // No TTL means data never expires
        assert!(!is_row_expired(&row, 1, &ttl, &now).unwrap());
    }

    #[test]
    fn test_is_row_expired_instant_ttl() {
        use common_time::ttl::TimeToLive;

        let ttl = Some(TimeToLive::Instant);
        let now = Timestamp::new(7200, TimeUnit::Second);

        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::TimestampSecondValue(7199)),
                },
            ],
        };

        // Instant TTL means all data expires immediately
        assert!(is_row_expired(&row, 1, &ttl, &now).unwrap());
    }

    #[test]
    fn test_is_row_expired_forever_ttl() {
        use common_time::ttl::TimeToLive;

        let ttl = Some(TimeToLive::Forever);
        let now = Timestamp::new(7200, TimeUnit::Second);

        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::TimestampSecondValue(100)),
                },
            ],
        };

        // Forever TTL means data never expires
        assert!(!is_row_expired(&row, 1, &ttl, &now).unwrap());
    }

    #[test]
    fn test_filter_expired_rows_from_insert_request() {
        use common_time::ttl::TimeToLive;
        use std::time::Duration as StdDuration;

        // TTL of 1 hour
        let ttl = Some(TimeToLive::Duration(StdDuration::from_secs(3600)));

        // Current time: 2 hours (7200s)
        let now = Timestamp::new(7200, TimeUnit::Second);

        // Create rows: 2 expired, 2 valid
        let rows = vec![
            Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::I64Value(1)),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampSecondValue(3000)),
                    }, // expired
                ],
            },
            Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::I64Value(2)),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampSecondValue(5400)),
                    }, // valid
                ],
            },
            Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::I64Value(3)),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampSecondValue(2000)),
                    }, // expired
                ],
            },
            Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::I64Value(4)),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampSecondValue(6000)),
                    }, // valid
                ],
            },
        ];

        let timestamp_index = 1;

        let (filtered_rows, filtered_count) =
            filter_expired_rows(rows, timestamp_index, &ttl, &now).unwrap();

        assert_eq!(filtered_count, 2); // 2 rows filtered
        assert_eq!(filtered_rows.len(), 2); // 2 rows remaining

        // Check remaining rows are the valid ones (id=2 and id=4)
        assert_eq!(
            filtered_rows[0].values[0].value_data,
            Some(ValueData::I64Value(2))
        );
        assert_eq!(
            filtered_rows[1].values[0].value_data,
            Some(ValueData::I64Value(4))
        );
    }

    #[test]
    fn test_filter_expired_rows_no_ttl() {
        let ttl = None;
        let now = Timestamp::new(7200, TimeUnit::Second);

        let rows = vec![
            Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::I64Value(1)),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampSecondValue(100)),
                    },
                ],
            },
            Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::I64Value(2)),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampSecondValue(200)),
                    },
                ],
            },
        ];

        let (filtered_rows, filtered_count) = filter_expired_rows(rows.clone(), 1, &ttl, &now).unwrap();

        assert_eq!(filtered_count, 0); // No rows filtered
        assert_eq!(filtered_rows.len(), 2); // All rows remain
    }
}
