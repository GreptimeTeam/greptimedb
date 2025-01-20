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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use api::v1::column::Values;
use api::v1::greptime_request::Request;
use api::v1::value::ValueData;
use api::v1::{Decimal128, InsertRequests, IntervalMonthDayNano, RowInsertRequests};
use common_telemetry::{debug, warn};

pub(crate) type LimiterRef = Arc<Limiter>;

/// A frontend request limiter that controls the total size of in-flight write requests.
pub(crate) struct Limiter {
    // The maximum number of bytes that can be in flight.
    max_in_flight_write_bytes: u64,

    // The current in-flight write bytes.
    in_flight_write_bytes: Arc<AtomicU64>,
}

/// A counter for the in-flight write bytes.
pub(crate) struct InFlightWriteBytesCounter {
    // The current in-flight write bytes.
    in_flight_write_bytes: Arc<AtomicU64>,

    // The write bytes that are being processed.
    processing_write_bytes: u64,
}

impl InFlightWriteBytesCounter {
    /// Creates a new InFlightWriteBytesCounter. It will decrease the in-flight write bytes when dropped.
    pub fn new(in_flight_write_bytes: Arc<AtomicU64>, processing_write_bytes: u64) -> Self {
        debug!(
            "processing write bytes: {}, current in-flight write bytes: {}",
            processing_write_bytes,
            in_flight_write_bytes.load(Ordering::Relaxed)
        );
        Self {
            in_flight_write_bytes,
            processing_write_bytes,
        }
    }
}

impl Drop for InFlightWriteBytesCounter {
    // When the request is finished, the in-flight write bytes should be decreased.
    fn drop(&mut self) {
        self.in_flight_write_bytes
            .fetch_sub(self.processing_write_bytes, Ordering::Relaxed);
    }
}

impl Limiter {
    pub fn new(max_in_flight_write_bytes: u64) -> Self {
        Self {
            max_in_flight_write_bytes,
            in_flight_write_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn limit_request(&self, request: &Request) -> Option<InFlightWriteBytesCounter> {
        let size = match request {
            Request::Inserts(requests) => self.insert_requests_data_size(requests),
            Request::RowInserts(requests) => self.rows_insert_requests_data_size(requests),
            _ => 0,
        };
        self.limit_in_flight_write_bytes(size as u64)
    }

    pub fn limit_row_inserts(
        &self,
        requests: &RowInsertRequests,
    ) -> Option<InFlightWriteBytesCounter> {
        let size = self.rows_insert_requests_data_size(requests);
        self.limit_in_flight_write_bytes(size as u64)
    }

    /// Returns None if the in-flight write bytes exceed the maximum limit.
    /// Otherwise, returns Some(InFlightWriteBytesCounter) and the in-flight write bytes will be increased.
    pub fn limit_in_flight_write_bytes(&self, bytes: u64) -> Option<InFlightWriteBytesCounter> {
        let result = self.in_flight_write_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| {
                if current + bytes > self.max_in_flight_write_bytes {
                    warn!(
                        "in-flight write bytes exceed the maximum limit {}, request with {} bytes will be limited",
                        self.max_in_flight_write_bytes,
                        bytes
                    );
                    return None;
                }
                Some(current + bytes)
            },
        );

        match result {
            // Update the in-flight write bytes successfully.
            Ok(_) => Some(InFlightWriteBytesCounter::new(
                self.in_flight_write_bytes.clone(),
                bytes,
            )),
            // It means the in-flight write bytes exceed the maximum limit.
            Err(_) => None,
        }
    }

    /// Returns the current in-flight write bytes.
    #[allow(dead_code)]
    pub fn in_flight_write_bytes(&self) -> u64 {
        self.in_flight_write_bytes.load(Ordering::Relaxed)
    }

    fn insert_requests_data_size(&self, request: &InsertRequests) -> usize {
        let mut size: usize = 0;
        for insert in &request.inserts {
            for column in &insert.columns {
                if let Some(values) = &column.values {
                    size += self.size_of_column_values(values);
                }
            }
        }
        size
    }

    fn rows_insert_requests_data_size(&self, request: &RowInsertRequests) -> usize {
        let mut size: usize = 0;
        for insert in &request.inserts {
            if let Some(rows) = &insert.rows {
                for row in &rows.rows {
                    for value in &row.values {
                        if let Some(value) = &value.value_data {
                            size += self.size_of_value_data(value);
                        }
                    }
                }
            }
        }
        size
    }

    fn size_of_column_values(&self, values: &Values) -> usize {
        let mut size: usize = 0;
        size += values.i8_values.len() * size_of::<i32>();
        size += values.i16_values.len() * size_of::<i32>();
        size += values.i32_values.len() * size_of::<i32>();
        size += values.i64_values.len() * size_of::<i64>();
        size += values.u8_values.len() * size_of::<u32>();
        size += values.u16_values.len() * size_of::<u32>();
        size += values.u32_values.len() * size_of::<u32>();
        size += values.u64_values.len() * size_of::<u64>();
        size += values.f32_values.len() * size_of::<f32>();
        size += values.f64_values.len() * size_of::<f64>();
        size += values.bool_values.len() * size_of::<bool>();
        size += values
            .binary_values
            .iter()
            .map(|v| v.len() * size_of::<u8>())
            .sum::<usize>();
        size += values.string_values.iter().map(|v| v.len()).sum::<usize>();
        size += values.date_values.len() * size_of::<i32>();
        size += values.datetime_values.len() * size_of::<i64>();
        size += values.timestamp_second_values.len() * size_of::<i64>();
        size += values.timestamp_millisecond_values.len() * size_of::<i64>();
        size += values.timestamp_microsecond_values.len() * size_of::<i64>();
        size += values.timestamp_nanosecond_values.len() * size_of::<i64>();
        size += values.time_second_values.len() * size_of::<i64>();
        size += values.time_millisecond_values.len() * size_of::<i64>();
        size += values.time_microsecond_values.len() * size_of::<i64>();
        size += values.time_nanosecond_values.len() * size_of::<i64>();
        size += values.interval_year_month_values.len() * size_of::<i64>();
        size += values.interval_day_time_values.len() * size_of::<i64>();
        size += values.interval_month_day_nano_values.len() * size_of::<IntervalMonthDayNano>();
        size += values.decimal128_values.len() * size_of::<Decimal128>();
        size
    }

    fn size_of_value_data(&self, value: &ValueData) -> usize {
        match value {
            ValueData::I8Value(_) => size_of::<i32>(),
            ValueData::I16Value(_) => size_of::<i32>(),
            ValueData::I32Value(_) => size_of::<i32>(),
            ValueData::I64Value(_) => size_of::<i64>(),
            ValueData::U8Value(_) => size_of::<u32>(),
            ValueData::U16Value(_) => size_of::<u32>(),
            ValueData::U32Value(_) => size_of::<u32>(),
            ValueData::U64Value(_) => size_of::<u64>(),
            ValueData::F32Value(_) => size_of::<f32>(),
            ValueData::F64Value(_) => size_of::<f64>(),
            ValueData::BoolValue(_) => size_of::<bool>(),
            ValueData::BinaryValue(v) => v.len() * size_of::<u8>(),
            ValueData::StringValue(v) => v.len(),
            ValueData::DateValue(_) => size_of::<i32>(),
            ValueData::DatetimeValue(_) => size_of::<i64>(),
            ValueData::TimestampSecondValue(_) => size_of::<i64>(),
            ValueData::TimestampMillisecondValue(_) => size_of::<i64>(),
            ValueData::TimestampMicrosecondValue(_) => size_of::<i64>(),
            ValueData::TimestampNanosecondValue(_) => size_of::<i64>(),
            ValueData::TimeSecondValue(_) => size_of::<i64>(),
            ValueData::TimeMillisecondValue(_) => size_of::<i64>(),
            ValueData::TimeMicrosecondValue(_) => size_of::<i64>(),
            ValueData::TimeNanosecondValue(_) => size_of::<i64>(),
            ValueData::IntervalYearMonthValue(_) => size_of::<i32>(),
            ValueData::IntervalDayTimeValue(_) => size_of::<i64>(),
            ValueData::IntervalMonthDayNanoValue(_) => size_of::<IntervalMonthDayNano>(),
            ValueData::Decimal128Value(_) => size_of::<Decimal128>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use api::v1::greptime_request::Request;
    use api::v1::{Column, InsertRequest};

    use super::*;

    fn generate_request(size: usize) -> Request {
        let i8_values = vec![0; size / 4];
        Request::Inserts(InsertRequests {
            inserts: vec![InsertRequest {
                columns: vec![Column {
                    values: Some(Values {
                        i8_values,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        })
    }

    #[tokio::test]
    async fn test_limiter() {
        let limiter_ref: LimiterRef = Arc::new(Limiter::new(1024));
        let tasks_count = 10;
        let request_data_size = 100;
        let mut handles = vec![];

        // Generate multiple requests to test the limiter.
        for _ in 0..tasks_count {
            let limiter = limiter_ref.clone();
            let handle = tokio::spawn(async move {
                let result = limiter.limit_request(&generate_request(request_data_size));
                assert!(result.is_some());
            });
            handles.push(handle);
        }

        // Wait for all threads to complete.
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[test]
    fn test_in_flight_write_bytes() {
        let limiter_ref: LimiterRef = Arc::new(Limiter::new(1024));
        let req1 = generate_request(100);
        let result1 = limiter_ref.limit_request(&req1);
        assert!(result1.is_some());
        assert_eq!(limiter_ref.in_flight_write_bytes(), 100);

        let req2 = generate_request(200);
        let result2 = limiter_ref.limit_request(&req2);
        assert!(result2.is_some());
        assert_eq!(limiter_ref.in_flight_write_bytes(), 300);

        drop(result1.unwrap());
        assert_eq!(limiter_ref.in_flight_write_bytes(), 200);

        drop(result2.unwrap());
        assert_eq!(limiter_ref.in_flight_write_bytes(), 0);
    }
}
