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

use std::sync::Arc;

use api::v1::column::Values;
use api::v1::greptime_request::Request;
use api::v1::value::ValueData;
use api::v1::{
    Decimal128, InsertRequests, IntervalMonthDayNano, JsonValue, RowInsertRequest,
    RowInsertRequests, json_value,
};
use pipeline::ContextReq;
use snafu::ResultExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{AcquireLimiterSnafu, Result};

pub(crate) type LimiterRef = Arc<Limiter>;

/// A frontend request limiter that controls the total size of in-flight write
/// requests.
pub(crate) struct Limiter {
    max_in_flight_write_bytes: usize,
    byte_counter: Arc<Semaphore>,
}

impl Limiter {
    pub fn new(max_in_flight_write_bytes: usize) -> Self {
        Self {
            byte_counter: Arc::new(Semaphore::new(max_in_flight_write_bytes)),
            max_in_flight_write_bytes,
        }
    }

    pub async fn limit_request(&self, request: &Request) -> Result<OwnedSemaphorePermit> {
        let size = match request {
            Request::Inserts(requests) => self.insert_requests_data_size(requests),
            Request::RowInserts(requests) => {
                self.rows_insert_requests_data_size(requests.inserts.iter())
            }
            _ => 0,
        };
        self.limit_in_flight_write_bytes(size).await
    }

    pub async fn limit_row_inserts(
        &self,
        requests: &RowInsertRequests,
    ) -> Result<OwnedSemaphorePermit> {
        let size = self.rows_insert_requests_data_size(requests.inserts.iter());
        self.limit_in_flight_write_bytes(size).await
    }

    pub async fn limit_ctx_req(&self, opt_req: &ContextReq) -> Result<OwnedSemaphorePermit> {
        let size = self.rows_insert_requests_data_size(opt_req.ref_all_req());
        self.limit_in_flight_write_bytes(size).await
    }

    /// Await until more inflight bytes are available
    pub async fn limit_in_flight_write_bytes(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        self.byte_counter
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .context(AcquireLimiterSnafu)
    }

    /// Returns the current in-flight write bytes.
    #[allow(dead_code)]
    pub fn in_flight_write_bytes(&self) -> usize {
        self.max_in_flight_write_bytes - self.byte_counter.available_permits()
    }

    fn insert_requests_data_size(&self, request: &InsertRequests) -> usize {
        let mut size: usize = 0;
        for insert in &request.inserts {
            for column in &insert.columns {
                if let Some(values) = &column.values {
                    size += Self::size_of_column_values(values);
                }
            }
        }
        size
    }

    fn rows_insert_requests_data_size<'a>(
        &self,
        inserts: impl Iterator<Item = &'a RowInsertRequest>,
    ) -> usize {
        let mut size: usize = 0;
        for insert in inserts {
            if let Some(rows) = &insert.rows {
                for row in &rows.rows {
                    for value in &row.values {
                        if let Some(value) = &value.value_data {
                            size += Self::size_of_value_data(value);
                        }
                    }
                }
            }
        }
        size
    }

    fn size_of_column_values(values: &Values) -> usize {
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
        size += values
            .list_values
            .iter()
            .map(|v| {
                v.items
                    .iter()
                    .map(|item| {
                        item.value_data
                            .as_ref()
                            .map(Self::size_of_value_data)
                            .unwrap_or(0)
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();
        size += values
            .struct_values
            .iter()
            .map(|v| {
                v.items
                    .iter()
                    .map(|item| {
                        item.value_data
                            .as_ref()
                            .map(Self::size_of_value_data)
                            .unwrap_or(0)
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();

        size
    }

    fn size_of_value_data(value: &ValueData) -> usize {
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
            ValueData::ListValue(list_values) => list_values
                .items
                .iter()
                .map(|item| {
                    item.value_data
                        .as_ref()
                        .map(Self::size_of_value_data)
                        .unwrap_or(0)
                })
                .sum(),
            ValueData::StructValue(struct_values) => struct_values
                .items
                .iter()
                .map(|item| {
                    item.value_data
                        .as_ref()
                        .map(Self::size_of_value_data)
                        .unwrap_or(0)
                })
                .sum(),
            ValueData::JsonValue(v) => {
                fn calc(v: &JsonValue) -> usize {
                    let Some(value) = v.value.as_ref() else {
                        return 0;
                    };
                    match value {
                        json_value::Value::Boolean(_) => size_of::<bool>(),
                        json_value::Value::Int(_) => size_of::<i64>(),
                        json_value::Value::Uint(_) => size_of::<u64>(),
                        json_value::Value::Float(_) => size_of::<f64>(),
                        json_value::Value::Str(s) => s.len(),
                        json_value::Value::Array(array) => array.items.iter().map(calc).sum(),
                        json_value::Value::Object(object) => object
                            .entries
                            .iter()
                            .flat_map(|entry| {
                                entry.value.as_ref().map(|v| entry.key.len() + calc(v))
                            })
                            .sum(),
                    }
                }
                calc(v)
            }
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
                let result = limiter
                    .limit_request(&generate_request(request_data_size))
                    .await;
                assert!(result.is_ok());
            });
            handles.push(handle);
        }

        // Wait for all threads to complete.
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_in_flight_write_bytes() {
        let limiter_ref: LimiterRef = Arc::new(Limiter::new(1024));
        let req1 = generate_request(100);
        let result1 = limiter_ref
            .limit_request(&req1)
            .await
            .expect("failed to acquire permits");
        assert_eq!(limiter_ref.in_flight_write_bytes(), 100);

        let req2 = generate_request(200);
        let result2 = limiter_ref
            .limit_request(&req2)
            .await
            .expect("failed to acquire permits");
        assert_eq!(limiter_ref.in_flight_write_bytes(), 300);

        drop(result1);
        assert_eq!(limiter_ref.in_flight_write_bytes(), 200);

        drop(result2);
        assert_eq!(limiter_ref.in_flight_write_bytes(), 0);
    }
}
