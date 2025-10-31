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

use arrow::array::AsArray;
use arrow::datatypes::{
    Date32Type, Date64Type, Decimal128Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use axum::Json;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use common_decimal::Decimal128;
use common_query::{Output, OutputData};
use common_recordbatch::{RecordBatch, util};
use common_time::time::Time;
use common_time::{
    Date, Duration, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, Timestamp,
};
use datafusion_common::ScalarValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{
    ConvertScalarValueSnafu, DataFusionSnafu, Error, NotSupportedSnafu, Result, ToJsonSnafu,
};
use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::result::error_result::ErrorResponse;
use crate::http::{Epoch, HttpResponse, ResponseFormat};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SqlQuery {
    pub db: Option<String>,
    // Returns epoch timestamps with the specified precision.
    // Both u and µ indicate microseconds.
    // epoch = [ns,u,µ,ms,s],
    pub epoch: Option<String>,
    pub sql: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct InfluxdbRecordsOutput {
    // The SQL query does not return the table name, but in InfluxDB,
    // we require the table name, so we set it to an empty string “”.
    name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<Vec<Value>>,
}

impl InfluxdbRecordsOutput {
    pub fn new(columns: Vec<String>, values: Vec<Vec<Value>>) -> Self {
        Self {
            name: String::default(),
            columns,
            values,
        }
    }
}

impl TryFrom<(Option<Epoch>, Vec<RecordBatch>)> for InfluxdbRecordsOutput {
    type Error = Error;

    fn try_from(
        (epoch, recordbatches): (Option<Epoch>, Vec<RecordBatch>),
    ) -> Result<InfluxdbRecordsOutput, Self::Error> {
        if recordbatches.is_empty() {
            Ok(InfluxdbRecordsOutput::new(vec![], vec![]))
        } else {
            // Safety: ensured by previous empty check
            let first = &recordbatches[0];
            let columns = first
                .schema
                .column_schemas()
                .iter()
                .map(|cs| cs.name.clone())
                .collect::<Vec<_>>();

            let mut rows =
                Vec::with_capacity(recordbatches.iter().map(|r| r.num_rows()).sum::<usize>());

            for recordbatch in recordbatches {
                let mut writer = RowWriter::new(epoch, recordbatch.num_columns());
                writer.write(recordbatch, &mut rows)?;
            }

            Ok(InfluxdbRecordsOutput::new(columns, rows))
        }
    }
}

struct RowWriter {
    epoch: Option<Epoch>,
    columns: usize,
    current: Option<Vec<Value>>,
}

impl RowWriter {
    fn new(epoch: Option<Epoch>, columns: usize) -> Self {
        Self {
            epoch,
            columns,
            current: None,
        }
    }

    fn push(&mut self, value: impl Into<datatypes::value::Value>) -> Result<()> {
        let value = value.into();

        let current = self
            .current
            .get_or_insert_with(|| Vec::with_capacity(self.columns));
        let value = Value::try_from(value).context(ToJsonSnafu)?;
        current.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Vec<Value> {
        self.current.take().unwrap_or_default()
    }

    fn write(&mut self, record_batch: RecordBatch, rows: &mut Vec<Vec<Value>>) -> Result<()> {
        let record_batch = record_batch.into_df_record_batch();
        for i in 0..record_batch.num_rows() {
            for array in record_batch.columns().iter() {
                if array.is_null(i) {
                    self.push(None::<bool>)?;
                    continue;
                }

                match array.data_type() {
                    DataType::Null => {
                        self.push(None::<bool>)?;
                    }
                    DataType::Boolean => {
                        let array = array.as_boolean();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::UInt8 => {
                        let array = array.as_primitive::<UInt8Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::UInt16 => {
                        let array = array.as_primitive::<UInt16Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::UInt32 => {
                        let array = array.as_primitive::<UInt32Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::UInt64 => {
                        let array = array.as_primitive::<UInt64Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Int8 => {
                        let array = array.as_primitive::<Int8Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Int16 => {
                        let array = array.as_primitive::<Int16Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Int32 => {
                        let array = array.as_primitive::<Int32Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Int64 => {
                        let array = array.as_primitive::<Int64Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Float32 => {
                        let array = array.as_primitive::<Float32Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Float64 => {
                        let array = array.as_primitive::<Float64Type>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Utf8 => {
                        let array = array.as_string::<i32>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::LargeUtf8 => {
                        let array = array.as_string::<i64>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Utf8View => {
                        let array = array.as_string_view();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Binary => {
                        let array = array.as_binary::<i32>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::LargeBinary => {
                        let array = array.as_binary::<i64>();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::BinaryView => {
                        let array = array.as_binary_view();
                        let v = array.value(i);
                        self.push(v)?;
                    }
                    DataType::Date32 => {
                        let array = array.as_primitive::<Date32Type>();
                        let v = Date::new(array.value(i));
                        self.push(v)?;
                    }
                    DataType::Date64 => {
                        let array = array.as_primitive::<Date64Type>();
                        // `Date64` values are milliseconds representation of `Date32` values,
                        // according to its specification. So we convert the `Date64` value here to
                        // the `Date32` value to process them unified.
                        let v = Date::new((array.value(i) / 86_400_000) as i32);
                        self.push(v)?;
                    }
                    DataType::Timestamp(time_unit, _) => {
                        let v = match time_unit {
                            TimeUnit::Second => {
                                let array = array.as_primitive::<TimestampSecondType>();
                                array.value(i)
                            }
                            TimeUnit::Millisecond => {
                                let array = array.as_primitive::<TimestampMillisecondType>();
                                array.value(i)
                            }
                            TimeUnit::Microsecond => {
                                let array = array.as_primitive::<TimestampMicrosecondType>();
                                array.value(i)
                            }
                            TimeUnit::Nanosecond => {
                                let array = array.as_primitive::<TimestampNanosecondType>();
                                array.value(i)
                            }
                        };
                        let mut ts = Timestamp::new(v, time_unit.into());
                        if let Some(epoch) = self.epoch
                            && let Some(converted) = epoch.convert_timestamp(ts)
                        {
                            ts = converted;
                        }
                        self.push(ts)?;
                    }
                    DataType::Time32(time_unit) | DataType::Time64(time_unit) => {
                        let v = match time_unit {
                            TimeUnit::Second => {
                                let array = array.as_primitive::<Time32SecondType>();
                                Time::new_second(array.value(i) as i64)
                            }
                            TimeUnit::Millisecond => {
                                let array = array.as_primitive::<Time32MillisecondType>();
                                Time::new_millisecond(array.value(i) as i64)
                            }
                            TimeUnit::Microsecond => {
                                let array = array.as_primitive::<Time64MicrosecondType>();
                                Time::new_microsecond(array.value(i))
                            }
                            TimeUnit::Nanosecond => {
                                let array = array.as_primitive::<Time64NanosecondType>();
                                Time::new_nanosecond(array.value(i))
                            }
                        };
                        self.push(v)?;
                    }
                    DataType::Interval(interval_unit) => match interval_unit {
                        IntervalUnit::YearMonth => {
                            let array = array.as_primitive::<IntervalYearMonthType>();
                            let v: IntervalYearMonth = array.value(i).into();
                            self.push(v)?;
                        }
                        IntervalUnit::DayTime => {
                            let array = array.as_primitive::<IntervalDayTimeType>();
                            let v: IntervalDayTime = array.value(i).into();
                            self.push(v)?;
                        }
                        IntervalUnit::MonthDayNano => {
                            let array = array.as_primitive::<IntervalMonthDayNanoType>();
                            let v: IntervalMonthDayNano = array.value(i).into();
                            self.push(v)?;
                        }
                    },
                    DataType::Duration(time_unit) => {
                        let v = match time_unit {
                            TimeUnit::Second => {
                                let array = array.as_primitive::<DurationSecondType>();
                                array.value(i)
                            }
                            TimeUnit::Millisecond => {
                                let array = array.as_primitive::<DurationMillisecondType>();
                                array.value(i)
                            }
                            TimeUnit::Microsecond => {
                                let array = array.as_primitive::<DurationMicrosecondType>();
                                array.value(i)
                            }
                            TimeUnit::Nanosecond => {
                                let array = array.as_primitive::<DurationNanosecondType>();
                                array.value(i)
                            }
                        };
                        let d = Duration::new(v, time_unit.into());
                        self.push(d)?;
                    }
                    DataType::List(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        let v: datatypes::value::Value =
                            v.try_into().context(ConvertScalarValueSnafu)?;
                        self.push(v)?;
                    }
                    DataType::Struct(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        let v: datatypes::value::Value =
                            v.try_into().context(ConvertScalarValueSnafu)?;
                        self.push(v)?;
                    }
                    DataType::Decimal128(precision, scale) => {
                        let array = array.as_primitive::<Decimal128Type>();
                        let v = Decimal128::new(array.value(i), *precision, *scale);
                        self.push(v)?;
                    }
                    _ => {
                        return NotSupportedSnafu {
                            feat: format!("convert {} to influxdb value", array.data_type()),
                        }
                        .fail();
                    }
                }
            }

            rows.push(self.finish())
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct InfluxdbOutput {
    pub statement_id: u32,
    pub series: Vec<InfluxdbRecordsOutput>,
}

impl InfluxdbOutput {
    pub fn num_rows(&self) -> usize {
        self.series.iter().map(|r| r.values.len()).sum()
    }

    pub fn num_cols(&self) -> usize {
        self.series
            .first()
            .map(|r| r.columns.len())
            .unwrap_or(0usize)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfluxdbV1Response {
    results: Vec<InfluxdbOutput>,
    execution_time_ms: u64,
}

impl InfluxdbV1Response {
    pub fn with_execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time_ms = execution_time;
        self
    }

    /// Create a influxdb v1 response from query result
    pub async fn from_output(
        outputs: Vec<crate::error::Result<Output>>,
        epoch: Option<Epoch>,
    ) -> HttpResponse {
        // TODO(sunng87): this api response structure cannot represent error well.
        //  It hides successful execution results from error response
        let mut results = Vec::with_capacity(outputs.len());
        for (statement_id, out) in outputs.into_iter().enumerate() {
            let statement_id = statement_id as u32;
            match out {
                Ok(o) => {
                    match o.data {
                        OutputData::AffectedRows(_) => {
                            results.push(InfluxdbOutput {
                                statement_id,
                                series: vec![],
                            });
                        }
                        OutputData::Stream(stream) => {
                            // TODO(sunng87): streaming response
                            match util::collect(stream).await {
                                Ok(rows) => match InfluxdbRecordsOutput::try_from((epoch, rows)) {
                                    Ok(rows) => {
                                        results.push(InfluxdbOutput {
                                            statement_id,
                                            series: vec![rows],
                                        });
                                    }
                                    Err(err) => {
                                        return HttpResponse::Error(ErrorResponse::from_error(err));
                                    }
                                },
                                Err(err) => {
                                    return HttpResponse::Error(ErrorResponse::from_error(err));
                                }
                            }
                        }
                        OutputData::RecordBatches(rbs) => {
                            match InfluxdbRecordsOutput::try_from((epoch, rbs.take())) {
                                Ok(rows) => {
                                    results.push(InfluxdbOutput {
                                        statement_id,
                                        series: vec![rows],
                                    });
                                }
                                Err(err) => {
                                    return HttpResponse::Error(ErrorResponse::from_error(err));
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    return HttpResponse::Error(ErrorResponse::from_error(err));
                }
            }
        }

        HttpResponse::InfluxdbV1(InfluxdbV1Response {
            results,
            execution_time_ms: 0,
        })
    }

    pub fn results(&self) -> &[InfluxdbOutput] {
        &self.results
    }

    pub fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }
}

impl IntoResponse for InfluxdbV1Response {
    fn into_response(self) -> Response {
        let execution_time = self.execution_time_ms;
        let mut resp = Json(self).into_response();
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_FORMAT,
            HeaderValue::from_static(ResponseFormat::InfluxdbV1.as_str()),
        );
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        resp
    }
}
