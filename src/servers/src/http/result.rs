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
    Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_schema::{DataType, IntervalUnit};
use common_decimal::Decimal128;
use common_recordbatch::RecordBatch;
use common_time::{Date, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use datafusion_common::ScalarValue;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use snafu::ResultExt;

use crate::error::{
    ConvertScalarValueSnafu, DataFusionSnafu, NotSupportedSnafu, Result, ToJsonSnafu,
    UnexpectedResultSnafu,
};

pub(crate) mod arrow_result;
pub(crate) mod csv_result;
pub mod error_result;
pub(crate) mod greptime_manage_resp;
pub mod greptime_result_v1;
pub mod influxdb_result_v1;
pub(crate) mod json_result;
pub(crate) mod null_result;
pub(crate) mod prometheus_resp;
pub(crate) mod table_result;

pub struct HttpOutputWriter {
    columns: usize,
    value_transformer: Option<Box<dyn Fn(Value) -> Value>>,
    current: Option<Vec<serde_json::Value>>,
}

impl HttpOutputWriter {
    pub fn new(columns: usize, value_transformer: Option<Box<dyn Fn(Value) -> Value>>) -> Self {
        Self {
            columns,
            value_transformer,
            current: None,
        }
    }

    fn write_bytes(&mut self, bytes: &[u8], datatype: &ConcreteDataType) -> Result<()> {
        if datatype.is_json() {
            let value = datatypes::types::jsonb_to_serde_json(bytes).map_err(|e| {
                UnexpectedResultSnafu {
                    reason: format!("corrupted jsonb data: {bytes:?}, error: {e}"),
                }
                .build()
            })?;
            self.push(value);
            Ok(())
        } else {
            self.write_value(bytes)
        }
    }

    fn write_value(&mut self, value: impl Into<Value>) -> Result<()> {
        let value = value.into();

        let value = if let Some(f) = &self.value_transformer {
            f(value)
        } else {
            value
        };

        let value = serde_json::Value::try_from(value).context(ToJsonSnafu)?;
        self.push(value);
        Ok(())
    }

    fn push(&mut self, value: serde_json::Value) {
        let current = self
            .current
            .get_or_insert_with(|| Vec::with_capacity(self.columns));
        current.push(value);
    }

    fn finish(&mut self) -> Vec<serde_json::Value> {
        self.current.take().unwrap_or_default()
    }

    pub fn write(
        &mut self,
        record_batch: RecordBatch,
        rows: &mut Vec<Vec<serde_json::Value>>,
    ) -> Result<()> {
        let schema = record_batch.schema.clone();
        let record_batch = record_batch.into_df_record_batch();
        for i in 0..record_batch.num_rows() {
            for (schema, array) in schema
                .column_schemas()
                .iter()
                .zip(record_batch.columns().iter())
            {
                if array.is_null(i) {
                    self.write_value(Value::Null)?;
                    continue;
                }

                match array.data_type() {
                    DataType::Null => {
                        self.write_value(Value::Null)?;
                    }
                    DataType::Boolean => {
                        let array = array.as_boolean();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::UInt8 => {
                        let array = array.as_primitive::<UInt8Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::UInt16 => {
                        let array = array.as_primitive::<UInt16Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::UInt32 => {
                        let array = array.as_primitive::<UInt32Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::UInt64 => {
                        let array = array.as_primitive::<UInt64Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Int8 => {
                        let array = array.as_primitive::<Int8Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Int16 => {
                        let array = array.as_primitive::<Int16Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Int32 => {
                        let array = array.as_primitive::<Int32Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Int64 => {
                        let array = array.as_primitive::<Int64Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Float32 => {
                        let array = array.as_primitive::<Float32Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Float64 => {
                        let array = array.as_primitive::<Float64Type>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Utf8 => {
                        let array = array.as_string::<i32>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::LargeUtf8 => {
                        let array = array.as_string::<i64>();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Utf8View => {
                        let array = array.as_string_view();
                        let v = array.value(i);
                        self.write_value(v)?;
                    }
                    DataType::Binary => {
                        let array = array.as_binary::<i32>();
                        let v = array.value(i);
                        self.write_bytes(v, &schema.data_type)?;
                    }
                    DataType::LargeBinary => {
                        let array = array.as_binary::<i64>();
                        let v = array.value(i);
                        self.write_bytes(v, &schema.data_type)?;
                    }
                    DataType::BinaryView => {
                        let array = array.as_binary_view();
                        let v = array.value(i);
                        self.write_bytes(v, &schema.data_type)?;
                    }
                    DataType::Date32 => {
                        let array = array.as_primitive::<Date32Type>();
                        let v = Date::new(array.value(i));
                        self.write_value(v)?;
                    }
                    DataType::Date64 => {
                        let array = array.as_primitive::<Date64Type>();
                        // `Date64` values are milliseconds representation of `Date32` values,
                        // according to its specification. So we convert the `Date64` value here to
                        // the `Date32` value to process them unified.
                        let v = Date::new((array.value(i) / 86_400_000) as i32);
                        self.write_value(v)?;
                    }
                    DataType::Timestamp(_, _) => {
                        let ts = datatypes::arrow_array::timestamp_array_value(array, i);
                        self.write_value(ts)?;
                    }
                    DataType::Time32(_) | DataType::Time64(_) => {
                        let v = datatypes::arrow_array::time_array_value(array, i);
                        self.write_value(v)?;
                    }
                    DataType::Interval(interval_unit) => match interval_unit {
                        IntervalUnit::YearMonth => {
                            let array = array.as_primitive::<IntervalYearMonthType>();
                            let v: IntervalYearMonth = array.value(i).into();
                            self.write_value(v)?;
                        }
                        IntervalUnit::DayTime => {
                            let array = array.as_primitive::<IntervalDayTimeType>();
                            let v: IntervalDayTime = array.value(i).into();
                            self.write_value(v)?;
                        }
                        IntervalUnit::MonthDayNano => {
                            let array = array.as_primitive::<IntervalMonthDayNanoType>();
                            let v: IntervalMonthDayNano = array.value(i).into();
                            self.write_value(v)?;
                        }
                    },
                    DataType::Duration(_) => {
                        let d = datatypes::arrow_array::duration_array_value(array, i);
                        self.write_value(d)?;
                    }
                    DataType::List(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        let v: Value = v.try_into().context(ConvertScalarValueSnafu)?;
                        self.write_value(v)?;
                    }
                    DataType::Struct(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        let v: Value = v.try_into().context(ConvertScalarValueSnafu)?;
                        self.write_value(v)?;
                    }
                    DataType::Decimal128(precision, scale) => {
                        let array = array.as_primitive::<Decimal128Type>();
                        let v = Decimal128::new(array.value(i), *precision, *scale);
                        self.write_value(v)?;
                    }
                    _ => {
                        return NotSupportedSnafu {
                            feat: format!("convert {} to http output value", array.data_type()),
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
