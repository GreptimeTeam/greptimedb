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
use datatypes::types::jsonb_to_string;
use datatypes::value::Value;
use snafu::ResultExt;

use crate::error::{
    ConvertScalarValueSnafu, ConvertSqlValueSnafu, DataFusionSnafu, NotSupportedSnafu, Result,
    ToJsonSnafu,
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

pub(super) struct HttpOutputWriter {
    columns: usize,
    value_transformer: Option<Box<dyn Fn(Value) -> Value>>,
    current: Option<Vec<serde_json::Value>>,
}

impl HttpOutputWriter {
    pub(super) fn new(
        columns: usize,
        value_transformer: Option<Box<dyn Fn(Value) -> Value>>,
    ) -> Self {
        Self {
            columns,
            value_transformer,
            current: None,
        }
    }

    fn push(&mut self, value: impl Into<Value>) -> Result<()> {
        let value = value.into();

        let value = if let Some(f) = &self.value_transformer {
            f(value)
        } else {
            value
        };

        let value = serde_json::Value::try_from(value).context(ToJsonSnafu)?;

        let current = self
            .current
            .get_or_insert_with(|| Vec::with_capacity(self.columns));
        current.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Vec<serde_json::Value> {
        self.current.take().unwrap_or_default()
    }

    pub(super) fn write(
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
                    self.push(Value::Null)?;
                    continue;
                }

                match array.data_type() {
                    DataType::Null => {
                        self.push(Value::Null)?;
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
                        if schema.data_type.is_json() {
                            let v = jsonb_to_string(v).context(ConvertSqlValueSnafu)?;
                            self.push(v)?;
                        } else {
                            self.push(v)?;
                        }
                    }
                    DataType::LargeBinary => {
                        let array = array.as_binary::<i64>();
                        let v = array.value(i);
                        if schema.data_type.is_json() {
                            let v = jsonb_to_string(v).context(ConvertSqlValueSnafu)?;
                            self.push(v)?;
                        } else {
                            self.push(v)?;
                        }
                    }
                    DataType::BinaryView => {
                        let array = array.as_binary_view();
                        let v = array.value(i);
                        if schema.data_type.is_json() {
                            let v = jsonb_to_string(v).context(ConvertSqlValueSnafu)?;
                            self.push(v)?;
                        } else {
                            self.push(v)?;
                        }
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
                    DataType::Timestamp(_, _) => {
                        let ts = datatypes::arrow_array::timestamp_array_value(array, i);
                        self.push(ts)?;
                    }
                    DataType::Time32(_) | DataType::Time64(_) => {
                        let v = datatypes::arrow_array::time_array_value(array, i);
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
                    DataType::Duration(_) => {
                        let d = datatypes::arrow_array::duration_array_value(array, i);
                        self.push(d)?;
                    }
                    DataType::List(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        let v: Value = v.try_into().context(ConvertScalarValueSnafu)?;
                        self.push(v)?;
                    }
                    DataType::Struct(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        let v: Value = v.try_into().context(ConvertScalarValueSnafu)?;
                        self.push(v)?;
                    }
                    DataType::Decimal128(precision, scale) => {
                        let array = array.as_primitive::<Decimal128Type>();
                        let v = Decimal128::new(array.value(i), *precision, *scale);
                        self.push(v)?;
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
