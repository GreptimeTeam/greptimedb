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

mod error;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow_pg::encoder::encode_value;
use arrow_pg::list_encoder::encode_list;
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use common_recordbatch::RecordBatch;
use common_time::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use datafusion_common::ScalarValue;
use datafusion_expr::LogicalPlan;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::json::JsonStructureSettings;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::types::{IntervalType, TimestampType, jsonb_to_string};
use datatypes::value::StructValue;
use pg_interval::Interval as PgInterval;
use pgwire::api::Type;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::types::format::FormatOptions as PgFormatOptions;
use query::planner::DfLogicalPlanner;
use session::context::QueryContextRef;
use snafu::ResultExt;

pub use self::error::{PgErrorCode, PgErrorSeverity};
use crate::SqlPlan;
use crate::error::{self as server_error, InferParameterTypesSnafu, Result};
use crate::postgres::utils::convert_err;

pub(super) fn schema_to_pg(
    origin: &Schema,
    field_formats: &Format,
    format_options: Option<Arc<PgFormatOptions>>,
) -> Result<Vec<FieldInfo>> {
    origin
        .column_schemas()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let mut field_info = FieldInfo::new(
                col.name.clone(),
                None,
                None,
                type_gt_to_pg(&col.data_type)?,
                field_formats.format_for(idx),
            );
            if let Some(format_options) = &format_options {
                field_info = field_info.with_format_options(format_options.clone());
            }
            Ok(field_info)
        })
        .collect::<Result<Vec<FieldInfo>>>()
}

/// this function will encode greptime's `StructValue` into PostgreSQL jsonb type
///
/// Note that greptimedb has different types of StructValue for storing json data,
/// based on policy defined in `JsonStructureSettings`. But here the `StructValue`
/// should be fully structured.
///
/// there are alternatives like records, arrays, etc. but there are also limitations:
/// records: there is no support for include keys
/// arrays: element in array must be the same type
fn encode_struct(
    _query_ctx: &QueryContextRef,
    struct_value: StructValue,
    builder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    let encoding_setting = JsonStructureSettings::Structured(None);
    let json_value = encoding_setting
        .decode(Value::Struct(struct_value))
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    builder.encode_field(&json_value)
}

pub(crate) struct RecordBatchRowIterator {
    query_ctx: QueryContextRef,
    pg_schema: Arc<Vec<FieldInfo>>,
    schema: SchemaRef,
    record_batch: arrow::record_batch::RecordBatch,
    i: usize,
}

impl Iterator for RecordBatchRowIterator {
    type Item = PgWireResult<DataRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut encoder = DataRowEncoder::new(self.pg_schema.clone());
        if self.i < self.record_batch.num_rows() {
            if let Err(e) = self.encode_row(self.i, &mut encoder) {
                return Some(Err(e));
            }
            self.i += 1;
            Some(Ok(encoder.take_row()))
        } else {
            None
        }
    }
}

impl RecordBatchRowIterator {
    pub(crate) fn new(
        query_ctx: QueryContextRef,
        pg_schema: Arc<Vec<FieldInfo>>,
        record_batch: RecordBatch,
    ) -> Self {
        let schema = record_batch.schema.clone();
        let record_batch = record_batch.into_df_record_batch();
        Self {
            query_ctx,
            pg_schema,
            schema,
            record_batch,
            i: 0,
        }
    }

    fn encode_row(&mut self, i: usize, encoder: &mut DataRowEncoder) -> PgWireResult<()> {
        let arrow_schema = self.record_batch.schema();
        for (j, column) in self.record_batch.columns().iter().enumerate() {
            if column.is_null(i) {
                encoder.encode_field(&None::<&i8>)?;
                continue;
            }

            let pg_field = &self.pg_schema[j];
            match column.data_type() {
                // these types are greptimedb specific or custom
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    // jsonb
                    if let ConcreteDataType::Json(_) = &self.schema.column_schemas()[j].data_type {
                        let v = datatypes::arrow_array::binary_array_value(column, i);
                        let s = jsonb_to_string(v).map_err(convert_err)?;
                        encoder.encode_field(&s)?;
                    } else {
                        // bytea
                        let arrow_field = arrow_schema.field(j);
                        encode_value(encoder, column, i, arrow_field, pg_field)?;
                    }
                }

                DataType::List(_) => {
                    let array = column.as_list::<i32>();
                    let items = array.value(i);

                    encode_list(encoder, items, pg_field)?;
                }
                DataType::Struct(_) => {
                    encode_struct(&self.query_ctx, Default::default(), encoder)?;
                }
                _ => {
                    // Encode value using arrow-pg
                    let arrow_field = arrow_schema.field(j);
                    encode_value(encoder, column, i, arrow_field, pg_field)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn type_gt_to_pg(origin: &ConcreteDataType) -> Result<Type> {
    match origin {
        &ConcreteDataType::Null(_) => Ok(Type::UNKNOWN),
        &ConcreteDataType::Boolean(_) => Ok(Type::BOOL),
        &ConcreteDataType::Int8(_) => Ok(Type::CHAR),
        &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt8(_) => Ok(Type::INT2),
        &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt16(_) => Ok(Type::INT4),
        &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt32(_) => Ok(Type::INT8),
        &ConcreteDataType::UInt64(_) => Ok(Type::NUMERIC),
        &ConcreteDataType::Float32(_) => Ok(Type::FLOAT4),
        &ConcreteDataType::Float64(_) => Ok(Type::FLOAT8),
        &ConcreteDataType::Binary(_) | &ConcreteDataType::Vector(_) => Ok(Type::BYTEA),
        &ConcreteDataType::String(_) => Ok(Type::VARCHAR),
        &ConcreteDataType::Date(_) => Ok(Type::DATE),
        &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP),
        &ConcreteDataType::Time(_) => Ok(Type::TIME),
        &ConcreteDataType::Interval(_) => Ok(Type::INTERVAL),
        &ConcreteDataType::Decimal128(_) => Ok(Type::NUMERIC),
        &ConcreteDataType::Json(_) => Ok(Type::JSON),
        ConcreteDataType::List(list) => match list.item_type() {
            &ConcreteDataType::Null(_) => Ok(Type::UNKNOWN),
            &ConcreteDataType::Boolean(_) => Ok(Type::BOOL_ARRAY),
            &ConcreteDataType::Int8(_) => Ok(Type::CHAR_ARRAY),
            &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt8(_) => Ok(Type::INT2_ARRAY),
            &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt16(_) => Ok(Type::INT4_ARRAY),
            &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt32(_) => Ok(Type::INT8_ARRAY),
            &ConcreteDataType::UInt64(_) => Ok(Type::NUMERIC_ARRAY),
            &ConcreteDataType::Float32(_) => Ok(Type::FLOAT4_ARRAY),
            &ConcreteDataType::Float64(_) => Ok(Type::FLOAT8_ARRAY),
            &ConcreteDataType::Binary(_) => Ok(Type::BYTEA_ARRAY),
            &ConcreteDataType::String(_) => Ok(Type::VARCHAR_ARRAY),
            &ConcreteDataType::Date(_) => Ok(Type::DATE_ARRAY),
            &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP_ARRAY),
            &ConcreteDataType::Time(_) => Ok(Type::TIME_ARRAY),
            &ConcreteDataType::Interval(_) => Ok(Type::INTERVAL_ARRAY),
            &ConcreteDataType::Decimal128(_) => Ok(Type::NUMERIC_ARRAY),
            &ConcreteDataType::Json(_) => Ok(Type::JSON_ARRAY),
            &ConcreteDataType::Duration(_) => Ok(Type::INTERVAL_ARRAY),
            &ConcreteDataType::Struct(_) => Ok(Type::JSON_ARRAY),
            &ConcreteDataType::Dictionary(_)
            | &ConcreteDataType::Vector(_)
            | &ConcreteDataType::List(_) => server_error::UnsupportedDataTypeSnafu {
                data_type: origin,
                reason: "not implemented",
            }
            .fail(),
        },
        &ConcreteDataType::Dictionary(_) => server_error::UnsupportedDataTypeSnafu {
            data_type: origin,
            reason: "not implemented",
        }
        .fail(),
        &ConcreteDataType::Duration(_) => Ok(Type::INTERVAL),
        &ConcreteDataType::Struct(_) => Ok(Type::JSON),
    }
}

#[allow(dead_code)]
pub(super) fn type_pg_to_gt(origin: &Type) -> Result<ConcreteDataType> {
    // Note that we only support a small amount of pg data types
    match origin {
        &Type::BOOL => Ok(ConcreteDataType::boolean_datatype()),
        &Type::CHAR => Ok(ConcreteDataType::int8_datatype()),
        &Type::INT2 => Ok(ConcreteDataType::int16_datatype()),
        &Type::INT4 => Ok(ConcreteDataType::int32_datatype()),
        &Type::INT8 => Ok(ConcreteDataType::int64_datatype()),
        &Type::VARCHAR | &Type::TEXT => Ok(ConcreteDataType::string_datatype()),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => Ok(ConcreteDataType::timestamp_datatype(
            common_time::timestamp::TimeUnit::Millisecond,
        )),
        &Type::DATE => Ok(ConcreteDataType::date_datatype()),
        &Type::TIME => Ok(ConcreteDataType::timestamp_datatype(
            common_time::timestamp::TimeUnit::Microsecond,
        )),
        &Type::CHAR_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int8_datatype(),
        ))),
        &Type::INT2_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int16_datatype(),
        ))),
        &Type::INT4_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int32_datatype(),
        ))),
        &Type::INT8_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int64_datatype(),
        ))),
        &Type::VARCHAR_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::string_datatype(),
        ))),
        _ => server_error::InternalSnafu {
            err_msg: format!("unimplemented datatype {origin:?}"),
        }
        .fail(),
    }
}

pub(super) fn parameter_to_string(portal: &Portal<SqlPlan>, idx: usize) -> PgWireResult<String> {
    // the index is managed from portal's parameters count so it's safe to
    // unwrap here.
    let param_type = portal
        .statement
        .parameter_types
        .get(idx)
        .unwrap()
        .as_ref()
        .unwrap_or(&Type::UNKNOWN);
    match param_type {
        &Type::VARCHAR | &Type::TEXT => Ok(format!(
            "'{}'",
            portal
                .parameter::<String>(idx, param_type)?
                .as_deref()
                .unwrap_or("")
        )),
        &Type::BOOL => Ok(portal
            .parameter::<bool>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT4 => Ok(portal
            .parameter::<i32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT8 => Ok(portal
            .parameter::<i64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT4 => Ok(portal
            .parameter::<f32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT8 => Ok(portal
            .parameter::<f64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::DATE => Ok(portal
            .parameter::<NaiveDate>(idx, param_type)?
            .map(|v| v.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::TIMESTAMP => Ok(portal
            .parameter::<NaiveDateTime>(idx, param_type)?
            .map(|v| v.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INTERVAL => Ok(portal
            .parameter::<PgInterval>(idx, param_type)?
            .map(|v| v.to_sql())
            .unwrap_or_else(|| "".to_owned())),
        _ => Err(invalid_parameter_error(
            "unsupported_parameter_type",
            Some(param_type.to_string()),
        )),
    }
}

pub(super) fn invalid_parameter_error(msg: &str, detail: Option<String>) -> PgWireError {
    let mut error_info = PgErrorCode::Ec22023.to_err_info(msg.to_string());
    error_info.detail = detail;
    PgWireError::UserError(Box::new(error_info))
}

fn to_timestamp_scalar_value<T>(
    data: Option<T>,
    unit: &TimestampType,
    ctype: &ConcreteDataType,
) -> PgWireResult<ScalarValue>
where
    T: Into<i64>,
{
    if let Some(n) = data {
        Value::Timestamp(unit.create_timestamp(n.into()))
            .try_to_scalar_value(ctype)
            .map_err(convert_err)
    } else {
        Ok(ScalarValue::Null)
    }
}

pub(super) fn parameters_to_scalar_values(
    plan: &LogicalPlan,
    portal: &Portal<SqlPlan>,
) -> PgWireResult<Vec<ScalarValue>> {
    let param_count = portal.parameter_len();
    let mut results = Vec::with_capacity(param_count);

    let client_param_types = &portal.statement.parameter_types;
    let server_param_types = DfLogicalPlanner::get_infered_parameter_types(plan)
        .context(InferParameterTypesSnafu)
        .map_err(convert_err)?
        .into_iter()
        .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
        .collect::<HashMap<_, _>>();

    for idx in 0..param_count {
        let server_type = server_param_types
            .get(&format!("${}", idx + 1))
            .and_then(|t| t.as_ref());

        let client_type = if let Some(Some(client_given_type)) = client_param_types.get(idx) {
            client_given_type.clone()
        } else if let Some(server_provided_type) = &server_type {
            type_gt_to_pg(server_provided_type).map_err(convert_err)?
        } else {
            return Err(invalid_parameter_error(
                "unknown_parameter_type",
                Some(format!(
                    "Cannot get parameter type information for parameter {}",
                    idx
                )),
            ));
        };

        let value = match &client_type {
            &Type::VARCHAR | &Type::TEXT => {
                let data = portal.parameter::<String>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::String(t) => {
                            if t.is_large() {
                                ScalarValue::LargeUtf8(data)
                            } else {
                                ScalarValue::Utf8(data)
                            }
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Utf8(data)
                }
            }
            &Type::BOOL => {
                let data = portal.parameter::<bool>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Boolean(_) => ScalarValue::Boolean(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Boolean(data)
                }
            }
            &Type::INT2 => {
                let data = portal.parameter::<i16>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Timestamp(unit) => {
                            to_timestamp_scalar_value(data, unit, server_type)?
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Int16(data)
                }
            }
            &Type::INT4 => {
                let data = portal.parameter::<i32>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Timestamp(unit) => {
                            to_timestamp_scalar_value(data, unit, server_type)?
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Int32(data)
                }
            }
            &Type::INT8 => {
                let data = portal.parameter::<i64>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Timestamp(unit) => {
                            to_timestamp_scalar_value(data, unit, server_type)?
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Int64(data)
                }
            }
            &Type::FLOAT4 => {
                let data = portal.parameter::<f32>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Float32(_) => ScalarValue::Float32(data),
                        ConcreteDataType::Float64(_) => {
                            ScalarValue::Float64(data.map(|n| n as f64))
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Float32(data)
                }
            }
            &Type::FLOAT8 => {
                let data = portal.parameter::<f64>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Float32(_) => {
                            ScalarValue::Float32(data.map(|n| n as f32))
                        }
                        ConcreteDataType::Float64(_) => ScalarValue::Float64(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Float64(data)
                }
            }
            &Type::TIMESTAMP => {
                let data = portal.parameter::<NaiveDateTime>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Timestamp(unit) => match *unit {
                            TimestampType::Second(_) => ScalarValue::TimestampSecond(
                                data.map(|ts| ts.and_utc().timestamp()),
                                None,
                            ),
                            TimestampType::Millisecond(_) => ScalarValue::TimestampMillisecond(
                                data.map(|ts| ts.and_utc().timestamp_millis()),
                                None,
                            ),
                            TimestampType::Microsecond(_) => ScalarValue::TimestampMicrosecond(
                                data.map(|ts| ts.and_utc().timestamp_micros()),
                                None,
                            ),
                            TimestampType::Nanosecond(_) => ScalarValue::TimestampNanosecond(
                                data.and_then(|ts| ts.and_utc().timestamp_nanos_opt()),
                                None,
                            ),
                        },
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::TimestampMillisecond(
                        data.map(|ts| ts.and_utc().timestamp_millis()),
                        None,
                    )
                }
            }
            &Type::TIMESTAMPTZ => {
                let data = portal.parameter::<DateTime<FixedOffset>>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Timestamp(unit) => match *unit {
                            TimestampType::Second(_) => {
                                ScalarValue::TimestampSecond(data.map(|ts| ts.timestamp()), None)
                            }
                            TimestampType::Millisecond(_) => ScalarValue::TimestampMillisecond(
                                data.map(|ts| ts.timestamp_millis()),
                                None,
                            ),
                            TimestampType::Microsecond(_) => ScalarValue::TimestampMicrosecond(
                                data.map(|ts| ts.timestamp_micros()),
                                None,
                            ),
                            TimestampType::Nanosecond(_) => ScalarValue::TimestampNanosecond(
                                data.and_then(|ts| ts.timestamp_nanos_opt()),
                                None,
                            ),
                        },
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::TimestampMillisecond(data.map(|ts| ts.timestamp_millis()), None)
                }
            }
            &Type::DATE => {
                let data = portal.parameter::<NaiveDate>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Date(_) => ScalarValue::Date32(
                            data.map(|d| (d - DateTime::UNIX_EPOCH.date_naive()).num_days() as i32),
                        ),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Date32(
                        data.map(|d| (d - DateTime::UNIX_EPOCH.date_naive()).num_days() as i32),
                    )
                }
            }
            &Type::INTERVAL => {
                let data = portal.parameter::<PgInterval>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Interval(IntervalType::YearMonth(_)) => {
                            ScalarValue::IntervalYearMonth(
                                data.map(|i| {
                                    if i.days != 0 || i.microseconds != 0 {
                                        Err(invalid_parameter_error(
                                            "invalid_parameter_type",
                                            Some(format!(
                                                "Expected: {}, found: {}",
                                                server_type, client_type
                                            )),
                                        ))
                                    } else {
                                        Ok(IntervalYearMonth::new(i.months).to_i32())
                                    }
                                })
                                .transpose()?,
                            )
                        }
                        ConcreteDataType::Interval(IntervalType::DayTime(_)) => {
                            ScalarValue::IntervalDayTime(
                                data.map(|i| {
                                    if i.months != 0 || i.microseconds % 1000 != 0 {
                                        Err(invalid_parameter_error(
                                            "invalid_parameter_type",
                                            Some(format!(
                                                "Expected: {}, found: {}",
                                                server_type, client_type
                                            )),
                                        ))
                                    } else {
                                        Ok(IntervalDayTime::new(
                                            i.days,
                                            (i.microseconds / 1000) as i32,
                                        )
                                        .into())
                                    }
                                })
                                .transpose()?,
                            )
                        }
                        ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => {
                            ScalarValue::IntervalMonthDayNano(data.map(|i| {
                                IntervalMonthDayNano::new(
                                    i.months,
                                    i.days,
                                    i.microseconds * 1_000i64,
                                )
                                .into()
                            }))
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::IntervalMonthDayNano(data.map(|i| {
                        IntervalMonthDayNano::new(i.months, i.days, i.microseconds * 1_000i64)
                            .into()
                    }))
                }
            }
            &Type::BYTEA => {
                let data = portal.parameter::<Vec<u8>>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::String(t) => {
                            let s = data.map(|d| String::from_utf8_lossy(&d).to_string());
                            if t.is_large() {
                                ScalarValue::LargeUtf8(s)
                            } else {
                                ScalarValue::Utf8(s)
                            }
                        }
                        ConcreteDataType::Binary(_) => ScalarValue::Binary(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Binary(data)
                }
            }
            &Type::JSONB => {
                let data = portal.parameter::<serde_json::Value>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Binary(_) => {
                            ScalarValue::Binary(data.map(|d| d.to_string().into_bytes()))
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Binary(data.map(|d| d.to_string().into_bytes()))
                }
            }
            &Type::INT2_ARRAY => {
                let data = portal.parameter::<Vec<i16>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int16, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::INT4_ARRAY => {
                let data = portal.parameter::<Vec<i32>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int32, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::INT8_ARRAY => {
                let data = portal.parameter::<Vec<i64>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int64, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::VARCHAR_ARRAY => {
                let data = portal.parameter::<Vec<String>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Utf8, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::TIMESTAMP_ARRAY => {
                let data = portal.parameter::<Vec<NaiveDateTime>>(idx, &client_type)?;
                if let Some(data) = data {
                    if let Some(ConcreteDataType::List(list_type)) = &server_type {
                        match list_type.item_type() {
                            ConcreteDataType::Timestamp(unit) => match *unit {
                                TimestampType::Second(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampSecond(
                                                Some(ts.and_utc().timestamp()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Second, None),
                                        true,
                                    ))
                                }
                                TimestampType::Millisecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMillisecond(
                                                Some(ts.and_utc().timestamp_millis()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Microsecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMicrosecond(
                                                Some(ts.and_utc().timestamp_micros()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Nanosecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .filter_map(|ts| {
                                            ts.and_utc().timestamp_nanos_opt().map(|nanos| {
                                                ScalarValue::TimestampNanosecond(Some(nanos), None)
                                            })
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                                        true,
                                    ))
                                }
                            },
                            _ => {
                                return Err(invalid_parameter_error(
                                    "invalid_parameter_type",
                                    Some(format!(
                                        "Expected: {}, found: {}",
                                        list_type.item_type(),
                                        client_type
                                    )),
                                ));
                            }
                        }
                    } else {
                        // Default to millisecond when no server type is specified
                        let values = data
                            .into_iter()
                            .map(|ts| {
                                ScalarValue::TimestampMillisecond(
                                    Some(ts.and_utc().timestamp_millis()),
                                    None,
                                )
                            })
                            .collect::<Vec<_>>();
                        ScalarValue::List(ScalarValue::new_list(
                            &values,
                            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                            true,
                        ))
                    }
                } else {
                    ScalarValue::Null
                }
            }
            &Type::TIMESTAMPTZ_ARRAY => {
                let data = portal.parameter::<Vec<DateTime<FixedOffset>>>(idx, &client_type)?;
                if let Some(data) = data {
                    if let Some(ConcreteDataType::List(list_type)) = &server_type {
                        match list_type.item_type() {
                            ConcreteDataType::Timestamp(unit) => match *unit {
                                TimestampType::Second(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampSecond(Some(ts.timestamp()), None)
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Second, None),
                                        true,
                                    ))
                                }
                                TimestampType::Millisecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMillisecond(
                                                Some(ts.timestamp_millis()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Microsecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMicrosecond(
                                                Some(ts.timestamp_micros()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Nanosecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .filter_map(|ts| {
                                            ts.timestamp_nanos_opt().map(|nanos| {
                                                ScalarValue::TimestampNanosecond(Some(nanos), None)
                                            })
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                                        true,
                                    ))
                                }
                            },
                            _ => {
                                return Err(invalid_parameter_error(
                                    "invalid_parameter_type",
                                    Some(format!(
                                        "Expected: {}, found: {}",
                                        list_type.item_type(),
                                        client_type
                                    )),
                                ));
                            }
                        }
                    } else {
                        // Default to millisecond when no server type is specified
                        let values = data
                            .into_iter()
                            .map(|ts| {
                                ScalarValue::TimestampMillisecond(Some(ts.timestamp_millis()), None)
                            })
                            .collect::<Vec<_>>();
                        ScalarValue::List(ScalarValue::new_list(
                            &values,
                            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                            true,
                        ))
                    }
                } else {
                    ScalarValue::Null
                }
            }
            _ => Err(invalid_parameter_error(
                "unsupported_parameter_value",
                Some(format!("Found type: {}", client_type)),
            ))?,
        };

        results.push(value);
    }

    Ok(results)
}

pub(super) fn param_types_to_pg_types(
    param_types: &HashMap<String, Option<ConcreteDataType>>,
) -> Result<Vec<Type>> {
    let param_count = param_types.len();
    let mut types = Vec::with_capacity(param_count);
    for i in 0..param_count {
        if let Some(Some(param_type)) = param_types.get(&format!("${}", i + 1)) {
            let pg_type = type_gt_to_pg(param_type)?;
            types.push(pg_type);
        } else {
            types.push(Type::UNKNOWN);
        }
    }
    Ok(types)
}

pub fn format_options_from_query_ctx(query_ctx: &QueryContextRef) -> Arc<PgFormatOptions> {
    let config = query_ctx.configuration_parameter();
    let (date_style, date_order) = *config.pg_datetime_style();

    let mut format_options = PgFormatOptions::default();
    format_options.date_style = format!("{}, {}", date_style, date_order);
    format_options.interval_style = config.pg_intervalstyle_format().to_string();
    format_options.bytea_output = config.postgres_bytea_output().to_string();
    format_options.time_zone = query_ctx.timezone().to_string();

    Arc::new(format_options)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Float64Builder, Int64Builder, ListBuilder, StringBuilder, TimestampSecondBuilder,
    };
    use arrow_schema::{Field, IntervalUnit};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{
        BinaryVector, BooleanVector, DateVector, Float32Vector, Float64Vector, Int8Vector,
        Int16Vector, Int32Vector, Int64Vector, IntervalDayTimeVector, IntervalMonthDayNanoVector,
        IntervalYearMonthVector, ListVector, NullVector, StringVector, TimeSecondVector,
        TimestampSecondVector, UInt8Vector, UInt16Vector, UInt32Vector, UInt64Vector, VectorRef,
    };
    use pgwire::api::Type;
    use pgwire::api::results::{FieldFormat, FieldInfo};
    use session::context::QueryContextBuilder;

    use super::*;

    #[test]
    fn test_schema_convert() {
        let column_schemas = vec![
            ColumnSchema::new("nulls", ConcreteDataType::null_datatype(), true),
            ColumnSchema::new("bools", ConcreteDataType::boolean_datatype(), true),
            ColumnSchema::new("int8s", ConcreteDataType::int8_datatype(), true),
            ColumnSchema::new("int16s", ConcreteDataType::int16_datatype(), true),
            ColumnSchema::new("int32s", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("int64s", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("uint8s", ConcreteDataType::uint8_datatype(), true),
            ColumnSchema::new("uint16s", ConcreteDataType::uint16_datatype(), true),
            ColumnSchema::new("uint32s", ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new("uint64s", ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new("float32s", ConcreteDataType::float32_datatype(), true),
            ColumnSchema::new("float64s", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("binaries", ConcreteDataType::binary_datatype(), true),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "timestamps",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new("dates", ConcreteDataType::date_datatype(), true),
            ColumnSchema::new("times", ConcreteDataType::time_second_datatype(), true),
            ColumnSchema::new(
                "intervals",
                ConcreteDataType::interval_month_day_nano_datatype(),
                true,
            ),
        ];
        let pg_field_info = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "uint64s".into(),
                None,
                None,
                Type::NUMERIC,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "binaries".into(),
                None,
                None,
                Type::BYTEA,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "strings".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new("dates".into(), None, None, Type::DATE, FieldFormat::Text),
            FieldInfo::new("times".into(), None, None, Type::TIME, FieldFormat::Text),
            FieldInfo::new(
                "intervals".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
        ];
        let schema = Schema::new(column_schemas);
        let fs = schema_to_pg(&schema, &Format::UnifiedText, None).unwrap();
        assert_eq!(fs, pg_field_info);
    }

    #[test]
    fn test_encode_text_format_data() {
        let schema = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "uint64s".into(),
                None,
                None,
                Type::NUMERIC,
                FieldFormat::Text,
            ),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "strings".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "binaries".into(),
                None,
                None,
                Type::BYTEA,
                FieldFormat::Text,
            ),
            FieldInfo::new("dates".into(), None, None, Type::DATE, FieldFormat::Text),
            FieldInfo::new("times".into(), None, None, Type::TIME, FieldFormat::Text),
            FieldInfo::new(
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_year_month".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_day_time".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_month_day_nano".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "int_list".into(),
                None,
                None,
                Type::INT8_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float_list".into(),
                None,
                None,
                Type::FLOAT8_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "string_list".into(),
                None,
                None,
                Type::VARCHAR_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamp_list".into(),
                None,
                None,
                Type::TIMESTAMP_ARRAY,
                FieldFormat::Text,
            ),
        ];

        let arrow_schema = arrow_schema::Schema::new(vec![
            Field::new("x", DataType::Null, true),
            Field::new("x", DataType::Boolean, true),
            Field::new("x", DataType::UInt8, true),
            Field::new("x", DataType::UInt16, true),
            Field::new("x", DataType::UInt32, true),
            Field::new("x", DataType::UInt64, true),
            Field::new("x", DataType::Int8, true),
            Field::new("x", DataType::Int16, true),
            Field::new("x", DataType::Int32, true),
            Field::new("x", DataType::Int64, true),
            Field::new("x", DataType::Float32, true),
            Field::new("x", DataType::Float64, true),
            Field::new("x", DataType::Utf8, true),
            Field::new("x", DataType::Binary, true),
            Field::new("x", DataType::Date32, true),
            Field::new("x", DataType::Time32(TimeUnit::Second), true),
            Field::new("x", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new("x", DataType::Interval(IntervalUnit::YearMonth), true),
            Field::new("x", DataType::Interval(IntervalUnit::DayTime), true),
            Field::new("x", DataType::Interval(IntervalUnit::MonthDayNano), true),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                ))),
                true,
            ),
        ]);

        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_value([Some(1i64), None, Some(2)]);
        builder.append_null();
        builder.append_value([Some(-1i64), None, Some(-2)]);
        let i64_list_array = builder.finish();

        let mut builder = ListBuilder::new(Float64Builder::new());
        builder.append_value([Some(1.0f64), None, Some(2.0)]);
        builder.append_null();
        builder.append_value([Some(-1.0f64), None, Some(-2.0)]);
        let f64_list_array = builder.finish();

        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_value([Some("a"), None, Some("b")]);
        builder.append_null();
        builder.append_value([Some("c"), None, Some("d")]);
        let string_list_array = builder.finish();

        let mut builder = ListBuilder::new(TimestampSecondBuilder::new());
        builder.append_value([Some(1i64), None, Some(2)]);
        builder.append_null();
        builder.append_value([Some(3i64), None, Some(4)]);
        let timestamp_list_array = builder.finish();

        let values = vec![
            Arc::new(NullVector::new(3)) as VectorRef,
            Arc::new(BooleanVector::from(vec![Some(true), Some(false), None])),
            Arc::new(UInt8Vector::from(vec![Some(u8::MAX), Some(u8::MIN), None])),
            Arc::new(UInt16Vector::from(vec![
                Some(u16::MAX),
                Some(u16::MIN),
                None,
            ])),
            Arc::new(UInt32Vector::from(vec![
                Some(u32::MAX),
                Some(u32::MIN),
                None,
            ])),
            Arc::new(UInt64Vector::from(vec![
                Some(u64::MAX),
                Some(u64::MIN),
                None,
            ])),
            Arc::new(Int8Vector::from(vec![Some(i8::MAX), Some(i8::MIN), None])),
            Arc::new(Int16Vector::from(vec![
                Some(i16::MAX),
                Some(i16::MIN),
                None,
            ])),
            Arc::new(Int32Vector::from(vec![
                Some(i32::MAX),
                Some(i32::MIN),
                None,
            ])),
            Arc::new(Int64Vector::from(vec![
                Some(i64::MAX),
                Some(i64::MIN),
                None,
            ])),
            Arc::new(Float32Vector::from(vec![
                None,
                Some(f32::MAX),
                Some(f32::MIN),
            ])),
            Arc::new(Float64Vector::from(vec![
                None,
                Some(f64::MAX),
                Some(f64::MIN),
            ])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
            ])),
            Arc::new(BinaryVector::from(vec![
                None,
                Some("hello".as_bytes().to_vec()),
                Some("world".as_bytes().to_vec()),
            ])),
            Arc::new(DateVector::from(vec![Some(1001), None, Some(1)])),
            Arc::new(TimeSecondVector::from(vec![Some(1001), None, Some(1)])),
            Arc::new(TimestampSecondVector::from(vec![
                Some(1000001),
                None,
                Some(1),
            ])),
            Arc::new(IntervalYearMonthVector::from(vec![Some(1), None, Some(2)])),
            Arc::new(IntervalDayTimeVector::from(vec![
                Some(arrow::datatypes::IntervalDayTime::new(1, 1)),
                None,
                Some(arrow::datatypes::IntervalDayTime::new(2, 2)),
            ])),
            Arc::new(IntervalMonthDayNanoVector::from(vec![
                Some(arrow::datatypes::IntervalMonthDayNano::new(1, 1, 10)),
                None,
                Some(arrow::datatypes::IntervalMonthDayNano::new(2, 2, 20)),
            ])),
            Arc::new(ListVector::from(i64_list_array)),
            Arc::new(ListVector::from(f64_list_array)),
            Arc::new(ListVector::from(string_list_array)),
            Arc::new(ListVector::from(timestamp_list_array)),
        ];
        let record_batch =
            RecordBatch::new(Arc::new(arrow_schema.try_into().unwrap()), values).unwrap();

        let query_context = QueryContextBuilder::default()
            .configuration_parameter(Default::default())
            .build()
            .into();
        let schema = Arc::new(schema);

        let rows = RecordBatchRowIterator::new(query_context, schema.clone(), record_batch)
            .filter_map(|x| x.ok())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row.field_count, schema.len() as i16);
        }
    }

    #[test]
    fn test_invalid_parameter() {
        // test for refactor with PgErrorCode
        let msg = "invalid_parameter_count";
        let error = invalid_parameter_error(msg, None);
        if let PgWireError::UserError(value) = error {
            assert_eq!("ERROR", value.severity);
            assert_eq!("22023", value.code);
            assert_eq!(msg, value.message);
        } else {
            panic!("test_invalid_parameter failed");
        }
    }
}
