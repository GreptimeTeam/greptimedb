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

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::LocalResult;
use common_query::Output;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::RecordBatch;
use common_time::timestamp::TimeUnit;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::{Schema, SchemaRef};
use futures::{future, stream, Stream, StreamExt};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{query_response, DataRowEncoder, FieldFormat, FieldInfo, Response, Tag};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::MemPortalStore;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::statement::Statement;

use super::PostgresServerHandler;
use crate::error::{self, Error, Result};

#[async_trait]
impl SimpleQueryHandler for PostgresServerHandler {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let outputs = self
            .query_handler
            .do_query(query, self.query_ctx.clone())
            .await;

        let mut results = Vec::with_capacity(outputs.len());

        for output in outputs {
            let resp = output_to_query_response(output, FieldFormat::Text)?;
            results.push(resp);
        }

        Ok(results)
    }
}

fn output_to_query_response(
    output: Result<Output>,
    field_format: FieldFormat,
) -> PgWireResult<Response> {
    match output {
        Ok(Output::AffectedRows(rows)) => Ok(Response::Execution(Tag::new_for_execution(
            "OK",
            Some(rows),
        ))),
        Ok(Output::Stream(record_stream)) => {
            let schema = record_stream.schema();
            recordbatches_to_query_response(record_stream, schema, field_format)
        }
        Ok(Output::RecordBatches(recordbatches)) => {
            let schema = recordbatches.schema();
            recordbatches_to_query_response(recordbatches.as_stream(), schema, field_format)
        }
        Err(e) => Ok(Response::Error(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "XX000".to_string(),
            e.to_string(),
        )))),
    }
}

fn recordbatches_to_query_response<S>(
    recordbatches_stream: S,
    schema: SchemaRef,
    field_format: FieldFormat,
) -> PgWireResult<Response>
where
    S: Stream<Item = RecordBatchResult<RecordBatch>> + Send + Unpin + 'static,
{
    let pg_schema = Arc::new(
        schema_to_pg(schema.as_ref(), field_format)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?,
    );
    let ncols = pg_schema.len();

    let pg_schema_ref = pg_schema.clone();
    let data_row_stream = recordbatches_stream
        .map(|record_batch_result| match record_batch_result {
            Ok(rb) => stream::iter(
                // collect rows from a single recordbatch into vector to avoid
                // borrowing it
                rb.rows().map(Ok).collect::<Vec<_>>().into_iter(),
            )
            .boxed(),
            Err(e) => stream::once(future::err(PgWireError::ApiError(Box::new(e)))).boxed(),
        })
        .flatten() // flatten into stream<result<row>>
        .map(move |row| {
            row.and_then(|row| match field_format {
                FieldFormat::Text => {
                    let mut encoder = DataRowEncoder::new(ncols);
                    for value in row.into_iter() {
                        encode_text_value(&value, &mut encoder)?;
                    }
                    encoder.finish()
                }
                FieldFormat::Binary => {
                    let mut encoder = DataRowEncoder::new(ncols);
                    for (idx, value) in row.into_iter().enumerate() {
                        encode_binary_value(&value, pg_schema_ref[idx].datatype(), &mut encoder)?;
                    }
                    encoder.finish()
                }
            })
        });

    match field_format {
        FieldFormat::Text => Ok(Response::Query(query_response(
            Some(pg_schema.deref().clone()),
            data_row_stream,
        ))),
        FieldFormat::Binary => Ok(Response::Query(query_response(None, data_row_stream))),
    }
}

fn schema_to_pg(origin: &Schema, field_format: FieldFormat) -> Result<Vec<FieldInfo>> {
    origin
        .column_schemas()
        .iter()
        .map(|col| {
            Ok(FieldInfo::new(
                col.name.clone(),
                None,
                None,
                type_gt_to_pg(&col.data_type)?,
                field_format,
            ))
        })
        .collect::<Result<Vec<FieldInfo>>>()
}

fn encode_text_value(value: &Value, builder: &mut DataRowEncoder) -> PgWireResult<()> {
    match value {
        Value::Null => builder.encode_text_format_field(None::<&i8>),
        Value::Boolean(v) => builder.encode_text_format_field(Some(v)),
        Value::UInt8(v) => builder.encode_text_format_field(Some(v)),
        Value::UInt16(v) => builder.encode_text_format_field(Some(v)),
        Value::UInt32(v) => builder.encode_text_format_field(Some(v)),
        Value::UInt64(v) => builder.encode_text_format_field(Some(v)),
        Value::Int8(v) => builder.encode_text_format_field(Some(v)),
        Value::Int16(v) => builder.encode_text_format_field(Some(v)),
        Value::Int32(v) => builder.encode_text_format_field(Some(v)),
        Value::Int64(v) => builder.encode_text_format_field(Some(v)),
        Value::Float32(v) => builder.encode_text_format_field(Some(&v.0)),
        Value::Float64(v) => builder.encode_text_format_field(Some(&v.0)),
        Value::String(v) => builder.encode_text_format_field(Some(&v.as_utf8())),
        Value::Binary(v) => builder.encode_text_format_field(Some(&hex::encode(v.deref()))),
        Value::Date(v) => {
            if let Some(date) = v.to_chrono_date() {
                builder.encode_text_format_field(Some(&date.format("%Y-%m-%d").to_string()))
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::DateTime(v) => {
            if let Some(datetime) = v.to_chrono_datetime() {
                builder.encode_text_format_field(Some(
                    &datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                ))
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::Timestamp(v) => {
            if let LocalResult::Single(datetime) = v.to_chrono_datetime() {
                builder.encode_text_format_field(Some(
                    &datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                ))
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::List(_) => Err(PgWireError::ApiError(Box::new(Error::Internal {
            err_msg: format!(
                "cannot write value {:?} in postgres protocol: unimplemented",
                &value
            ),
        }))),
    }
}

fn encode_binary_value(
    value: &Value,
    datatype: &Type,
    builder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    match value {
        Value::Null => builder.encode_binary_format_field(&None::<&i8>, datatype),
        Value::Boolean(v) => builder.encode_binary_format_field(v, datatype),
        Value::UInt8(v) => builder.encode_binary_format_field(&(*v as i8), datatype),
        Value::UInt16(v) => builder.encode_binary_format_field(&(*v as i16), datatype),
        Value::UInt32(v) => builder.encode_binary_format_field(&(*v as i32), datatype),
        Value::UInt64(v) => builder.encode_binary_format_field(&(*v as i64), datatype),
        Value::Int8(v) => builder.encode_binary_format_field(v, datatype),
        Value::Int16(v) => builder.encode_binary_format_field(v, datatype),
        Value::Int32(v) => builder.encode_binary_format_field(v, datatype),
        Value::Int64(v) => builder.encode_binary_format_field(v, datatype),
        Value::Float32(v) => builder.encode_binary_format_field(&v.0, datatype),
        Value::Float64(v) => builder.encode_binary_format_field(&v.0, datatype),
        Value::String(v) => builder.encode_binary_format_field(&v.as_utf8(), datatype),
        Value::Binary(v) => builder.encode_binary_format_field(&v.deref(), datatype),
        Value::Date(v) => {
            if let Some(date) = v.to_chrono_date() {
                builder.encode_binary_format_field(&date, datatype)
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::DateTime(v) => {
            if let Some(datetime) = v.to_chrono_datetime() {
                builder.encode_binary_format_field(&datetime, datatype)
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert datetime to postgres type {v:?}",),
                })))
            }
        }
        Value::Timestamp(v) => {
            // convert timestamp to SystemTime
            if let Some(ts) = v.convert_to(TimeUnit::Microsecond) {
                let sys_time = std::time::UNIX_EPOCH + Duration::from_micros(ts.value() as u64);
                builder.encode_binary_format_field(&sys_time, datatype)
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert timestamp to postgres type {v:?}",),
                })))
            }
        }
        Value::List(_) => Err(PgWireError::ApiError(Box::new(Error::Internal {
            err_msg: format!(
                "cannot write value {:?} in postgres protocol: unimplemented",
                &value
            ),
        }))),
    }
}

fn type_gt_to_pg(origin: &ConcreteDataType) -> Result<Type> {
    match origin {
        &ConcreteDataType::Null(_) => Ok(Type::UNKNOWN),
        &ConcreteDataType::Boolean(_) => Ok(Type::BOOL),
        &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => Ok(Type::CHAR),
        &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => Ok(Type::INT2),
        &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => Ok(Type::INT4),
        &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => Ok(Type::INT8),
        &ConcreteDataType::Float32(_) => Ok(Type::FLOAT4),
        &ConcreteDataType::Float64(_) => Ok(Type::FLOAT8),
        &ConcreteDataType::Binary(_) => Ok(Type::BYTEA),
        &ConcreteDataType::String(_) => Ok(Type::VARCHAR),
        &ConcreteDataType::Date(_) => Ok(Type::DATE),
        &ConcreteDataType::DateTime(_) => Ok(Type::TIMESTAMP),
        &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP),
        &ConcreteDataType::List(_) | &ConcreteDataType::Dictionary(_) => error::InternalSnafu {
            err_msg: format!("not implemented for column datatype {origin:?}"),
        }
        .fail(),
    }
}

fn type_pg_to_gt(origin: &Type) -> Result<ConcreteDataType> {
    // Note that we only support a small amount of pg data types
    match origin {
        &Type::BOOL => Ok(ConcreteDataType::boolean_datatype()),
        &Type::CHAR => Ok(ConcreteDataType::int8_datatype()),
        &Type::INT2 => Ok(ConcreteDataType::int16_datatype()),
        &Type::INT4 => Ok(ConcreteDataType::int32_datatype()),
        &Type::INT8 => Ok(ConcreteDataType::int64_datatype()),
        &Type::VARCHAR | &Type::TEXT => Ok(ConcreteDataType::string_datatype()),
        &Type::TIMESTAMP => Ok(ConcreteDataType::timestamp_datatype(
            common_time::timestamp::TimeUnit::Millisecond,
        )),
        &Type::DATE => Ok(ConcreteDataType::date_datatype()),
        &Type::TIME => Ok(ConcreteDataType::datetime_datatype()),
        _ => error::InternalSnafu {
            err_msg: format!("unimplemented datatype {origin:?}"),
        }
        .fail(),
    }
}

#[derive(Default)]
pub struct POCQueryParser;

impl QueryParser for POCQueryParser {
    type Statement = (Statement, String);

    fn parse_sql(&self, sql: &str, types: &[Type]) -> PgWireResult<Self::Statement> {
        let mut stmts = ParserContext::create_with_dialect(sql, &GenericDialect {})
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() != 1 {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P14".to_owned(),
                "invalid_prepared_statement_definition".to_owned(),
            ))))
        } else {
            let mut stmt = stmts.remove(0);
            if let Statement::Query(qs) = &mut stmt {
                for t in types {
                    let gt_type =
                        type_pg_to_gt(t).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    qs.param_types_mut().push(gt_type);
                }
            }

            Ok((stmt, sql.to_owned()))
        }
    }
}

fn parameter_to_string(portal: &Portal<(Statement, String)>, idx: usize) -> PgWireResult<String> {
    // the index is managed from portal's parameters count so it's safe to
    // unwrap here.
    let param_type = portal.statement().parameter_types().get(idx).unwrap();
    match param_type {
        &Type::VARCHAR | &Type::TEXT => Ok(format!(
            "'{}'",
            portal.parameter::<String>(idx)?.as_deref().unwrap_or("")
        )),
        &Type::BOOL => Ok(portal
            .parameter::<bool>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT4 => Ok(portal
            .parameter::<i32>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT8 => Ok(portal
            .parameter::<i64>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT4 => Ok(portal
            .parameter::<f32>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT8 => Ok(portal
            .parameter::<f64>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22023".to_owned(),
            "unsupported_parameter_value".to_owned(),
        )))),
    }
}

// TODO(sunng87): this is a proof-of-concept implementation of postgres extended
// query. We will choose better `Statement` for caching, a good statement type
// is easy to:
//
// - getting schema from
// - setting parameters in
//
// Datafusion's LogicalPlan is a good candidate for SELECT. But we need to
// confirm it's support for other SQL command like INSERT, UPDATE.
#[async_trait]
impl ExtendedQueryHandler for PostgresServerHandler {
    type Statement = (Statement, String);
    type QueryParser = POCQueryParser;
    type PortalStore = MemPortalStore<Self::Statement>;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (_, sql) = portal.statement().statement();

        // manually replace variables in prepared statement
        let mut sql = sql.to_owned();
        for i in 0..portal.parameter_len() {
            sql = sql.replace(&format!("${}", i + 1), &parameter_to_string(portal, i)?);
        }

        let output = self
            .query_handler
            .do_query(&sql, self.query_ctx.clone())
            .await
            .remove(0);

        output_to_query_response(output, FieldFormat::Binary)
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<Vec<FieldInfo>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (stmt, _) = statement.statement();
        if let Some((schema, _)) = self
            .query_handler
            .do_describe(stmt.clone(), self.query_ctx.clone())
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?
        {
            schema_to_pg(&schema, FieldFormat::Binary)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod test {
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::ListValue;
    use pgwire::api::results::FieldInfo;
    use pgwire::api::Type;

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
        ];
        let pg_field_info = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8, FieldFormat::Text),
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
        ];
        let schema = Schema::new(column_schemas);
        let fs = schema_to_pg(&schema, FieldFormat::Text).unwrap();
        assert_eq!(fs, pg_field_info);
    }

    #[test]
    fn test_encode_text_format_data() {
        let schema = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
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
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
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
            FieldInfo::new(
                "datetimes".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
        ];

        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::UInt8(u8::MAX),
            Value::UInt16(u16::MAX),
            Value::UInt32(u32::MAX),
            Value::UInt64(u64::MAX),
            Value::Int8(i8::MAX),
            Value::Int8(i8::MIN),
            Value::Int16(i16::MAX),
            Value::Int16(i16::MIN),
            Value::Int32(i32::MAX),
            Value::Int32(i32::MIN),
            Value::Int64(i64::MAX),
            Value::Int64(i64::MIN),
            Value::Float32(f32::MAX.into()),
            Value::Float32(f32::MIN.into()),
            Value::Float32(0f32.into()),
            Value::Float64(f64::MAX.into()),
            Value::Float64(f64::MIN.into()),
            Value::Float64(0f64.into()),
            Value::String("greptime".into()),
            Value::Binary("greptime".as_bytes().into()),
            Value::Date(1001i32.into()),
            Value::DateTime(1000001i64.into()),
            Value::Timestamp(1000001i64.into()),
        ];
        let mut builder = DataRowEncoder::new(schema.len());
        for i in values {
            assert!(encode_text_value(&i, &mut builder).is_ok());
        }

        let err = encode_text_value(
            &Value::List(ListValue::new(
                Some(Box::default()),
                ConcreteDataType::int8_datatype(),
            )),
            &mut builder,
        )
        .unwrap_err();
        match err {
            PgWireError::ApiError(e) => {
                assert!(format!("{e}").contains("Internal error:"));
            }
            _ => {
                unreachable!()
            }
        }
    }
}
