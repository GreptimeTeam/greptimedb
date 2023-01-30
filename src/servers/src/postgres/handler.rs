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

use async_trait::async_trait;
use common_query::Output;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::RecordBatch;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::SchemaRef;
use futures::{future, stream, Stream, StreamExt};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{text_query_response, FieldInfo, Response, Tag, TextDataRowEncoder};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::MemPortalStore;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

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
            let resp = match output {
                Ok(Output::AffectedRows(rows)) => {
                    Response::Execution(Tag::new_for_execution("OK", Some(rows)))
                }
                Ok(Output::Stream(record_stream)) => {
                    let schema = record_stream.schema();
                    recordbatches_to_query_response(record_stream, schema)?
                }
                Ok(Output::RecordBatches(recordbatches)) => {
                    let schema = recordbatches.schema();
                    recordbatches_to_query_response(
                        stream::iter(recordbatches.take().into_iter().map(Ok)),
                        schema,
                    )?
                }
                Err(e) => Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "XX000".to_string(),
                    e.to_string(),
                ))),
            };
            results.push(resp);
        }

        Ok(results)
    }
}

fn recordbatches_to_query_response<S>(
    recordbatches_stream: S,
    schema: SchemaRef,
) -> PgWireResult<Response>
where
    S: Stream<Item = RecordBatchResult<RecordBatch>> + Send + Unpin + 'static,
{
    let pg_schema = schema_to_pg(schema).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let ncols = pg_schema.len();

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
            row.and_then(|row| {
                let mut encoder = TextDataRowEncoder::new(ncols);
                for value in row.into_iter() {
                    encode_value(&value, &mut encoder)?;
                }
                encoder.finish()
            })
        });

    Ok(Response::Query(text_query_response(
        pg_schema,
        data_row_stream,
    )))
}

fn schema_to_pg(origin: SchemaRef) -> Result<Vec<FieldInfo>> {
    origin
        .column_schemas()
        .iter()
        .map(|col| {
            Ok(FieldInfo::new(
                col.name.clone(),
                None,
                None,
                type_translate(&col.data_type)?,
            ))
        })
        .collect::<Result<Vec<FieldInfo>>>()
}

fn encode_value(value: &Value, builder: &mut TextDataRowEncoder) -> PgWireResult<()> {
    match value {
        Value::Null => builder.append_field(None::<&i8>),
        Value::Boolean(v) => builder.append_field(Some(v)),
        Value::UInt8(v) => builder.append_field(Some(v)),
        Value::UInt16(v) => builder.append_field(Some(v)),
        Value::UInt32(v) => builder.append_field(Some(v)),
        Value::UInt64(v) => builder.append_field(Some(v)),
        Value::Int8(v) => builder.append_field(Some(v)),
        Value::Int16(v) => builder.append_field(Some(v)),
        Value::Int32(v) => builder.append_field(Some(v)),
        Value::Int64(v) => builder.append_field(Some(v)),
        Value::Float32(v) => builder.append_field(Some(&v.0)),
        Value::Float64(v) => builder.append_field(Some(&v.0)),
        Value::String(v) => builder.append_field(Some(&v.as_utf8())),
        Value::Binary(v) => builder.append_field(Some(&hex::encode(v.deref()))),
        Value::Date(v) => builder.append_field(Some(&v.to_string())),
        Value::DateTime(v) => builder.append_field(Some(&v.to_string())),
        Value::Timestamp(v) => builder.append_field(Some(&v.to_iso8601_string())),
        Value::List(_) => Err(PgWireError::ApiError(Box::new(Error::Internal {
            err_msg: format!(
                "cannot write value {:?} in postgres protocol: unimplemented",
                &value
            ),
        }))),
    }
}

fn type_translate(origin: &ConcreteDataType) -> Result<Type> {
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
        &ConcreteDataType::List(_) => error::InternalSnafu {
            err_msg: format!("not implemented for column datatype {origin:?}"),
        }
        .fail(),
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresServerHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;
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
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(Response::Error(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            "Extended query is not implemented on this server yet".to_owned(),
        ))))
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        _statement: &Self::Statement,
    ) -> PgWireResult<Vec<FieldInfo>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(vec![])
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

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
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN),
            FieldInfo::new("bools".into(), None, None, Type::BOOL),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR),
            FieldInfo::new("int16s".into(), None, None, Type::INT2),
            FieldInfo::new("int32s".into(), None, None, Type::INT4),
            FieldInfo::new("int64s".into(), None, None, Type::INT8),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8),
            FieldInfo::new("float32s".into(), None, None, Type::FLOAT4),
            FieldInfo::new("float64s".into(), None, None, Type::FLOAT8),
            FieldInfo::new("binaries".into(), None, None, Type::BYTEA),
            FieldInfo::new("strings".into(), None, None, Type::VARCHAR),
            FieldInfo::new("timestamps".into(), None, None, Type::TIMESTAMP),
            FieldInfo::new("dates".into(), None, None, Type::DATE),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let fs = schema_to_pg(schema).unwrap();
        assert_eq!(fs, pg_field_info);
    }

    #[test]
    fn test_encode_text_format_data() {
        let schema = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN),
            FieldInfo::new("bools".into(), None, None, Type::BOOL),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR),
            FieldInfo::new("int16s".into(), None, None, Type::INT2),
            FieldInfo::new("int16s".into(), None, None, Type::INT2),
            FieldInfo::new("int32s".into(), None, None, Type::INT4),
            FieldInfo::new("int32s".into(), None, None, Type::INT4),
            FieldInfo::new("int64s".into(), None, None, Type::INT8),
            FieldInfo::new("int64s".into(), None, None, Type::INT8),
            FieldInfo::new("float32s".into(), None, None, Type::FLOAT4),
            FieldInfo::new("float32s".into(), None, None, Type::FLOAT4),
            FieldInfo::new("float32s".into(), None, None, Type::FLOAT4),
            FieldInfo::new("float64s".into(), None, None, Type::FLOAT8),
            FieldInfo::new("float64s".into(), None, None, Type::FLOAT8),
            FieldInfo::new("float64s".into(), None, None, Type::FLOAT8),
            FieldInfo::new("strings".into(), None, None, Type::VARCHAR),
            FieldInfo::new("binaries".into(), None, None, Type::BYTEA),
            FieldInfo::new("dates".into(), None, None, Type::DATE),
            FieldInfo::new("datetimes".into(), None, None, Type::TIMESTAMP),
            FieldInfo::new("timestamps".into(), None, None, Type::TIMESTAMP),
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
        let mut builder = TextDataRowEncoder::new(schema.len());
        for i in values {
            assert!(encode_value(&i, &mut builder).is_ok());
        }

        let err = encode_value(
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
