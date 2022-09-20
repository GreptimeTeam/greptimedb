use std::ops::Deref;

use async_trait::async_trait;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_time::timestamp::TimeUnit;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::SchemaRef;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, Response, Tag, TextQueryResponseBuilder};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};

use crate::error::{self, Error, Result};
use crate::query_handler::SqlQueryHandlerRef;

pub struct PostgresServerHandler {
    query_handler: SqlQueryHandlerRef,
}

impl PostgresServerHandler {
    pub fn new(query_handler: SqlQueryHandlerRef) -> Self {
        PostgresServerHandler { query_handler }
    }
}

#[async_trait]
impl SimpleQueryHandler for PostgresServerHandler {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let output = self
            .query_handler
            .do_query(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match output {
            Output::AffectedRows(rows) => Ok(Response::Execution(Tag::new_for_execution(
                "OK",
                Some(rows),
            ))),
            Output::Stream(record_stream) => {
                let schema = record_stream.schema();
                let recordbatches = util::collect(record_stream)
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                recordbatches_to_query_response(recordbatches.iter(), schema)
            }
            Output::RecordBatches(recordbatches) => {
                let schema = recordbatches.schema();
                recordbatches_to_query_response(recordbatches.take().iter(), schema)
            }
        }
    }
}

fn recordbatches_to_query_response<'a, I>(
    recordbatches: I,
    schema: SchemaRef,
) -> PgWireResult<Response>
where
    I: Iterator<Item = &'a RecordBatch>,
{
    let pg_schema = schema_to_pg(schema).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let mut builder = TextQueryResponseBuilder::new(pg_schema);

    for recordbatch in recordbatches {
        for row in recordbatch.rows() {
            let row = row.map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            for value in row.into_iter() {
                encode_value(&value, &mut builder)?;
            }
            builder.finish_row();
        }
    }

    Ok(Response::Query(builder.build()))
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

fn encode_value(value: &Value, builder: &mut TextQueryResponseBuilder) -> PgWireResult<()> {
    match value {
        Value::Null => builder.append_field(None::<i8>),
        Value::Boolean(v) => builder.append_field(Some(v)),
        Value::UInt8(v) => builder.append_field(Some(v)),
        Value::UInt16(v) => builder.append_field(Some(v)),
        Value::UInt32(v) => builder.append_field(Some(v)),
        Value::UInt64(v) => builder.append_field(Some(v)),
        Value::Int8(v) => builder.append_field(Some(v)),
        Value::Int16(v) => builder.append_field(Some(v)),
        Value::Int32(v) => builder.append_field(Some(v)),
        Value::Int64(v) => builder.append_field(Some(v)),
        Value::Float32(v) => builder.append_field(Some(v.0)),
        Value::Float64(v) => builder.append_field(Some(v.0)),
        Value::String(v) => builder.append_field(Some(v.as_utf8())),
        Value::Binary(v) => builder.append_field(Some(hex::encode(v.deref()))),
        Value::Date(v) => builder.append_field(Some(v.val())),
        Value::DateTime(v) => builder.append_field(Some(v.val())),
        Value::Timestamp(v) => builder.append_field(Some(v.convert_to(TimeUnit::Millisecond))),
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
            err_msg: format!("not implemented for column datatype {:?}", origin),
        }
        .fail(),
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresServerHandler {
    async fn do_query<C>(&self, _client: &mut C, _portal: &Portal) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        unimplemented!()
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
                ConcreteDataType::timestamp_millis_datatype(),
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
        let schema = Arc::new(Schema::try_new(column_schemas).unwrap());
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
        let mut builder = TextQueryResponseBuilder::new(schema);
        for i in values {
            assert!(encode_value(&i, &mut builder).is_ok());
        }

        let err = encode_value(
            &Value::List(ListValue::new(
                Some(Box::new(vec![])),
                ConcreteDataType::int8_datatype(),
            )),
            &mut builder,
        )
        .unwrap_err();
        match err {
            PgWireError::ApiError(e) => {
                assert!(format!("{}", e).contains("Internal error:"));
            }
            _ => {
                unreachable!()
            }
        }
    }
}
