use std::ops::Deref;

use async_trait::async_trait;
use common_recordbatch::{util, RecordBatch};
use common_time::timestamp::TimeUnit;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::SchemaRef;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, Response, Tag, TextQueryResponseBuilder};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use query::Output;

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
                recordbatches_to_query_response(recordbatches.to_vec().iter(), schema)
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
        Value::List(_) => {
            return Err(PgWireError::ApiError(Box::new(Error::Internal {
                err_msg: format!(
                    "cannot write value {:?} in postgres protocol: unimplemented",
                    &value
                ),
            })));
        }
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
