use std::ops::Deref;

use async_trait::async_trait;
use common_recordbatch::util;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::SchemaRef;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, QueryResponseBuilder, Response, Tag};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use query::Output;

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
            Output::RecordBatch(record_stream) => {
                let schema = record_stream.schema();
                let mut builder = QueryResponseBuilder::new(schema_to_pg(schema));
                let recordbatches = util::collect(record_stream)
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

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
        }
    }
}

fn schema_to_pg(origin: SchemaRef) -> Vec<FieldInfo> {
    origin
        .column_schemas()
        .iter()
        .map(|col| FieldInfo::new(col.name.clone(), None, None, type_translate(&col.data_type)))
        .collect::<Vec<FieldInfo>>()
}

fn encode_value(value: &Value, builder: &mut QueryResponseBuilder) -> PgWireResult<()> {
    match value {
        Value::Null => builder.append_field(None::<i8>),
        Value::Boolean(v) => builder.append_field(v),
        Value::UInt8(v) => builder.append_field(*v as i8),
        Value::UInt16(v) => builder.append_field(*v as i16),
        Value::UInt32(v) => builder.append_field(*v as i32),
        Value::UInt64(v) => builder.append_field(*v as i64),
        Value::Int8(v) => builder.append_field(v),
        Value::Int16(v) => builder.append_field(v),
        Value::Int32(v) => builder.append_field(v),
        Value::Int64(v) => builder.append_field(v),
        Value::Float32(v) => builder.append_field(v.0),
        Value::Float64(v) => builder.append_field(v.0),
        Value::String(v) => builder.append_field(v.as_utf8()),
        Value::Binary(v) => builder.append_field(v.deref()),
        Value::Date(v) => builder.append_field(v.val()),
        Value::DateTime(v) => builder.append_field(v.val()),
        Value::List(_) => {
            unimplemented!("List is not supported for now")
        }
    }
}

fn type_translate(origin: &ConcreteDataType) -> Type {
    match origin {
        &ConcreteDataType::Null(_) => Type::UNKNOWN,
        &ConcreteDataType::Boolean(_) => Type::BOOL,
        &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => Type::CHAR,
        &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => Type::INT2,
        &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => Type::INT4,
        &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => Type::INT8,
        &ConcreteDataType::Float32(_) => Type::FLOAT4,
        &ConcreteDataType::Float64(_) => Type::FLOAT8,
        &ConcreteDataType::Binary(_) => Type::BYTEA,
        &ConcreteDataType::String(_) => Type::VARCHAR,
        &ConcreteDataType::Date(_) => Type::DATE,
        &ConcreteDataType::DateTime(_) => Type::TIMESTAMP,
        _ => unimplemented!("Unsupported greptime type"),
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
