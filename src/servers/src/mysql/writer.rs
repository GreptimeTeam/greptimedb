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

use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_telemetry::error;
use common_time::datetime::DateTime;
use common_time::timestamp::TimeUnit;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::{ColumnSchema, SchemaRef};
use opensrv_mysql::{
    Column, ColumnFlags, ColumnType, ErrorKind, OkResponse, QueryResultWriter, RowWriter,
};
use snafu::prelude::*;
use tokio::io::AsyncWrite;

use crate::error::{self, Error, Result};

struct QueryResult {
    recordbatches: Vec<RecordBatch>,
    schema: SchemaRef,
}

pub struct MysqlResultWriter<'a, W: AsyncWrite + Unpin> {
    // `QueryResultWriter` will be consumed when the write completed (see
    // QueryResultWriter::completed), thus we use an option to wrap it.
    inner: Option<QueryResultWriter<'a, W>>,
}

impl<'a, W: AsyncWrite + Unpin> MysqlResultWriter<'a, W> {
    pub fn new(inner: QueryResultWriter<'a, W>) -> MysqlResultWriter<'a, W> {
        MysqlResultWriter::<'a, W> { inner: Some(inner) }
    }

    pub async fn write(&mut self, query: &str, output: Result<Output>) -> Result<()> {
        let writer = self.inner.take().context(error::InternalSnafu {
            err_msg: "inner MySQL writer is consumed",
        })?;
        match output {
            Ok(output) => match output {
                Output::Stream(stream) => {
                    let schema = stream.schema().clone();
                    let recordbatches = util::collect(stream)
                        .await
                        .context(error::CollectRecordbatchSnafu)?;
                    let query_result = QueryResult {
                        recordbatches,
                        schema,
                    };
                    Self::write_query_result(query, query_result, writer).await?
                }
                Output::RecordBatches(recordbatches) => {
                    let query_result = QueryResult {
                        schema: recordbatches.schema(),
                        recordbatches: recordbatches.take(),
                    };
                    Self::write_query_result(query, query_result, writer).await?
                }
                Output::AffectedRows(rows) => Self::write_affected_rows(writer, rows).await?,
            },
            Err(error) => Self::write_query_error(query, error, writer).await?,
        }
        Ok(())
    }

    async fn write_affected_rows(w: QueryResultWriter<'a, W>, rows: usize) -> Result<()> {
        w.completed(OkResponse {
            affected_rows: rows as u64,
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    async fn write_query_result(
        query: &str,
        query_result: QueryResult,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        match create_mysql_column_def(&query_result.schema) {
            Ok(column_def) => {
                let mut row_writer = writer.start(&column_def).await?;
                for recordbatch in &query_result.recordbatches {
                    Self::write_recordbatch(&mut row_writer, recordbatch).await?;
                }
                row_writer.finish().await?;
                Ok(())
            }
            Err(error) => Self::write_query_error(query, error, writer).await,
        }
    }

    async fn write_recordbatch(
        row_writer: &mut RowWriter<'_, W>,
        recordbatch: &RecordBatch,
    ) -> Result<()> {
        for row in recordbatch.rows() {
            for value in row.into_iter() {
                match value {
                    Value::Null => row_writer.write_col(None::<u8>)?,
                    Value::Boolean(v) => row_writer.write_col(v as i8)?,
                    Value::UInt8(v) => row_writer.write_col(v)?,
                    Value::UInt16(v) => row_writer.write_col(v)?,
                    Value::UInt32(v) => row_writer.write_col(v)?,
                    Value::UInt64(v) => row_writer.write_col(v)?,
                    Value::Int8(v) => row_writer.write_col(v)?,
                    Value::Int16(v) => row_writer.write_col(v)?,
                    Value::Int32(v) => row_writer.write_col(v)?,
                    Value::Int64(v) => row_writer.write_col(v)?,
                    Value::Float32(v) => row_writer.write_col(v.0)?,
                    Value::Float64(v) => row_writer.write_col(v.0)?,
                    Value::String(v) => row_writer.write_col(v.as_utf8())?,
                    Value::Binary(v) => row_writer.write_col(v.deref())?,
                    Value::Date(v) => row_writer.write_col(v.val())?,
                    Value::DateTime(v) => row_writer.write_col(v.val())?,
                    Value::Timestamp(v) => row_writer.write_col(
                        // safety: converting timestamp with whatever unit to second will not cause overflow
                        DateTime::new(v.convert_to(TimeUnit::Second).unwrap().value()).to_string(),
                    )?,
                    Value::List(_) => {
                        return Err(Error::Internal {
                            err_msg: format!(
                                "cannot write value {:?} in mysql protocol: unimplemented",
                                &value
                            ),
                        })
                    }
                }
            }
            row_writer.end_row().await?;
        }
        Ok(())
    }

    async fn write_query_error(
        query: &str,
        error: Error,
        w: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        error!(error; "Failed to execute query '{}'", query);

        let kind = ErrorKind::ER_INTERNAL_ERROR;
        w.error(kind, error.to_string().as_bytes()).await?;
        Ok(())
    }
}

fn create_mysql_column(column_schema: &ColumnSchema) -> Result<Column> {
    let column_type = match column_schema.data_type {
        ConcreteDataType::Null(_) => Ok(ColumnType::MYSQL_TYPE_NULL),
        ConcreteDataType::Boolean(_) | ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => {
            Ok(ColumnType::MYSQL_TYPE_TINY)
        }
        ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => {
            Ok(ColumnType::MYSQL_TYPE_SHORT)
        }
        ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => Ok(ColumnType::MYSQL_TYPE_LONG),
        ConcreteDataType::Int64(_) | ConcreteDataType::UInt64(_) => {
            Ok(ColumnType::MYSQL_TYPE_LONGLONG)
        }
        ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_) => {
            Ok(ColumnType::MYSQL_TYPE_FLOAT)
        }
        ConcreteDataType::Binary(_) | ConcreteDataType::String(_) => {
            Ok(ColumnType::MYSQL_TYPE_VARCHAR)
        }
        ConcreteDataType::Timestamp(_) => Ok(ColumnType::MYSQL_TYPE_DATETIME),
        _ => error::InternalSnafu {
            err_msg: format!(
                "not implemented for column datatype {:?}",
                column_schema.data_type
            ),
        }
        .fail(),
    };
    let mut colflags = ColumnFlags::empty();
    match column_schema.data_type {
        ConcreteDataType::UInt16(_)
        | ConcreteDataType::UInt8(_)
        | ConcreteDataType::UInt32(_)
        | ConcreteDataType::UInt64(_) => colflags |= ColumnFlags::UNSIGNED_FLAG,
        _ => {}
    };
    column_type.map(|column_type| Column {
        column: column_schema.name.clone(),
        coltype: column_type,

        // TODO(LFC): Currently "table" and "colflags" are not relevant in MySQL server
        //   implementation, will revisit them again in the future.
        table: "".to_string(),
        colflags,
    })
}

/// Creates MySQL columns definition from our column schema.
pub fn create_mysql_column_def(schema: &SchemaRef) -> Result<Vec<Column>> {
    schema
        .column_schemas()
        .iter()
        .map(create_mysql_column)
        .collect()
}
