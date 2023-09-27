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

use common_error::ext::{BoxedError, ErrorExt};
use common_query::Output;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::SchemaRef;
use futures::StreamExt;
use metrics::increment_counter;
use opensrv_mysql::{
    Column, ColumnFlags, ColumnType, ErrorKind, OkResponse, QueryResultWriter, RowWriter,
};
use session::context::QueryContextRef;
use snafu::prelude::*;
use tokio::io::AsyncWrite;

use crate::error::{self, Error, OtherSnafu, Result};
use crate::metrics::*;

/// Try to write multiple output to the writer if possible.
pub async fn write_output<W: AsyncWrite + Send + Sync + Unpin>(
    w: QueryResultWriter<'_, W>,
    query_context: QueryContextRef,
    outputs: Vec<Result<Output>>,
) -> Result<()> {
    let mut writer = Some(MysqlResultWriter::new(w, query_context.clone()));
    for output in outputs {
        let result_writer = writer.take().context(error::InternalSnafu {
            err_msg: "Sending multiple result set is unsupported",
        })?;
        writer = result_writer.try_write_one(output).await?;
    }

    if let Some(result_writer) = writer {
        result_writer.finish().await?;
    }
    Ok(())
}

struct QueryResult {
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
}

pub struct MysqlResultWriter<'a, W: AsyncWrite + Unpin> {
    writer: QueryResultWriter<'a, W>,
    query_context: QueryContextRef,
}

impl<'a, W: AsyncWrite + Unpin> MysqlResultWriter<'a, W> {
    pub fn new(
        writer: QueryResultWriter<'a, W>,
        query_context: QueryContextRef,
    ) -> MysqlResultWriter<'a, W> {
        MysqlResultWriter::<'a, W> {
            writer,
            query_context,
        }
    }

    /// Try to write one result set. If there are more than one result set, return `Some`.
    pub async fn try_write_one(
        self,
        output: Result<Output>,
    ) -> Result<Option<MysqlResultWriter<'a, W>>> {
        // We don't support sending multiple query result because the RowWriter's lifetime is bound to
        // a local variable.
        match output {
            Ok(output) => match output {
                Output::Stream(stream) => {
                    let query_result = QueryResult {
                        schema: stream.schema(),
                        stream,
                    };
                    Self::write_query_result(query_result, self.writer, self.query_context).await?;
                }
                Output::RecordBatches(recordbatches) => {
                    let query_result = QueryResult {
                        schema: recordbatches.schema(),
                        stream: recordbatches.as_stream(),
                    };
                    Self::write_query_result(query_result, self.writer, self.query_context).await?;
                }
                Output::AffectedRows(rows) => {
                    let next_writer = Self::write_affected_rows(self.writer, rows).await?;
                    return Ok(Some(MysqlResultWriter::new(
                        next_writer,
                        self.query_context,
                    )));
                }
            },
            Err(error) => Self::write_query_error(error, self.writer).await?,
        }
        Ok(None)
    }

    /// Indicate no more result set to write. No need to call this if there is only one result set.
    pub async fn finish(self) -> Result<()> {
        self.writer.no_more_results().await?;
        Ok(())
    }

    async fn write_affected_rows(
        w: QueryResultWriter<'a, W>,
        rows: usize,
    ) -> Result<QueryResultWriter<'a, W>> {
        let next_writer = w
            .complete_one(OkResponse {
                affected_rows: rows as u64,
                ..Default::default()
            })
            .await?;
        Ok(next_writer)
    }

    async fn write_query_result(
        mut query_result: QueryResult,
        writer: QueryResultWriter<'a, W>,
        query_context: QueryContextRef,
    ) -> Result<()> {
        match create_mysql_column_def(&query_result.schema) {
            Ok(column_def) => {
                // The RowWriter's lifetime is bound to `column_def` thus we can't use finish_one()
                // to return a new QueryResultWriter.
                let mut row_writer = writer.start(&column_def).await?;
                while let Some(record_batch) = query_result.stream.next().await {
                    match record_batch {
                        Ok(record_batch) => {
                            Self::write_recordbatch(
                                &mut row_writer,
                                &record_batch,
                                query_context.clone(),
                            )
                            .await?
                        }
                        Err(e) => {
                            return Err(e).map_err(BoxedError::new).context(OtherSnafu);
                        }
                    }
                }
                row_writer.finish().await?;
                Ok(())
            }
            Err(error) => Self::write_query_error(error, writer).await,
        }
    }

    async fn write_recordbatch(
        row_writer: &mut RowWriter<'_, W>,
        recordbatch: &RecordBatch,
        query_context: QueryContextRef,
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
                    Value::Date(v) => row_writer.write_col(v.to_chrono_date())?,
                    Value::DateTime(v) => row_writer.write_col(v.to_chrono_datetime())?,
                    Value::Timestamp(v) => row_writer
                        .write_col(v.to_timezone_aware_string(query_context.time_zone()))?,
                    Value::Interval(v) => row_writer.write_col(v.to_iso8601_string())?,
                    Value::Duration(v) => row_writer.write_col(v.to_std_duration())?,
                    Value::List(_) => {
                        return Err(Error::Internal {
                            err_msg: format!(
                                "cannot write value {:?} in mysql protocol: unimplemented",
                                &value
                            ),
                        })
                    }
                    Value::Time(v) => row_writer
                        .write_col(v.to_timezone_aware_string(query_context.time_zone()))?,
                }
            }
            row_writer.end_row().await?;
        }
        Ok(())
    }

    async fn write_query_error(error: impl ErrorExt, w: QueryResultWriter<'a, W>) -> Result<()> {
        increment_counter!(
            METRIC_ERROR_COUNTER,
            &[(METRIC_PROTOCOL_LABEL, METRIC_ERROR_COUNTER_LABEL_MYSQL)]
        );

        let kind = ErrorKind::ER_INTERNAL_ERROR;
        let error = error.output_msg();
        w.error(kind, error.as_bytes()).await?;
        Ok(())
    }
}

pub(crate) fn create_mysql_column(
    data_type: &ConcreteDataType,
    column_name: &str,
) -> Result<Column> {
    let column_type = match data_type {
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
        ConcreteDataType::Float32(_) => Ok(ColumnType::MYSQL_TYPE_FLOAT),
        ConcreteDataType::Float64(_) => Ok(ColumnType::MYSQL_TYPE_DOUBLE),
        ConcreteDataType::Binary(_) | ConcreteDataType::String(_) => {
            Ok(ColumnType::MYSQL_TYPE_VARCHAR)
        }
        ConcreteDataType::Timestamp(_) => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP),
        ConcreteDataType::Time(_) => Ok(ColumnType::MYSQL_TYPE_TIME),
        ConcreteDataType::Date(_) => Ok(ColumnType::MYSQL_TYPE_DATE),
        ConcreteDataType::DateTime(_) => Ok(ColumnType::MYSQL_TYPE_DATETIME),
        ConcreteDataType::Interval(_) => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
        ConcreteDataType::Duration(_) => Ok(ColumnType::MYSQL_TYPE_TIME),
        _ => error::InternalSnafu {
            err_msg: format!("not implemented for column datatype {:?}", data_type),
        }
        .fail(),
    };
    let mut colflags = ColumnFlags::empty();
    match data_type {
        ConcreteDataType::UInt16(_)
        | ConcreteDataType::UInt8(_)
        | ConcreteDataType::UInt32(_)
        | ConcreteDataType::UInt64(_) => colflags |= ColumnFlags::UNSIGNED_FLAG,
        _ => {}
    };
    column_type.map(|column_type| Column {
        column: column_name.to_string(),
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
        .map(|column_schema| create_mysql_column(&column_schema.data_type, &column_schema.name))
        .collect()
}
