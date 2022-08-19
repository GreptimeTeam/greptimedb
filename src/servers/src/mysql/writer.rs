use std::io;

use common_recordbatch::{util, RecordBatch};
use datatypes::arrow_array::arrow_array_get;
use datatypes::prelude::{BorrowedValue, ConcreteDataType};
use datatypes::schema::{ColumnSchema, SchemaRef};
use opensrv_mysql::{
    Column, ColumnFlags, ColumnType, ErrorKind, OkResponse, QueryResultWriter, RowWriter,
};
use query::Output;
use snafu::prelude::*;

use crate::error::{self, Error, Result};

struct QueryResult {
    recordbatches: Vec<RecordBatch>,
    schema: SchemaRef,
}

pub struct MysqlResultWriter<'a, W: io::Write> {
    // `QueryResultWriter` will be consumed when the write completed (see
    // QueryResultWriter::completed), thus we use an option to wrap it.
    inner: Option<QueryResultWriter<'a, W>>,
}

impl<'a, W: io::Write> MysqlResultWriter<'a, W> {
    pub fn new(inner: QueryResultWriter<'a, W>) -> MysqlResultWriter<'a, W> {
        MysqlResultWriter::<'a, W> { inner: Some(inner) }
    }

    pub async fn write(&mut self, output: Result<Output>) -> Result<()> {
        let writer = self.inner.take().context(error::InternalSnafu {
            err_msg: "inner MySQL writer is consumed",
        })?;
        match output {
            Ok(output) => match output {
                Output::RecordBatch(stream) => {
                    let schema = stream.schema().clone();
                    let recordbatches = util::collect(stream)
                        .await
                        .context(error::CollectRecordbatchSnafu)?;
                    let query_result = QueryResult {
                        recordbatches,
                        schema,
                    };
                    Self::write_query_result(query_result, writer)?
                }
                Output::AffectedRows(rows) => Self::write_affected_rows(writer, rows)?,
            },
            Err(error) => Self::write_query_error(error, writer)?,
        }
        Ok(())
    }

    fn write_affected_rows(writer: QueryResultWriter<W>, rows: usize) -> Result<()> {
        writer.completed(OkResponse {
            affected_rows: rows as u64,
            ..Default::default()
        })?;
        Ok(())
    }

    fn write_query_result(
        query_result: QueryResult,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        if query_result.recordbatches.is_empty() {
            writer.completed(OkResponse::default())?;
            return Ok(());
        }

        match create_mysql_column_def(&query_result.schema) {
            Ok(column_def) => {
                let mut row_writer = writer.start(&column_def)?;
                for recordbatch in &query_result.recordbatches {
                    Self::write_recordbatch(&mut row_writer, recordbatch)?;
                }
                row_writer.finish()?;
                Ok(())
            }
            Err(error) => Self::write_query_error(error, writer),
        }
    }

    fn write_recordbatch(row_writer: &mut RowWriter<W>, recordbatch: &RecordBatch) -> Result<()> {
        let row_iter = RecordBatchIterator::new(recordbatch);
        for row in row_iter {
            for field in row.into_iter() {
                let value = field?;
                match value {
                    BorrowedValue::Null => row_writer.write_col(None::<u8>)?,
                    BorrowedValue::Boolean(v) => row_writer.write_col(v as i8)?,
                    BorrowedValue::UInt8(v) => row_writer.write_col(v)?,
                    BorrowedValue::UInt16(v) => row_writer.write_col(v)?,
                    BorrowedValue::UInt32(v) => row_writer.write_col(v)?,
                    BorrowedValue::UInt64(v) => row_writer.write_col(v)?,
                    BorrowedValue::Int8(v) => row_writer.write_col(v)?,
                    BorrowedValue::Int16(v) => row_writer.write_col(v)?,
                    BorrowedValue::Int32(v) => row_writer.write_col(v)?,
                    BorrowedValue::Int64(v) => row_writer.write_col(v)?,
                    BorrowedValue::Float32(v) => row_writer.write_col(v.0)?,
                    BorrowedValue::Float64(v) => row_writer.write_col(v.0)?,
                    BorrowedValue::String(v) => row_writer.write_col(v)?,
                    BorrowedValue::Binary(v) => row_writer.write_col(v)?,
                    BorrowedValue::Date(v) => row_writer.write_col(v)?,
                    BorrowedValue::DateTime(v) => row_writer.write_col(v)?,
                    BorrowedValue::List(_) => {
                        return Err(Error::Internal {
                            err_msg: format!(
                                "cannot write value {:?} in mysql protocol: unimplemented",
                                &value
                            ),
                        })
                    }
                }
            }
            row_writer.end_row()?;
        }
        Ok(())
    }

    fn write_query_error(error: Error, writer: QueryResultWriter<'a, W>) -> Result<()> {
        writer.error(ErrorKind::ER_INTERNAL_ERROR, error.to_string().as_bytes())?;
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
        _ => error::InternalSnafu {
            err_msg: format!(
                "not implemented for column datatype {:?}",
                column_schema.data_type
            ),
        }
        .fail(),
    };
    column_type.map(|column_type| Column {
        column: column_schema.name.clone(),
        coltype: column_type,

        // TODO(LFC): Currently "table" and "colflags" are not relevant in MySQL server
        //   implementation, will revisit them again in the future.
        table: "".to_string(),
        colflags: ColumnFlags::empty(),
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

struct RecordBatchIterator<'a> {
    record_batch: &'a RecordBatch,
    rows: usize,
    columns: usize,
    row_cursor: usize,
}

impl<'a> RecordBatchIterator<'a> {
    fn new(record_batch: &'a RecordBatch) -> RecordBatchIterator {
        RecordBatchIterator {
            record_batch,
            rows: record_batch.df_recordbatch.num_rows(),
            columns: record_batch.df_recordbatch.num_columns(),
            row_cursor: 0,
        }
    }
}

impl<'a> Iterator for RecordBatchIterator<'a> {
    type Item = Vec<Result<BorrowedValue<'a>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_cursor == self.rows {
            None
        } else {
            let mut row = Vec::with_capacity(self.columns);

            for col in 0..self.columns {
                let column_array = self.record_batch.df_recordbatch.column(col);
                let field = arrow_array_get(column_array.as_ref(), self.row_cursor)
                    .context(error::DataTypesSnafu);
                row.push(field);
            }

            self.row_cursor += 1;
            Some(row)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::*;
    use datatypes::schema::Schema;
    use datatypes::vectors::{StringVector, UInt32Vector};

    use super::*;

    #[test]
    fn test_record_batch_visitor() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
                None,
            ])),
        ];
        let recordbatch = RecordBatch::new(schema, columns).unwrap();

        let mut record_batch_iter = RecordBatchIterator::new(&recordbatch);
        assert_eq!(
            vec![BorrowedValue::UInt32(1), BorrowedValue::Null],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<BorrowedValue>>()
        );

        assert_eq!(
            vec![BorrowedValue::UInt32(2), BorrowedValue::String("hello")],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<BorrowedValue>>()
        );

        assert_eq!(
            vec![BorrowedValue::UInt32(3), BorrowedValue::String("greptime")],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<BorrowedValue>>()
        );

        assert_eq!(
            vec![BorrowedValue::UInt32(4), BorrowedValue::Null],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<BorrowedValue>>()
        );

        assert!(record_batch_iter.next().is_none());
    }
}
