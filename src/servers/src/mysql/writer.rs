use std::io;

use common_recordbatch::{util, RecordBatch};
use datatypes::prelude::{ConcreteDataType, Value, VectorHelper};
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
        let matrix = transpose(recordbatch)?;
        for row in matrix.iter() {
            for v in row.iter() {
                match v {
                    Value::Null => row_writer.write_col(None::<u8>)?,
                    Value::Boolean(v) => row_writer.write_col(*v as i8)?,
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
                    Value::Binary(v) => row_writer.write_col(v.to_vec())?,
                    Value::Date(v) => row_writer.write_col(v)?,
                    Value::DateTime(v) => row_writer.write_col(v)?,
                    _ => {
                        return Err(Error::Internal {
                            err_msg: format!(
                                "cannot write value {:?} in mysql protocol: unimplemented",
                                v
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

/// RecordBatch organizes its values in columns while MySQL needs to write row by row.
/// This function creates a view of [Value]s organized in rows from RecordBatch (just like matrix
/// transpose, hence the function name), helping us write RecordBatch to MySQL.
fn transpose(recordbatch: &RecordBatch) -> Result<Vec<Vec<Value>>> {
    let recordbatch = &recordbatch.df_recordbatch;
    let rows = recordbatch.num_rows();
    let columns = recordbatch.num_columns();
    let mut matrix = vec![vec![Value::Null; columns]; rows];
    for column in 0..columns {
        let array = recordbatch.column(column);
        let vector = VectorHelper::try_into_vector(array).context(error::VectorConversionSnafu)?;
        // Clippy suggests us to use "matrix.iter_mut().enumerate().take(rows)", which is not wanted.
        #[allow(clippy::needless_range_loop)]
        for row in 0..rows {
            matrix[row][column] = vector.get(row);
        }
    }
    Ok(matrix)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_base::bytes::StringBytes;
    use datatypes::prelude::*;
    use datatypes::schema::Schema;
    use datatypes::vectors::{StringVector, UInt32Vector};

    use super::*;

    #[test]
    fn test_transpose() {
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
        let matrix = transpose(&recordbatch).unwrap();
        assert_eq!(4, matrix.len());
        assert_eq!(vec![Value::UInt32(1), Value::Null], matrix[0]);
        assert_eq!(
            vec![Value::UInt32(2), Value::String(StringBytes::from("hello"))],
            matrix[1]
        );
        assert_eq!(
            vec![
                Value::UInt32(3),
                Value::String(StringBytes::from("greptime"))
            ],
            matrix[2]
        );
        assert_eq!(vec![Value::UInt32(4), Value::Null], matrix[3]);
    }
}
