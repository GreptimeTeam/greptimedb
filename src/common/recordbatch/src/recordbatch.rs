use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow_array::arrow_array_get;
use datatypes::schema::SchemaRef;
use datatypes::value::Value;
use datatypes::vectors::{Helper, VectorRef};
use serde::ser::{Error, SerializeStruct};
use serde::{Serialize, Serializer};
use snafu::ResultExt;

use crate::error::{self, Result};

#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    pub schema: SchemaRef,
    pub df_recordbatch: DfRecordBatch,
}

impl RecordBatch {
    pub fn new<I: IntoIterator<Item = VectorRef>>(
        schema: SchemaRef,
        columns: I,
    ) -> Result<RecordBatch> {
        let arrow_arrays = columns.into_iter().map(|v| v.to_arrow_array()).collect();

        let df_recordbatch = DfRecordBatch::try_new(schema.arrow_schema().clone(), arrow_arrays)
            .context(error::NewDfRecordBatchSnafu)?;

        Ok(RecordBatch {
            schema,
            df_recordbatch,
        })
    }

    /// Create an iterator to traverse the data by row
    pub fn rows(&self) -> RecordBatchRowIterator<'_> {
        RecordBatchRowIterator::new(self)
    }
}

impl Serialize for RecordBatch {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("record", 2)?;
        s.serialize_field("schema", &self.schema.arrow_schema())?;

        let df_columns = self.df_recordbatch.columns();

        let vec = df_columns
            .iter()
            .map(|c| Helper::try_into_vector(c.clone())?.serialize_to_json())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(S::Error::custom)?;

        s.serialize_field("columns", &vec)?;
        s.end()
    }
}

pub struct RecordBatchRowIterator<'a> {
    record_batch: &'a RecordBatch,
    rows: usize,
    columns: usize,
    row_cursor: usize,
}

impl<'a> RecordBatchRowIterator<'a> {
    fn new(record_batch: &'a RecordBatch) -> RecordBatchRowIterator {
        RecordBatchRowIterator {
            record_batch,
            rows: record_batch.df_recordbatch.num_rows(),
            columns: record_batch.df_recordbatch.num_columns(),
            row_cursor: 0,
        }
    }
}

impl<'a> Iterator for RecordBatchRowIterator<'a> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_cursor == self.rows {
            None
        } else {
            let mut row = Vec::with_capacity(self.columns);

            for col in 0..self.columns {
                let column_array = self.record_batch.df_recordbatch.column(col);
                match arrow_array_get(column_array.as_ref(), self.row_cursor)
                    .context(error::DataTypesSnafu)
                {
                    Ok(field) => row.push(field),
                    Err(e) => return Some(Err(e.into())),
                }
            }

            self.row_cursor += 1;
            Some(Ok(row))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::field_util::SchemaExt;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use datatypes::arrow::array::UInt32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{StringVector, UInt32Vector, Vector};

    use super::*;

    #[test]
    fn test_new_record_batch() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt32, false),
        ]));
        let schema = Arc::new(Schema::try_from(arrow_schema).unwrap());

        let v = Arc::new(UInt32Vector::from_slice(&[1, 2, 3]));
        let columns: Vec<VectorRef> = vec![v.clone(), v.clone()];

        let batch = RecordBatch::new(schema.clone(), columns).unwrap();
        let expect = v.to_arrow_array();
        for column in batch.df_recordbatch.columns() {
            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            assert_eq!(
                expect.as_any().downcast_ref::<UInt32Array>().unwrap(),
                array
            );
        }
        assert_eq!(schema, batch.schema);
    }

    #[test]
    pub fn test_serialize_recordbatch() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "number",
            DataType::UInt32,
            false,
        )]));
        let schema = Arc::new(Schema::try_from(arrow_schema.clone()).unwrap());

        let numbers: Vec<u32> = (0..10).collect();
        let df_batch = DfRecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(UInt32Array::from_slice(&numbers))],
        )
        .unwrap();

        let batch = RecordBatch {
            schema,
            df_recordbatch: df_batch,
        };

        let output = serde_json::to_string(&batch).unwrap();
        assert_eq!(
            r#"{"schema":{"fields":[{"name":"number","data_type":"UInt32","is_nullable":false,"metadata":{}}],"metadata":{}},"columns":[[0,1,2,3,4,5,6,7,8,9]]}"#,
            output
        );
    }

    #[test]
    fn test_record_batch_visitor() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::try_new(column_schemas).unwrap());
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

        let mut record_batch_iter = recordbatch.rows();
        assert_eq!(
            vec![Value::UInt32(1), Value::Null],
            record_batch_iter
                .next()
                .unwrap()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert_eq!(
            vec![Value::UInt32(2), Value::String("hello".into())],
            record_batch_iter
                .next()
                .unwrap()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert_eq!(
            vec![Value::UInt32(3), Value::String("greptime".into())],
            record_batch_iter
                .next()
                .unwrap()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert_eq!(
            vec![Value::UInt32(4), Value::Null],
            record_batch_iter
                .next()
                .unwrap()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert!(record_batch_iter.next().is_none());
    }
}
