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

use std::collections::HashMap;
use std::slice;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datatypes::prelude::DataType;
use datatypes::schema::SchemaRef;
use datatypes::value::Value;
use datatypes::vectors::{Helper, VectorRef};
use serde::ser::{Error, SerializeStruct};
use serde::{Serialize, Serializer};
use snafu::{OptionExt, ResultExt};

use crate::error::{
    self, CastVectorSnafu, ColumnNotExistsSnafu, DataTypesSnafu, ProjectArrowRecordBatchSnafu,
    Result,
};
use crate::DfRecordBatch;

/// A two-dimensional batch of column-oriented data with a defined schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    pub schema: SchemaRef,
    columns: Vec<VectorRef>,
    df_record_batch: DfRecordBatch,
}

impl RecordBatch {
    /// Create a new [`RecordBatch`] from `schema` and `columns`.
    pub fn new<I: IntoIterator<Item = VectorRef>>(
        schema: SchemaRef,
        columns: I,
    ) -> Result<RecordBatch> {
        let columns: Vec<_> = columns.into_iter().collect();
        let arrow_arrays = columns.iter().map(|v| v.to_arrow_array()).collect();

        let df_record_batch = DfRecordBatch::try_new(schema.arrow_schema().clone(), arrow_arrays)
            .context(error::NewDfRecordBatchSnafu)?;

        Ok(RecordBatch {
            schema,
            columns,
            df_record_batch,
        })
    }

    /// Create an empty [`RecordBatch`] from `schema`.
    pub fn new_empty(schema: SchemaRef) -> RecordBatch {
        let df_record_batch = DfRecordBatch::new_empty(schema.arrow_schema().clone());
        let columns = schema
            .column_schemas()
            .iter()
            .map(|col| col.data_type.create_mutable_vector(0).to_vector())
            .collect();
        RecordBatch {
            schema,
            columns,
            df_record_batch,
        }
    }

    pub fn try_project(&self, indices: &[usize]) -> Result<Self> {
        let schema = Arc::new(self.schema.try_project(indices).context(DataTypesSnafu)?);
        let mut columns = Vec::with_capacity(indices.len());
        for index in indices {
            columns.push(self.columns[*index].clone());
        }
        let df_record_batch = self.df_record_batch.project(indices).with_context(|_| {
            ProjectArrowRecordBatchSnafu {
                schema: self.schema.clone(),
                projection: indices.to_vec(),
            }
        })?;

        Ok(Self {
            schema,
            columns,
            df_record_batch,
        })
    }

    /// Create a new [`RecordBatch`] from `schema` and `df_record_batch`.
    ///
    /// This method doesn't check the schema.
    pub fn try_from_df_record_batch(
        schema: SchemaRef,
        df_record_batch: DfRecordBatch,
    ) -> Result<RecordBatch> {
        let columns = df_record_batch
            .columns()
            .iter()
            .map(|c| Helper::try_into_vector(c.clone()).context(error::DataTypesSnafu))
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch {
            schema,
            columns,
            df_record_batch,
        })
    }

    #[inline]
    pub fn df_record_batch(&self) -> &DfRecordBatch {
        &self.df_record_batch
    }

    #[inline]
    pub fn into_df_record_batch(self) -> DfRecordBatch {
        self.df_record_batch
    }

    #[inline]
    pub fn columns(&self) -> &[VectorRef] {
        &self.columns
    }

    #[inline]
    pub fn column(&self, idx: usize) -> &VectorRef {
        &self.columns[idx]
    }

    pub fn column_by_name(&self, name: &str) -> Option<&VectorRef> {
        let idx = self.schema.column_index_by_name(name)?;
        Some(&self.columns[idx])
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.df_record_batch.num_rows()
    }

    /// Create an iterator to traverse the data by row
    pub fn rows(&self) -> RecordBatchRowIterator<'_> {
        RecordBatchRowIterator::new(self)
    }

    pub fn column_vectors(
        &self,
        table_name: &str,
        table_schema: SchemaRef,
    ) -> Result<HashMap<String, VectorRef>> {
        let mut vectors = HashMap::with_capacity(self.num_columns());

        // column schemas in recordbatch must match its vectors, otherwise it's corrupted
        for (vector_schema, vector) in self.schema.column_schemas().iter().zip(self.columns.iter())
        {
            let column_name = &vector_schema.name;
            let column_schema =
                table_schema
                    .column_schema_by_name(column_name)
                    .context(ColumnNotExistsSnafu {
                        table_name,
                        column_name,
                    })?;
            let vector = if vector_schema.data_type != column_schema.data_type {
                vector
                    .cast(&column_schema.data_type)
                    .with_context(|_| CastVectorSnafu {
                        from_type: vector.data_type(),
                        to_type: column_schema.data_type.clone(),
                    })?
            } else {
                vector.clone()
            };

            let _ = vectors.insert(column_name.clone(), vector);
        }

        Ok(vectors)
    }

    /// Pretty display this record batch like a table
    pub fn pretty_print(&self) -> String {
        pretty_format_batches(slice::from_ref(&self.df_record_batch))
            .map(|t| t.to_string())
            .unwrap_or("failed to pretty display a record batch".to_string())
    }

    /// Return a slice record batch starts from offset to len
    pub fn slice(&self, offset: usize, len: usize) -> Result<RecordBatch> {
        let columns = self.columns.iter().map(|vector| vector.slice(offset, len));
        RecordBatch::new(self.schema.clone(), columns)
    }
}

impl Serialize for RecordBatch {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TODO(yingwen): arrow and arrow2's schemas have different fields, so
        // it might be better to use our `RawSchema` as serialized field.
        let mut s = serializer.serialize_struct("record", 2)?;
        s.serialize_field("schema", &**self.schema.arrow_schema())?;

        let vec = self
            .columns
            .iter()
            .map(|c| c.serialize_to_json())
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
    fn new(record_batch: &'a RecordBatch) -> RecordBatchRowIterator<'a> {
        RecordBatchRowIterator {
            record_batch,
            rows: record_batch.df_record_batch.num_rows(),
            columns: record_batch.df_record_batch.num_columns(),
            row_cursor: 0,
        }
    }
}

impl Iterator for RecordBatchRowIterator<'_> {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_cursor == self.rows {
            None
        } else {
            let mut row = Vec::with_capacity(self.columns);

            for col in 0..self.columns {
                let column = self.record_batch.column(col);
                row.push(column.get(self.row_cursor));
            }

            self.row_cursor += 1;
            Some(row)
        }
    }
}

/// merge multiple recordbatch into a single
pub fn merge_record_batches(schema: SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
    let batches_len = batches.len();
    if batches_len == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let n_rows = batches.iter().map(|b| b.num_rows()).sum();
    let n_columns = schema.num_columns();
    // Collect arrays from each batch
    let mut merged_columns = Vec::with_capacity(n_columns);

    for col_idx in 0..n_columns {
        let mut acc = schema.column_schemas()[col_idx]
            .data_type
            .create_mutable_vector(n_rows);

        for batch in batches {
            let column = batch.column(col_idx);
            acc.extend_slice_of(column.as_ref(), 0, column.len())
                .context(error::DataTypesSnafu)?;
        }

        merged_columns.push(acc.to_vector());
    }

    // Create a new RecordBatch with merged columns
    RecordBatch::new(schema, merged_columns)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{StringVector, UInt32Vector};

    use super::*;

    #[test]
    fn test_record_batch() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt32, false),
        ]));
        let schema = Arc::new(Schema::try_from(arrow_schema).unwrap());

        let c1 = Arc::new(UInt32Vector::from_slice([1, 2, 3]));
        let c2 = Arc::new(UInt32Vector::from_slice([4, 5, 6]));
        let columns: Vec<VectorRef> = vec![c1, c2];

        let batch = RecordBatch::new(schema.clone(), columns.clone()).unwrap();
        assert_eq!(3, batch.num_rows());
        assert_eq!(&columns, batch.columns());
        for (i, expect) in columns.iter().enumerate().take(batch.num_columns()) {
            let column = batch.column(i);
            assert_eq!(expect, column);
        }
        assert_eq!(schema, batch.schema);

        assert_eq!(columns[0], *batch.column_by_name("c1").unwrap());
        assert_eq!(columns[1], *batch.column_by_name("c2").unwrap());
        assert!(batch.column_by_name("c3").is_none());

        let converted =
            RecordBatch::try_from_df_record_batch(schema, batch.df_record_batch().clone()).unwrap();
        assert_eq!(batch, converted);
        assert_eq!(*batch.df_record_batch(), converted.into_df_record_batch());
    }

    #[test]
    pub fn test_serialize_recordbatch() {
        let column_schemas = vec![ColumnSchema::new(
            "number",
            ConcreteDataType::uint32_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::try_new(column_schemas).unwrap());

        let numbers: Vec<u32> = (0..10).collect();
        let columns = vec![Arc::new(UInt32Vector::from_slice(numbers)) as VectorRef];
        let batch = RecordBatch::new(schema, columns).unwrap();

        let output = serde_json::to_string(&batch).unwrap();
        assert_eq!(
            r#"{"schema":{"fields":[{"name":"number","data_type":"UInt32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{"greptime:version":"0"}},"columns":[[0,1,2,3,4,5,6,7,8,9]]}"#,
            output
        );
    }

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

        let mut record_batch_iter = recordbatch.rows();
        assert_eq!(
            vec![Value::UInt32(1), Value::Null],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert_eq!(
            vec![Value::UInt32(2), Value::String("hello".into())],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert_eq!(
            vec![Value::UInt32(3), Value::String("greptime".into())],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert_eq!(
            vec![Value::UInt32(4), Value::Null],
            record_batch_iter
                .next()
                .unwrap()
                .into_iter()
                .collect::<Vec<Value>>()
        );

        assert!(record_batch_iter.next().is_none());
    }
}
