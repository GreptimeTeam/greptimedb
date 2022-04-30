use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::datatypes::DataType;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::schema::Schema;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

#[derive(Clone, Debug)]
pub struct RecordBatch {
    pub schema: Arc<Schema>,
    pub df_recordbatch: DfRecordBatch,
}

impl Serialize for RecordBatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("record", 2)?;
        s.serialize_field("schema", &self.schema.arrow_schema())?;

        let df_columns = self.df_recordbatch.columns();

        let mut columns: Vec<&[u32]> = Vec::with_capacity(df_columns.len());

        for array in df_columns {
            match array.data_type() {
                DataType::UInt32 => {
                    for column in array.as_any().downcast_ref::<UInt32Array>() {
                        columns.push(column.values().as_slice());
                    }
                }

                _ => unimplemented!(),
            }
        }

        s.serialize_field("columns", &columns)?;
        s.end()
    }
}
