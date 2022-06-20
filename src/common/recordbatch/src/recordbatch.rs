use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::schema::SchemaRef;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::field_util::SchemaExt;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use datatypes::arrow::array::UInt32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datatypes::schema::Schema;

    use super::*;

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

        let mut output = vec![];
        let mut serializer = serde_json::Serializer::new(&mut output);
        batch.serialize(&mut serializer).unwrap();
        assert_eq!(
            r#"{"schema":{"fields":[{"name":"number","data_type":"UInt32","is_nullable":false,"metadata":{}}],"metadata":{}},"columns":[[0,1,2,3,4,5,6,7,8,9]]}"#,
            String::from_utf8_lossy(&output)
        );
    }
}
