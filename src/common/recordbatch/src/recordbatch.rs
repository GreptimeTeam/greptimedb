use std::sync::Arc;

use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::schema::Schema;
use datatypes::vectors::Helper;
use serde::ser::{Error, SerializeStruct};
use serde::{Serialize, Serializer};

#[derive(Clone, Debug, PartialEq)]
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

        let vec = df_columns
            .iter()
            .map(|c| Helper::try_into_vector(c.clone())?.serialize_to_json())
            .collect::<Result<Vec<_>, _>>()
            .map_err(S::Error::custom)?;

        s.serialize_field("columns", &vec)?;
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion_common::field_util::SchemaExt;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;

    use super::*;

    #[test]
    pub fn test_serialize_recordbatch() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "number",
            DataType::UInt32,
            false,
        )]));
        let schema = Arc::new(Schema::new(arrow_schema.clone()));

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
