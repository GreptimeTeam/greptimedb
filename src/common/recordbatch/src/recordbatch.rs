use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
};
use arrow::datatypes::DataType;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::schema::Schema;
use paste::paste;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    pub schema: Arc<Schema>,
    pub df_recordbatch: DfRecordBatch,
}

macro_rules! collect_columns {
    ($array: ident, $columns: ident, $($data_type: expr), +) => {
        paste! {
            match $array.data_type() {
                $(DataType::$data_type => {
                    if let Some(array) = $array.as_any().downcast_ref::<[<$data_type Array>]>() {
                        $columns.push(Column::$data_type(array.values().as_slice()));
                    }
                })+,
                DataType::Utf8 => {
                      if let Some(array) = $array.as_any().downcast_ref::<Utf8Array<i32>>() {
                          $columns.push(Column::Utf8(array.values().as_slice()));
                    }
                },
                _ => unimplemented!(),
            }
        }
    };
}

#[derive(Serialize)]
enum Column<'a> {
    Int64(&'a [i64]),
    Int32(&'a [i32]),
    Int16(&'a [i16]),
    Int8(&'a [i8]),
    UInt64(&'a [u64]),
    UInt32(&'a [u32]),
    UInt16(&'a [u16]),
    UInt8(&'a [u8]),
    Float64(&'a [f64]),
    Float32(&'a [f32]),
    Boolean((&'a [u8], usize, usize)),
    Utf8(&'a [u8]),
}

/// TODO(dennis): should be implemented in datatypes
impl Serialize for RecordBatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("record", 2)?;
        s.serialize_field("schema", &self.schema.arrow_schema())?;

        let df_columns = self.df_recordbatch.columns();
        let mut columns: Vec<Column> = Vec::with_capacity(df_columns.len());

        for array in df_columns {
            collect_columns!(
                array, columns, Int64, Int32, Int16, Int8, UInt64, UInt32, UInt16, UInt8, Float64,
                Float32, Boolean
            );
        }

        s.serialize_field("columns", &columns)?;

        s.end()
    }
}
