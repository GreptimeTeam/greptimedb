// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::result::{build_err_result, ObjectResultBuilder};
use api::v1::codec::SelectResult;
use api::v1::column::{SemanticType, Values};
use api::v1::{Column, ObjectResult};
use arrow::array::{Array, BooleanArray, PrimitiveArray};
use common_base::BitVec;
use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::{util, RecordBatches, SendableRecordBatchStream};
use datatypes::arrow_array::{BinaryArray, StringArray};
use datatypes::schema::SchemaRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{self, ConversionSnafu, Result};

pub async fn to_object_result(output: std::result::Result<Output, impl ErrorExt>) -> ObjectResult {
    let result = match output {
        Ok(Output::AffectedRows(rows)) => Ok(ObjectResultBuilder::new()
            .status_code(StatusCode::Success as u32)
            .mutate_result(rows as u32, 0)
            .build()),
        Ok(Output::Stream(stream)) => collect(stream).await,
        Ok(Output::RecordBatches(recordbatches)) => build_result(recordbatches),
        Err(e) => return build_err_result(&e),
    };
    match result {
        Ok(r) => r,
        Err(e) => build_err_result(&e),
    }
}
async fn collect(stream: SendableRecordBatchStream) -> Result<ObjectResult> {
    let schema = stream.schema();

    let recordbatches = util::collect(stream)
        .await
        .and_then(|batches| RecordBatches::try_new(schema, batches))
        .context(error::CollectRecordBatchesSnafu)?;

    let object_result = build_result(recordbatches)?;
    Ok(object_result)
}

fn build_result(recordbatches: RecordBatches) -> Result<ObjectResult> {
    let select_result = try_convert(recordbatches)?;
    let object_result = ObjectResultBuilder::new()
        .status_code(StatusCode::Success as u32)
        .select_result(select_result)
        .build();
    Ok(object_result)
}

#[inline]
fn get_semantic_type(schema: &SchemaRef, idx: usize) -> i32 {
    if Some(idx) == schema.timestamp_index() {
        SemanticType::Timestamp as i32
    } else {
        // FIXME(dennis): set primary key's columns semantic type as Tag,
        // but we can't get the table's schema here right now.
        SemanticType::Field as i32
    }
}

fn try_convert(record_batches: RecordBatches) -> Result<SelectResult> {
    let schema = record_batches.schema();
    let record_batches = record_batches.take();

    let row_count: usize = record_batches
        .iter()
        .map(|r| r.df_recordbatch.num_rows())
        .sum();

    let schemas = schema.column_schemas();
    let mut columns = Vec::with_capacity(schemas.len());

    for (idx, column_schema) in schemas.iter().enumerate() {
        let column_name = column_schema.name.clone();

        let arrays: Vec<Arc<dyn Array>> = record_batches
            .iter()
            .map(|r| r.df_recordbatch.columns()[idx].clone())
            .collect();

        let column = Column {
            column_name,
            values: Some(values(&arrays)?),
            null_mask: null_mask(&arrays, row_count),
            datatype: ColumnDataTypeWrapper::try_from(column_schema.data_type.clone())
                .context(error::ColumnDataTypeSnafu)?
                .datatype() as i32,
            semantic_type: get_semantic_type(&schema, idx),
        };
        columns.push(column);
    }

    Ok(SelectResult {
        columns,
        row_count: row_count as u32,
    })
}

pub fn null_mask(arrays: &Vec<Arc<dyn Array>>, row_count: usize) -> Vec<u8> {
    let null_count: usize = arrays.iter().map(|a| a.null_count()).sum();

    if null_count == 0 {
        return Vec::default();
    }

    let mut null_mask = BitVec::with_capacity(row_count);
    for array in arrays {
        let validity = array.validity();
        if let Some(v) = validity {
            v.iter().for_each(|x| null_mask.push(!x));
        } else {
            null_mask.extend_from_bitslice(&BitVec::repeat(false, array.len()));
        }
    }
    null_mask.into_vec()
}

macro_rules! convert_arrow_array_to_grpc_vals {
    ($data_type: expr, $arrays: ident,  $(($Type: pat, $CastType: ty, $field: ident, $MapFunction: expr)), +) => {
        match $data_type {
            $(
                $Type => {
                    let mut vals = Values::default();
                    for array in $arrays {
                        let array = array.as_any().downcast_ref::<$CastType>().with_context(|| ConversionSnafu {
                            from: format!("{:?}", $data_type),
                        })?;
                        vals.$field.extend(array
                            .iter()
                            .filter_map(|i| i.map($MapFunction))
                            .collect::<Vec<_>>());
                    }
                    return Ok(vals);
                },
            )+
            _ => unimplemented!(),
        }
    };
}

pub fn values(arrays: &[Arc<dyn Array>]) -> Result<Values> {
    if arrays.is_empty() {
        return Ok(Values::default());
    }
    let data_type = arrays[0].data_type();

    use arrow::datatypes::DataType;
    convert_arrow_array_to_grpc_vals!(
        data_type, arrays,

        (DataType::Boolean,       BooleanArray,           bool_values,    |x| {x}),

        (DataType::Int8,          PrimitiveArray<i8>,     i8_values,      |x| {*x as i32}),
        (DataType::Int16,         PrimitiveArray<i16>,    i16_values,     |x| {*x as i32}),
        (DataType::Int32,         PrimitiveArray<i32>,    i32_values,     |x| {*x}),
        (DataType::Int64,         PrimitiveArray<i64>,    i64_values,     |x| {*x}),

        (DataType::UInt8,         PrimitiveArray<u8>,     u8_values,      |x| {*x as u32}),
        (DataType::UInt16,        PrimitiveArray<u16>,    u16_values,     |x| {*x as u32}),
        (DataType::UInt32,        PrimitiveArray<u32>,    u32_values,     |x| {*x}),
        (DataType::UInt64,        PrimitiveArray<u64>,    u64_values,     |x| {*x}),

        (DataType::Float32,       PrimitiveArray<f32>,    f32_values,     |x| {*x}),
        (DataType::Float64,       PrimitiveArray<f64>,    f64_values,     |x| {*x}),

        (DataType::Binary,        BinaryArray,            binary_values,  |x| {x.into()}),
        (DataType::LargeBinary,   BinaryArray,            binary_values,  |x| {x.into()}),

        (DataType::Utf8,          StringArray,            string_values,  |x| {x.into()}),
        (DataType::LargeUtf8,     StringArray,            string_values,  |x| {x.into()}),

        (DataType::Date32,        PrimitiveArray<i32>,    date_values,    |x| {*x as i32}),
        (DataType::Date64,        PrimitiveArray<i64>,    datetime_values,|x| {*x as i64}),

        (DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _), PrimitiveArray<i64>, ts_millis_values, |x| {*x})
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BooleanArray, PrimitiveArray};
    use arrow::datatypes::{DataType, Field};
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datafusion::field_util::SchemaExt;
    use datatypes::arrow::datatypes::Schema as ArrowSchema;
    use datatypes::arrow_array::StringArray;
    use datatypes::schema::Schema;
    use datatypes::vectors::{UInt32Vector, VectorRef};

    use crate::select::{null_mask, try_convert, values};

    #[test]
    fn test_convert_record_batches_to_select_result() {
        let r1 = mock_record_batch();
        let schema = r1.schema.clone();
        let r2 = mock_record_batch();
        let record_batches = vec![r1, r2];
        let record_batches = RecordBatches::try_new(schema, record_batches).unwrap();

        let s = try_convert(record_batches).unwrap();

        let c1 = s.columns.get(0).unwrap();
        let c2 = s.columns.get(1).unwrap();
        assert_eq!("c1", c1.column_name);
        assert_eq!("c2", c2.column_name);

        assert_eq!(vec![0b0010_0100], c1.null_mask);
        assert_eq!(vec![0b0011_0110], c2.null_mask);

        assert_eq!(vec![1, 2, 1, 2], c1.values.as_ref().unwrap().u32_values);
        assert_eq!(vec![1, 1], c2.values.as_ref().unwrap().u32_values);
    }

    #[test]
    fn test_convert_arrow_arrays_i32() {
        let array: PrimitiveArray<i32> =
            PrimitiveArray::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![1, 2, 3], values.i32_values);
    }

    #[test]
    fn test_convert_arrow_arrays_string() {
        let array = StringArray::from(vec![
            Some("1".to_string()),
            Some("2".to_string()),
            None,
            Some("3".to_string()),
            None,
        ]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec!["1", "2", "3"], values.string_values);
    }

    #[test]
    fn test_convert_arrow_arrays_bool() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None, Some(false), None]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![true, false, false], values.bool_values);
    }

    #[test]
    fn test_convert_arrow_arrays_empty() {
        let array = BooleanArray::from(vec![None, None, None, None, None]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(Vec::<bool>::default(), values.bool_values);
    }

    #[test]
    fn test_null_mask() {
        let a1: Arc<dyn Array> = Arc::new(PrimitiveArray::from(vec![None, Some(2), None]));
        let a2: Arc<dyn Array> =
            Arc::new(PrimitiveArray::from(vec![Some(1), Some(2), None, Some(4)]));
        let mask = null_mask(&vec![a1, a2], 3 + 4);
        assert_eq!(vec![0b0010_0101], mask);

        let empty: Arc<dyn Array> = Arc::new(PrimitiveArray::<i32>::from(vec![None, None, None]));
        let mask = null_mask(&vec![empty.clone(), empty.clone(), empty], 9);
        assert_eq!(vec![0b1111_1111, 0b0000_0001], mask);

        let a1: Arc<dyn Array> = Arc::new(PrimitiveArray::from(vec![Some(1), Some(2), Some(3)]));
        let a2: Arc<dyn Array> = Arc::new(PrimitiveArray::from(vec![Some(4), Some(5), Some(6)]));
        let mask = null_mask(&vec![a1, a2], 3 + 3);
        assert_eq!(Vec::<u8>::default(), mask);

        let a1: Arc<dyn Array> = Arc::new(PrimitiveArray::from(vec![Some(1), Some(2), Some(3)]));
        let a2: Arc<dyn Array> = Arc::new(PrimitiveArray::from(vec![Some(4), Some(5), None]));
        let mask = null_mask(&vec![a1, a2], 3 + 3);
        assert_eq!(vec![0b0010_0000], mask);
    }

    fn mock_record_batch() -> RecordBatch {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt32, false),
        ]));
        let schema = Arc::new(Schema::try_from(arrow_schema).unwrap());

        let v1 = Arc::new(UInt32Vector::from(vec![Some(1), Some(2), None]));
        let v2 = Arc::new(UInt32Vector::from(vec![Some(1), None, None]));
        let columns: Vec<VectorRef> = vec![v1, v2];

        RecordBatch::new(schema, columns).unwrap()
    }
}
