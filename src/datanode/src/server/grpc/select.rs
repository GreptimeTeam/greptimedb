use std::sync::Arc;

use api::v1::{column::Values, Column, SelectResult};
use api::v1::{ObjectResult, ResultHeader};
use arrow::array::{Array, BooleanArray, PrimitiveArray};
use common_recordbatch::{util, RecordBatch};
use datatypes::arrow_array::{BinaryArray, StringArray};
use query::Output;
use snafu::OptionExt;

use crate::error::{ConversionSnafu, Result};
use crate::server::grpc::{
    bitset::BitSet,
    handler::{ERROR, SUCCESS},
    server::PROTOCOL_VERSION,
};

pub(crate) async fn select_result(select_result: Result<Output>) -> ObjectResult {
    let mut object_resp = ObjectResult::default();

    let mut header = ResultHeader {
        version: PROTOCOL_VERSION,
        ..Default::default()
    };

    match select_result {
        Ok(output) => match output {
            Output::AffectedRows(rows) => {
                header.code = SUCCESS;
                header.success = rows as u32;
            }
            Output::RecordBatch(stream) => match util::collect(stream).await {
                Ok(record_batches) => {
                    let select_result = convert_record_batches_to_select_result(record_batches);
                    match select_result {
                        Ok(select_result) => {
                            header.code = SUCCESS;
                            object_resp.results = select_result.into();
                        }
                        Err(err) => {
                            header.code = ERROR;
                            header.err_msg = err.to_string();
                        }
                    }
                }
                Err(err) => {
                    header.code = ERROR;
                    header.err_msg = err.to_string();
                }
            },
        },
        Err(err) => {
            header.code = ERROR;
            header.err_msg = err.to_string();
        }
    }
    object_resp.header = Some(header);
    object_resp
}

fn convert_record_batches_to_select_result(
    record_batches: Vec<RecordBatch>,
) -> Result<SelectResult> {
    let first = if let Some(r) = record_batches.get(0) {
        r
    } else {
        return Ok(SelectResult::default());
    };

    let row_count: usize = record_batches
        .iter()
        .map(|r| r.df_recordbatch.num_rows())
        .sum();

    let schemas = first.schema.column_schemas();
    let mut columns = Vec::with_capacity(schemas.len());

    for (idx, schema) in schemas.iter().enumerate() {
        let column_name = schema.name.clone();

        let arrays: Vec<Arc<dyn Array>> = record_batches
            .iter()
            .map(|r| r.df_recordbatch.columns()[idx].clone())
            .collect();

        let column = Column {
            column_name,
            values: Some(values(&arrays)?),
            null_mask: null_mask(&arrays, row_count),
            ..Default::default()
        };
        columns.push(column);
    }

    Ok(SelectResult {
        columns,
        row_count: row_count as u32,
    })
}

fn null_mask(arrays: &Vec<Arc<dyn Array>>, row_count: usize) -> Vec<u8> {
    if arrays.is_empty() {
        return Vec::default();
    }

    let null_count: usize = arrays.iter().map(|a| a.null_count()).sum();

    if null_count == 0 {
        return Vec::default();
    }

    let mut bitset = BitSet::with_capacity(row_count);
    for array in arrays {
        let validity = array.validity();
        if let Some(v) = validity {
            let is_sets: Vec<bool> = v.iter().map(|x| !x).collect();
            bitset.append(&is_sets);
        } else {
            bitset.append(&vec![false; array.len()]);
        }
    }

    bitset.buffer()
}

macro_rules! convert_arrow_array_to_grpc_vals {
    ($data_type: expr, $arrays: ident,  $(($Type: ident, $CastType: ty, $field: ident, $MapFunction: expr)), +) => {
        match $data_type {
            $(
                arrow::datatypes::DataType::$Type => {
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

fn values(arrays: &[Arc<dyn Array>]) -> Result<Values> {
    if arrays.is_empty() {
        return Ok(Values::default());
    }
    let data_type = arrays[0].data_type();

    convert_arrow_array_to_grpc_vals!(
        data_type, arrays,

        (Boolean,   BooleanArray,           bool_values,    |x| {x}),

        (Int8,      PrimitiveArray<i8>,     i8_values,      |x| {*x as i32}),
        (Int16,     PrimitiveArray<i16>,    i16_values,     |x| {*x as i32}),
        (Int32,     PrimitiveArray<i32>,    i32_values,     |x| {*x}),
        (Int64,     PrimitiveArray<i64>,    i64_values,     |x| {*x}),

        (UInt8,     PrimitiveArray<u8>,     u8_values,      |x| {*x as u32}),
        (UInt16,    PrimitiveArray<u16>,    u16_values,     |x| {*x as u32}),
        (UInt32,    PrimitiveArray<u32>,    u32_values,     |x| {*x}),
        (UInt64,    PrimitiveArray<u64>,    u64_values,     |x| {*x}),

        (Float32,   PrimitiveArray<f32>,    f32_values,     |x| {*x}),
        (Float64,   PrimitiveArray<f64>,    f64_values,     |x| {*x}),

        (Binary,    BinaryArray,            binary_values,  |x| {x.into()}),
        (LargeBinary, BinaryArray,          binary_values,  |x| {x.into()}),

        (Utf8,      StringArray,            string_values,  |x| {x.into()}),
        (LargeUtf8, StringArray,            string_values,  |x| {x.into()})
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, BooleanArray, PrimitiveArray},
        datatypes::{DataType, Field},
    };
    use common_recordbatch::RecordBatch;
    use datafusion::field_util::SchemaExt;
    use datatypes::arrow::datatypes::Schema as ArrowSchema;
    use datatypes::{
        arrow_array::StringArray,
        schema::Schema,
        vectors::{UInt32Vector, VectorRef},
    };

    use crate::server::grpc::select::{convert_record_batches_to_select_result, null_mask, values};

    #[test]
    fn test_convert_record_batches_to_select_result() {
        let r1 = mock_record_batch();
        let r2 = mock_record_batch();
        let record_batches = vec![r1, r2];

        let s = convert_record_batches_to_select_result(record_batches).unwrap();

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
