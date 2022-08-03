use std::collections::HashMap;
use std::sync::Arc;

use api::v1::column::Values;
use api::v1::{Column, ObjectResult, ResultHeader, SelectResult as GrpcSelectResult};
use arrow::array::{Array, BooleanArray, PrimitiveArray};
use arrow::datatypes::DataType;
use common_recordbatch::{util, RecordBatch};
use datatypes::arrow_array::{BinaryArray, StringArray};
use query::Output;
use snafu::OptionExt;

use crate::error::{ConversionSnafu, Result};
use crate::server::grpc::{
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
                    match convert_record_batches_to_select_result(record_batches) {
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

pub(crate) fn convert_record_batches_to_select_result(
    _record_batches: Vec<RecordBatch>,
) -> Result<GrpcSelectResult> {
    todo!()
}

pub type ColumnName = String;
pub type NullCount = u32;

#[allow(dead_code)]
pub struct SelectResult {
    row_count: u32,
    columns: HashMap<ColumnName, (Column, NullCount)>,
}

macro_rules! convert_arrow_array_to_grpc_vals {
    ($data_type: expr, $array: ident,  $(($Type: ident, $CastType: ty, $field: ident, $MapFunction: expr)), +) => {
        match $data_type {
            $(
                arrow::datatypes::DataType::$Type => {
                    let mut vals = Values::default();
                    let array = $array.as_any().downcast_ref::<$CastType>().with_context(|| ConversionSnafu {
                        from: format!("{:?}", $data_type),
                    })?;
                    vals.$field = array
                        .iter()
                        .filter_map(|i| i.map($MapFunction))
                        .collect::<Vec<_>>();
                    return Ok(vals);
                },
            )+
            _ => unimplemented!(),
        }

    };
}

#[allow(dead_code)]
fn convert_arrow_array(array: &Arc<dyn Array>, data_type: &DataType) -> Result<Values> {
    convert_arrow_array_to_grpc_vals!(
        data_type, array,

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

    use arrow::{array::{PrimitiveArray, Array, BooleanArray}, datatypes::DataType};
    use datatypes::arrow_array::StringArray;

    use crate::server::grpc::select::convert_arrow_array;

    #[test]
    fn test_convert_arrow_array_i32() {
        let data_type = &DataType::Int32;
        let array: PrimitiveArray<i32> =
            PrimitiveArray::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = convert_arrow_array(&array, data_type).unwrap();

        assert_eq!(vec![1, 2, 3], values.i32_values);
    }

    #[test]
    fn test_convert_arrow_array_string() {
        let data_type = &DataType::Utf8;
        let array = StringArray::from(vec![
            Some("1".to_string()),
            Some("2".to_string()),
            None,
            Some("3".to_string()),
            None,
        ]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = convert_arrow_array(&array, data_type).unwrap();

        assert_eq!(vec!["1", "2", "3"], values.string_values);
    }

    #[test]
    fn test_convert_arrow_array_bool() {
        let data_type = &DataType::Boolean;
        let array = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            None,
        ]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = convert_arrow_array(&array, data_type).unwrap();

        assert_eq!(vec![true, false, false], values.bool_values);
    }
}