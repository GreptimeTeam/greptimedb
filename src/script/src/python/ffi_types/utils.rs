use datafusion::arrow::compute;
use datafusion::arrow::datatypes::Field;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datatypes::arrow::datatypes::DataType as ArrowDataType;

pub fn new_item_field(data_type: ArrowDataType) -> Field {
    Field::new("item", data_type, false)
}

pub(crate) fn collect_diff_types_string(values: &[ScalarValue], ty: &ArrowDataType) -> String {
    values
        .iter()
        .enumerate()
        .filter_map(|(idx, val)| {
            if val.get_datatype() != *ty {
                Some((idx, val.get_datatype()))
            } else {
                None
            }
        })
        .map(|(idx, ty)| format!(" {:?} at {}th location\n", ty, idx + 1))
        .reduce(|mut acc, item| {
            acc.push_str(&item);
            acc
        })
        .unwrap_or_else(|| "Nothing".to_string())
}

/// Because most of the datafusion's UDF only support f32/64, so cast all to f64 to use datafusion's UDF
pub fn all_to_f64(col: ColumnarValue) -> Result<ColumnarValue, String> {
    match col {
        ColumnarValue::Array(arr) => {
            let res = compute::cast(&arr, &ArrowDataType::Float64).map_err(|err| {
                format!(
                    "Arrow Type Cast Fail(from {:#?} to {:#?}): {err:#?}",
                    arr.data_type(),
                    ArrowDataType::Float64
                )
            })?;
            Ok(ColumnarValue::Array(res))
        }
        ColumnarValue::Scalar(val) => {
            let val_in_f64 = match val {
                ScalarValue::Float64(Some(v)) => v,
                ScalarValue::Int64(Some(v)) => v as f64,
                ScalarValue::Boolean(Some(v)) => v as i64 as f64,
                _ => {
                    return Err(format!(
                        "Can't cast type {:#?} to {:#?}",
                        val.get_datatype(),
                        ArrowDataType::Float64
                    ))
                }
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                val_in_f64,
            ))))
        }
    }
}
