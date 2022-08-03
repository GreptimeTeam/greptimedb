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
    record_batches: Vec<RecordBatch>,
) -> Result<GrpcSelectResult> {
    let mut select_results = Vec::with_capacity(record_batches.len());
    for record_batch in record_batches {
        select_results.push(convert_record_batch(&record_batch)?);
    }

    Ok(aggregate_results(select_results))
}

fn aggregate_results(results: Vec<SelectResult>) -> GrpcSelectResult {
    if results.is_empty() {
        return GrpcSelectResult::default();
    }

    let first = &results[0];
    let column_count = first.columns.len();
    let mut columns = Vec::with_capacity(column_count);

    let total_row_count: u32 = results.iter().map(|r| r.row_count).sum();

    for column_name in first.columns.keys() {
        let null_mask = aggregate_null_mask(column_name, &results, total_row_count);

        // TODO(fys): aggregate values
        let values = Some(aggregate_values(column_name, &results));

        columns.push(Column {
            column_name: column_name.to_string(),
            values,
            null_mask,
            ..Default::default()
        });
    }

    GrpcSelectResult {
        columns,
        row_count: total_row_count,
    }
}

fn aggregate_values(_column_name: &str, _results: &[SelectResult]) -> Values {
    todo!()
}

fn aggregate_null_mask(
    column_name: &str,
    results: &[SelectResult],
    total_row_count: u32,
) -> Vec<u8> {
    let mut contain_null = false;

    for result in results {
        let (_, null_count) = result.columns.get(column_name).unwrap();
        if *null_count > 0 {
            contain_null = true;
        }
    }

    if !contain_null {
        Vec::default()
    } else {
        let mut null_masks = BitSet::with_size(total_row_count as usize);
        for result in results {
            let (column, _) = result.columns.get(column_name).unwrap();
            null_masks.extend(&BitSet::from_vec(
                column.null_mask.clone(),
                result.row_count as usize,
            ));
        }
        null_masks.buffer()
    }
}

fn convert_record_batch(record_batch: &RecordBatch) -> Result<SelectResult> {
    let df_record = &record_batch.df_recordbatch;
    let row_count = df_record.num_rows();
    let column_schemas = record_batch.schema.column_schemas();
    let mut column_map = HashMap::with_capacity(df_record.num_columns());

    for (idx, column_array) in df_record.columns().iter().enumerate() {
        let null_count = column_array.null_count();

        let null_mask = if null_count == 0 {
            Vec::default()
        } else {
            column_array
                .validity()
                .map(|vailidity| {
                    let mut bit_set = BitSet::with_size(row_count);
                    vailidity.iter().enumerate().for_each(|(_, vailidity)| {
                        bit_set.append(!vailidity);
                    });
                    bit_set.buffer()
                })
                .unwrap_or_default()
        };

        let values = convert_arrow_array(column_array, column_array.data_type())?;
        let column_name = &column_schemas[idx].name;

        let column = Column {
            column_name: column_name.to_string(),
            values: Some(values),
            null_mask,
            ..Default::default()
        };
        column_map.insert(column_name.to_string(), (column, null_count as u32));
    }

    Ok(SelectResult {
        columns: column_map,
        row_count: row_count as u32,
    })
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
    use std::{collections::HashMap, sync::Arc};

    use api::v1::Column;
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

    use crate::server::grpc::select::{
        aggregate_null_mask, convert_arrow_array, convert_record_batch, SelectResult,
    };

    #[test]
    fn test_aggregate_null_mask() {
        let select_results = &mock_inner_select_result_1();

        let null_mask1 = aggregate_null_mask("c1", select_results, 7);
        let null_mask2 = aggregate_null_mask("c2", select_results, 7);

        assert_eq!(vec![0b0100_0100], null_mask1);
        assert_eq!(vec![0b0100_1100], null_mask2);

        let select_results = &mock_inner_select_result_2();

        let null_mask1 = aggregate_null_mask("c1", select_results, 7);
        let null_mask2 = aggregate_null_mask("c2", select_results, 7);

        assert!(null_mask1.is_empty());
        assert!(null_mask2.is_empty());
    }

    // values contains some "null"
    fn mock_inner_select_result_1() -> Vec<SelectResult> {
        let c1 = Column {
            column_name: "c1".to_string(),
            null_mask: vec![0b0000_0100],
            ..Default::default()
        };
        let c2 = Column {
            column_name: "c2".to_string(),
            null_mask: vec![0b0000_1100],
            ..Default::default()
        };

        let mut map = HashMap::new();
        map.insert("c1".to_string(), (c1, 1));
        map.insert("c2".to_string(), (c2, 2));

        let s1 = SelectResult {
            row_count: 4,
            columns: map.clone(),
        };
        let s2 = SelectResult {
            row_count: 3,
            columns: map,
        };

        vec![s1, s2]
    }

    // values not contains "null"
    fn mock_inner_select_result_2() -> Vec<SelectResult> {
        let c1 = Column {
            column_name: "c1".to_string(),
            null_mask: vec![0b0000_0000],
            ..Default::default()
        };
        let c2 = Column {
            column_name: "c2".to_string(),
            null_mask: vec![0b0000_0000],
            ..Default::default()
        };

        let mut map = HashMap::new();
        map.insert("c1".to_string(), (c1, 0));
        map.insert("c2".to_string(), (c2, 0));

        let s1 = SelectResult {
            row_count: 4,
            columns: map.clone(),
        };
        let s2 = SelectResult {
            row_count: 3,
            columns: map,
        };

        vec![s1, s2]
    }

    #[test]
    fn test_convert_batch() {
        let record_batch = mock_record_batch();

        let result = convert_record_batch(&record_batch).unwrap();

        assert_eq!(result.row_count, 3);

        assert!(result.columns.contains_key("c1"));
        assert!(result.columns.contains_key("c2"));

        let (c1, c1_null_count) = result.columns.get("c1").unwrap();
        let (c2, c2_null_count) = result.columns.get("c2").unwrap();

        assert_eq!(1, *c1_null_count);
        assert_eq!(2, *c2_null_count);

        assert_eq!(vec![1, 2], c1.values.as_ref().unwrap().u32_values);
        assert_eq!(vec![1], c2.values.as_ref().unwrap().u32_values);

        assert_eq!(vec![4_u8], c1.null_mask);
        assert_eq!(vec![6_u8], c2.null_mask);
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
        let array = BooleanArray::from(vec![Some(true), Some(false), None, Some(false), None]);
        let array: Arc<dyn Array> = Arc::new(array);

        let values = convert_arrow_array(&array, data_type).unwrap();

        assert_eq!(vec![true, false, false], values.bool_values);
    }
}
