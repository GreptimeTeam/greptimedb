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

use std::sync::Arc;

use datatypes::arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder,
};
use datatypes::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;

use crate::error::{InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};
use crate::metric_value::metric_value_columns;

#[derive(Debug, Clone)]
pub(crate) struct MetricValueSplitColumn {
    pub(crate) float_index: usize,
    pub(crate) int_index: usize,
}

pub(crate) fn metric_value_split_columns(
    metadata: &RegionMetadata,
    arrow_schema: &SchemaRef,
) -> Vec<MetricValueSplitColumn> {
    metric_value_columns(metadata)
        .into_iter()
        .filter_map(|column| {
            let float_column = &metadata.column_metadatas[column.value_index];
            let int_column = &metadata.column_metadatas[column.int_index];
            let float_index = arrow_schema
                .index_of(&float_column.column_schema.name)
                .ok()?;
            let int_index = arrow_schema.index_of(&int_column.column_schema.name).ok()?;
            Some(MetricValueSplitColumn {
                float_index,
                int_index,
            })
        })
        .collect()
}

pub(crate) fn split_metric_value_columns(
    batch: &RecordBatch,
    split_columns: &[MetricValueSplitColumn],
) -> Result<RecordBatch> {
    if split_columns.is_empty() {
        return Ok(batch.clone());
    }

    let mut columns = batch.columns().to_vec();
    for split_column in split_columns {
        let float_array = batch
            .column(split_column.float_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "expected Float64 metric value column at index {}, got {:?}",
                    split_column.float_index,
                    batch.column(split_column.float_index).data_type()
                ),
            })?;
        let int_array = batch
            .column(split_column.int_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "expected Int64 metric value column {} at index {}, got {:?}",
                    batch.schema().field(split_column.int_index).name(),
                    split_column.int_index,
                    batch.column(split_column.int_index).data_type()
                ),
            })?;

        let (float_output, int_output) = split_one_value_column(float_array, int_array);
        columns[split_column.float_index] = float_output;
        columns[split_column.int_index] = int_output;
    }

    RecordBatch::try_new(batch.schema(), columns).context(NewRecordBatchSnafu)
}

fn split_one_value_column(
    float_array: &Float64Array,
    int_array: &Int64Array,
) -> (ArrayRef, ArrayRef) {
    if int_array.null_count() == int_array.len() {
        return split_float_only_column(float_array);
    }

    let mut float_builder = Float64Builder::with_capacity(float_array.len());
    let mut int_builder = Int64Builder::with_capacity(float_array.len());

    for row in 0..float_array.len() {
        let Some(value) = logical_value(float_array, int_array, row) else {
            float_builder.append_null();
            int_builder.append_null();
            continue;
        };

        if let Some(int_value) = integer_value(value) {
            float_builder.append_null();
            int_builder.append_value(int_value);
        } else {
            float_builder.append_value(value);
            int_builder.append_null();
        }
    }

    (
        Arc::new(float_builder.finish()),
        Arc::new(int_builder.finish()),
    )
}

fn split_float_only_column(float_array: &Float64Array) -> (ArrayRef, ArrayRef) {
    let mut float_validity = Vec::with_capacity(float_array.len().div_ceil(8));
    let mut int_validity = Vec::with_capacity(float_array.len().div_ceil(8));
    let mut int_values = vec![0; float_array.len()];

    if float_array.null_count() == 0 {
        for (chunk, int_chunk) in float_array.values().chunks(8).zip(int_values.chunks_mut(8)) {
            let mut int_mask = 0u8;
            for (bit, (value, int_output)) in
                chunk.iter().copied().zip(int_chunk.iter_mut()).enumerate()
            {
                if let Some(int_value) = integer_value(value) {
                    int_mask |= 1 << bit;
                    *int_output = int_value;
                }
            }
            let valid_mask = u8::MAX >> (8 - chunk.len());
            float_validity.push(valid_mask & !int_mask);
            int_validity.push(int_mask);
        }
    } else {
        for (chunk_index, (chunk, int_chunk)) in float_array
            .values()
            .chunks(8)
            .zip(int_values.chunks_mut(8))
            .enumerate()
        {
            let mut float_mask = 0u8;
            let mut int_mask = 0u8;
            for (bit, (value, int_output)) in
                chunk.iter().copied().zip(int_chunk.iter_mut()).enumerate()
            {
                let row = chunk_index * 8 + bit;
                if float_array.is_valid(row) {
                    if let Some(int_value) = integer_value(value) {
                        int_mask |= 1 << bit;
                        *int_output = int_value;
                    } else {
                        float_mask |= 1 << bit;
                    }
                }
            }
            float_validity.push(float_mask);
            int_validity.push(int_mask);
        }
    }

    let null_buffer = |validity, len| {
        let buffer = NullBuffer::new(BooleanBuffer::new(Buffer::from(validity), 0, len));
        (buffer.null_count() > 0).then_some(buffer)
    };
    let float_output = Float64Array::new(
        float_array.values().clone(),
        null_buffer(float_validity, float_array.len()),
    );
    let int_output = Int64Array::new(
        ScalarBuffer::from(int_values),
        null_buffer(int_validity, float_array.len()),
    );

    (Arc::new(float_output), Arc::new(int_output))
}

fn logical_value(float_array: &Float64Array, int_array: &Int64Array, row: usize) -> Option<f64> {
    if !int_array.is_null(row) {
        Some(int_array.value(row) as f64)
    } else if !float_array.is_null(row) {
        Some(float_array.value(row))
    } else {
        None
    }
}

fn integer_value(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    if value < i64::MIN as f64 || value >= i64::MAX as f64 {
        return None;
    }

    let int_value = value as i64;
    ((int_value as f64) == value).then_some(int_value)
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::arrow::array::{Float64Array, Int64Array};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::metric_engine_consts::metric_engine_value_int_column_name;
    use store_api::storage::RegionId;

    use super::*;
    use crate::metric_value::is_metric_engine_data_region;

    fn column_metadata(
        column_id: u32,
        semantic_type: SemanticType,
        name: &str,
        data_type: ConcreteDataType,
    ) -> ColumnMetadata {
        ColumnMetadata {
            column_id,
            semantic_type,
            column_schema: ColumnSchema::new(name, data_type, true),
        }
    }

    #[test]
    fn test_split_requires_metric_region() {
        let value_int_name = metric_engine_value_int_column_name("value");
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(column_metadata(
                0,
                SemanticType::Timestamp,
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
            ))
            .push_column_metadata(column_metadata(
                1,
                SemanticType::Field,
                "value",
                ConcreteDataType::float64_datatype(),
            ))
            .push_column_metadata(column_metadata(
                2,
                SemanticType::Field,
                &value_int_name,
                ConcreteDataType::int64_datatype(),
            ));
        let metadata = builder.build_without_validation().unwrap();

        assert!(!is_metric_engine_data_region(&metadata));
    }

    #[test]
    fn test_split_metric_value_columns_by_value() {
        let batch = RecordBatch::try_from_iter_with_nullable([
            (
                "greptime_value",
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    Some(1.5),
                    Some(2.0),
                    None,
                    Some(3.5),
                    Some(4.0),
                    Some(5.5),
                    Some(6.0),
                ])) as ArrayRef,
                true,
            ),
            (
                "__metric_int_greptime_value",
                Arc::new(Int64Array::from(vec![None; 9])) as ArrayRef,
                true,
            ),
        ])
        .unwrap();

        let batch = split_metric_value_columns(
            &batch,
            &[MetricValueSplitColumn {
                float_index: 0,
                int_index: 1,
            }],
        )
        .unwrap();

        let float_values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let int_values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(
            float_values.iter().collect::<Vec<_>>(),
            vec![
                None,
                None,
                Some(1.5),
                None,
                None,
                Some(3.5),
                None,
                Some(5.5),
                None,
            ]
        );
        assert_eq!(
            int_values.iter().collect::<Vec<_>>(),
            vec![
                Some(1),
                Some(2),
                None,
                Some(2),
                None,
                None,
                Some(4),
                None,
                Some(6),
            ]
        );
    }

    #[test]
    fn test_split_existing_integer_values() {
        let float_values = Float64Array::from(vec![None, Some(1.5), Some(2.0)]);
        let int_values = Int64Array::from(vec![Some(1), None, Some(3)]);

        let (float_output, int_output) = split_one_value_column(&float_values, &int_values);
        let float_output = float_output
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let int_output = int_output.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(
            float_output.iter().collect::<Vec<_>>(),
            vec![None, Some(1.5), None]
        );
        assert_eq!(
            int_output.iter().collect::<Vec<_>>(),
            vec![Some(1), None, Some(3)]
        );
    }

    #[test]
    fn test_integer_value_classification() {
        assert_eq!(integer_value(1.0), Some(1));
        assert_eq!(integer_value(-1.0), Some(-1));
        assert_eq!(integer_value(-0.0), Some(0));
        assert_eq!(integer_value(i64::MIN as f64), Some(i64::MIN));

        assert_eq!(integer_value(1.5), None);
        assert_eq!(integer_value(f64::NAN), None);
        assert_eq!(integer_value(f64::INFINITY), None);
        assert_eq!(integer_value(f64::NEG_INFINITY), None);
        assert_eq!(integer_value(i64::MAX as f64), None);
    }
}
