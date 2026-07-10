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

use api::v1::SemanticType;
use datatypes::arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder,
};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    metric_engine_value_int_column_name,
};
use store_api::storage::consts::ReservedColumnId;

use crate::error::{InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};

#[derive(Debug, Clone)]
pub(crate) struct MetricValueSplitColumn {
    pub(crate) float_index: usize,
    pub(crate) int_index: usize,
}

pub(crate) fn metric_value_split_columns(
    metadata: &RegionMetadata,
    arrow_schema: &SchemaRef,
) -> Vec<MetricValueSplitColumn> {
    if !is_metric_engine_data_region(metadata) {
        return vec![];
    }

    metadata
        .field_columns()
        .filter(|column| column.column_schema.data_type == ConcreteDataType::float64_datatype())
        .filter_map(|float_column| {
            let int_name = metric_engine_value_int_column_name(&float_column.column_schema.name);
            let int_column = metadata.column_by_name(&int_name)?;
            if int_column.semantic_type != SemanticType::Field
                || int_column.column_schema.data_type != ConcreteDataType::int64_datatype()
            {
                return None;
            }

            let float_index = arrow_schema
                .index_of(&float_column.column_schema.name)
                .ok()?;
            let int_index = arrow_schema.index_of(&int_name).ok()?;
            Some(MetricValueSplitColumn {
                float_index,
                int_index,
            })
        })
        .collect()
}

fn is_metric_engine_data_region(metadata: &RegionMetadata) -> bool {
    let has_internal_tag = |name, column_id| {
        metadata.column_by_name(name).is_some_and(|column| {
            column.semantic_type == SemanticType::Tag
                && column.column_id == column_id
                && metadata.primary_key.contains(&column_id)
        })
    };

    has_internal_tag(
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
        ReservedColumnId::table_id(),
    ) && has_internal_tag(DATA_SCHEMA_TSID_COLUMN_NAME, ReservedColumnId::tsid())
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
    use datatypes::arrow::array::{Float64Array, Int64Array};
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

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
                ])) as ArrayRef,
                true,
            ),
            (
                "greptime_value__metric_int",
                Arc::new(Int64Array::from(vec![None, None, None, None])) as ArrayRef,
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
            vec![None, None, Some(1.5), None]
        );
        assert_eq!(
            int_values.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), None, Some(2)]
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
