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

mod absent;
mod empty_metric;
mod histogram_fold;
mod instant_manipulate;
mod normalize;
mod planner;
mod range_manipulate;
mod scalar_calculate;
mod series_divide;
#[cfg(test)]
mod test_util;
mod union_distinct_on;

pub use absent::{Absent, AbsentExec, AbsentStream};
use common_query::native_histogram::{SUM_FIELD, native_histogram_value_type};
use common_query::prometheus::is_prometheus_stale_nan;
use datafusion::arrow::array::{Array, Float64Array, StructArray};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, TimestampMillisecondType};
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datatypes::data_type::DataType as _;
pub use empty_metric::{EmptyMetric, EmptyMetricExec, EmptyMetricStream, build_special_time_expr};
pub use histogram_fold::{HistogramFold, HistogramFoldExec, HistogramFoldStream};
pub use instant_manipulate::{InstantManipulate, InstantManipulateExec, InstantManipulateStream};
pub use normalize::{SeriesNormalize, SeriesNormalizeExec, SeriesNormalizeStream};
pub use planner::PromExtensionPlanner;
pub use range_manipulate::{RangeManipulate, RangeManipulateExec, RangeManipulateStream};
pub use scalar_calculate::ScalarCalculate;
pub use series_divide::{SeriesDivide, SeriesDivideExec, SeriesDivideStream};
pub use union_distinct_on::{UnionDistinctOn, UnionDistinctOnExec, UnionDistinctOnStream};

pub type Millisecond = <TimestampMillisecondType as ArrowPrimitiveType>::Native;

const METRIC_NUM_SERIES: &str = "num_series";

fn prometheus_stale_sample_column(column: &dyn Array) -> Option<(&dyn Array, &Float64Array)> {
    let values = if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
        values
    } else {
        let histograms = column.as_any().downcast_ref::<StructArray>()?;
        if histograms.data_type() != &native_histogram_value_type().as_arrow_type() {
            return None;
        }
        histograms
            .column_by_name(SUM_FIELD)?
            .as_any()
            .downcast_ref::<Float64Array>()?
    };
    Some((column, values))
}

fn is_prometheus_stale_sample((column, values): (&dyn Array, &Float64Array), row: usize) -> bool {
    column.is_valid(row) && values.is_valid(row) && is_prometheus_stale_nan(values.value(row))
}

/// Utilities for handling unfix logic in extension plans
/// Convert column name to index for serialization
pub fn serialize_column_index(schema: &DFSchemaRef, column_name: &str) -> u64 {
    schema
        .index_of_column_by_name(None, column_name)
        .map(|idx| idx as u64)
        .unwrap_or(u64::MAX) // make sure if not found, it will report error in deserialization
}

/// Convert index back to column name for deserialization
pub fn resolve_column_name(
    index: u64,
    schema: &DFSchemaRef,
    context: &str,
    column_type: &str,
) -> DataFusionResult<String> {
    let columns = schema.columns();
    columns
        .get(index as usize)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Failed to get {} column at idx {} during unfixing {} with columns:{:?}",
                column_type, index, context, columns
            ))
        })
        .map(|field| field.name().to_string())
}

/// Batch process multiple column indices
pub fn resolve_column_names(
    indices: &[u64],
    schema: &DFSchemaRef,
    context: &str,
    column_type: &str,
) -> DataFusionResult<Vec<String>> {
    indices
        .iter()
        .map(|idx| resolve_column_name(*idx, schema, context, column_type))
        .collect()
}
