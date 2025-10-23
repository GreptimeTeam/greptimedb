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
use datafusion::arrow::datatypes::{ArrowPrimitiveType, TimestampMillisecondType};
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
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
