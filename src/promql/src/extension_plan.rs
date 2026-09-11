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
use datafusion::arrow::array::{
    Array, Float64Array, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, TimeUnit, TimestampMillisecondType,
};
use datafusion::common::{Column, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datatypes::data_type::DataType as _;
pub use empty_metric::{EmptyMetric, EmptyMetricExec, EmptyMetricStream, build_special_time_expr};
pub use histogram_fold::{
    HistogramFold, HistogramFoldExec, HistogramFoldOperation, HistogramFoldStream,
};
pub use instant_manipulate::{InstantManipulate, InstantManipulateExec, InstantManipulateStream};
pub use normalize::{SeriesNormalize, SeriesNormalizeExec, SeriesNormalizeStream};
pub use planner::PromExtensionPlanner;
pub use range_manipulate::{RangeManipulate, RangeManipulateExec, RangeManipulateStream};
pub use scalar_calculate::ScalarCalculate;
pub use series_divide::{SeriesDivide, SeriesDivideExec, SeriesDivideStream};
pub use union_distinct_on::{UnionDistinctOn, UnionDistinctOnExec, UnionDistinctOnStream};

pub type Millisecond = <TimestampMillisecondType as ArrowPrimitiveType>::Native;

/// Borrows timestamp values without reducing their Arrow storage precision.
///
/// These integers are Arrow's native ticks, not milliseconds. Selector code must
/// compare samples on that native timeline, then convert only where PromQL's
/// millisecond evaluation or output ABI requires it.
pub(crate) fn native_timestamp_values(array: &dyn Array) -> datafusion::error::Result<&[i64]> {
    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|a| a.values().as_ref()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|a| a.values().as_ref()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| a.values().as_ref()),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| a.values().as_ref()),
        _ => None,
    };
    value.ok_or_else(|| {
        datafusion::error::DataFusionError::Execution("Time index column is not a timestamp".into())
    })
}

pub(crate) fn timestamp_unit(data_type: &DataType) -> datafusion::error::Result<TimeUnit> {
    match data_type {
        DataType::Timestamp(unit, _) => Ok(*unit),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Time index column is not a timestamp".into(),
        )),
    }
}

pub(crate) fn nanoseconds_per_native_tick(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

/// Returns the offset of an immediately underlying normalize node when the
/// requested time index retains its logical identity through projections.
pub(crate) fn local_offset(plan: &LogicalPlan, time_index: &str) -> Millisecond {
    let Some(index) = plan.schema().index_of_column_by_name(None, time_index) else {
        return 0;
    };
    let (qualifier, field) = plan.schema().qualified_field(index);
    let mut time_index = Column::new(qualifier.cloned(), field.name().clone());
    let mut plan = plan;

    loop {
        match plan {
            LogicalPlan::Extension(Extension { node }) => {
                return node
                    .as_any()
                    .downcast_ref::<SeriesNormalize>()
                    .and_then(|normalize| normalize.offset_for_time_index(&time_index))
                    .unwrap_or_default();
            }
            LogicalPlan::Projection(projection) => {
                let Some(output_index) = projection.schema.maybe_index_of_column(&time_index)
                else {
                    return 0;
                };
                let expr = &projection.expr[output_index];
                let source = match expr {
                    Expr::Column(column) => column,
                    Expr::Alias(alias) => {
                        let Expr::Column(column) = alias.expr.as_ref() else {
                            return 0;
                        };
                        if alias.name != column.name {
                            return 0;
                        }
                        column
                    }
                    _ => return 0,
                };
                let Some(input_index) = projection.input.schema().maybe_index_of_column(source)
                else {
                    return 0;
                };
                let (qualifier, field) = projection.input.schema().qualified_field(input_index);
                time_index = Column::new(qualifier.cloned(), field.name().clone());
                plan = projection.input.as_ref();
            }
            _ => return 0,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::ToDFSchema;
    use datafusion::logical_expr::{EmptyRelation, Extension, LogicalPlan, Projection};
    use datafusion_expr::col;

    use super::*;

    fn input() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new(
                    "other_ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, true),
            ]))
            .to_dfschema_ref()
            .unwrap(),
        })
    }

    fn normalized() -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                1_000,
                "timestamp",
                false,
                Vec::new(),
                input(),
            )),
        })
    }

    #[test]
    fn local_offset_tracks_identity_preserving_projections() {
        let projection =
            Projection::try_new(vec![col("timestamp"), col("value")], Arc::new(normalized()))
                .unwrap();
        let projection = Projection::try_new(
            vec![col("timestamp").alias("timestamp"), col("value")],
            Arc::new(LogicalPlan::Projection(projection)),
        )
        .unwrap();

        assert_eq!(
            1_000,
            local_offset(&LogicalPlan::Projection(projection), "timestamp")
        );
    }

    #[test]
    fn local_offset_rejects_a_different_timestamp_or_manipulator() {
        let renamed = Projection::try_new(
            vec![col("other_ts").alias("timestamp"), col("value")],
            Arc::new(normalized()),
        )
        .unwrap();
        assert_eq!(
            0,
            local_offset(&LogicalPlan::Projection(renamed), "timestamp")
        );

        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                Vec::new(),
                "timestamp".to_string(),
                normalized(),
            )),
        });
        assert_eq!(0, local_offset(&divide, "timestamp"));
    }
}
