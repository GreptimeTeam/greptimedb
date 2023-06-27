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

use arrow::array::{
    PrimitiveArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, Int64Type};
use arrow::error::ArrowError;
use arrow_array::{Array, BooleanArray, RecordBatch};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion::physical_plan::PhysicalExpr;
use datatypes::prelude::ConcreteDataType;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use table::predicate::Predicate;

use crate::error;
use crate::schema::StoreSchema;

/// Builds time range row filter.
pub(crate) fn build_row_filter(
    time_range: TimestampRange,
    predicate: &Predicate,
    store_schema: &Arc<StoreSchema>,
    schema_desc: &SchemaDescriptor,
    projection_mask: ProjectionMask,
    projection: Vec<usize>,
) -> Option<RowFilter> {
    let ts_col_idx = store_schema.timestamp_index();
    let ts_col = store_schema.columns().get(ts_col_idx)?;
    let ts_col_unit = match &ts_col.desc.data_type {
        ConcreteDataType::Int64(_) => TimeUnit::Millisecond,
        ConcreteDataType::Timestamp(ts_type) => ts_type.unit(),
        _ => unreachable!(),
    };

    let ts_col_projection = ProjectionMask::roots(schema_desc, vec![ts_col_idx]);

    // checks if converting time range unit into ts col unit will result into rounding error.
    if time_unit_lossy(&time_range, ts_col_unit) {
        let filter = RowFilter::new(vec![Box::new(PlainTimestampRowFilter::new(
            time_range,
            ts_col_projection,
        ))]);
        return Some(filter);
    }

    // If any of the conversion overflows, we cannot use arrow's computation method, instead
    // we resort to plain filter that compares timestamp with given range, less efficient,
    // but simpler.
    // TODO(hl): If the range is gt_eq/lt, we also use PlainTimestampRowFilter, but these cases
    // can also use arrow's gt_eq_scalar/lt_scalar methods.
    let time_range_row_filter = if let (Some(lower), Some(upper)) = (
        time_range
            .start()
            .and_then(|s| s.convert_to(ts_col_unit))
            .map(|t| t.value()),
        time_range
            .end()
            .and_then(|s| s.convert_to(ts_col_unit))
            .map(|t| t.value()),
    ) {
        Box::new(FastTimestampRowFilter::new(ts_col_projection, lower, upper)) as _
    } else {
        Box::new(PlainTimestampRowFilter::new(time_range, ts_col_projection)) as _
    };
    let mut predicates = vec![time_range_row_filter];
    if let Ok(datafusion_filters) = predicate_to_row_filter(predicate, projection_mask, projection)
    {
        predicates.extend(datafusion_filters);
    }
    let filter = RowFilter::new(predicates);
    Some(filter)
}

fn predicate_to_row_filter(
    predicate: &Predicate,
    projection_mask: ProjectionMask,
    projection: Vec<usize>,
) -> error::Result<Vec<Box<dyn ArrowPredicate>>> {
    let mut datafusion_predicates = Vec::with_capacity(predicate.exprs().len());
    for expr in predicate.exprs() {
        datafusion_predicates.push(Box::new(DatafusionArrowPredicate {
            projection_mask: projection_mask.clone(),
            projection: projection.clone(),
            physical_expr: expr.clone(),
        }) as _);
    }

    Ok(datafusion_predicates)
}

struct DatafusionArrowPredicate {
    projection_mask: ProjectionMask,
    projection: Vec<usize>,
    physical_expr: Arc<dyn PhysicalExpr>,
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        // 1. project according to projection
        let projected_batch = batch.project(&self.projection)?;

        // 2. evaluate the expr against the record batch
        match self
            .physical_expr
            .evaluate(&projected_batch)
            .map(|v| v.into_array(batch.num_rows()))
        {
            Ok(array) => {
                // TODO(hl): remove this unwrap
                let bool_arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();
                Ok(bool_arr)
            }
            Err(e) => Err(ArrowError::ComputeError(format!(
                "Error evaluating filter predicate: {e:?}"
            ))),
        }
    }
}

fn time_unit_lossy(range: &TimestampRange, ts_col_unit: TimeUnit) -> bool {
    range
        .start()
        .map(|start| start.unit().factor() < ts_col_unit.factor())
        .unwrap_or(false)
        || range
            .end()
            .map(|end| end.unit().factor() < ts_col_unit.factor())
            .unwrap_or(false)
}

/// `FastTimestampRowFilter` is used to filter rows within given timestamp range when reading
/// row groups from parquet files, while avoids fetching all columns from SSTs file.
struct FastTimestampRowFilter {
    lower_bound: i64,
    upper_bound: i64,
    projection: ProjectionMask,
}

impl FastTimestampRowFilter {
    fn new(projection: ProjectionMask, lower_bound: i64, upper_bound: i64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            projection,
        }
    }
}

impl ArrowPredicate for FastTimestampRowFilter {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    /// Selects the rows matching given time range.
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        // the projection has only timestamp column, so we can safely take the first column in batch.
        let ts_col = batch.column(0);

        macro_rules! downcast_and_compute {
            ($typ: ty) => {
                {
                    let ts_col = ts_col
                        .as_any()
                        .downcast_ref::<$typ>()
                        .unwrap(); // safety: we've checked the data type of timestamp column.
                    let left = arrow::compute::gt_eq_scalar(ts_col, self.lower_bound)?;
                    let right = arrow::compute::lt_scalar(ts_col, self.upper_bound)?;
                    arrow::compute::and(&left, &right)
                }
            };
        }

        match ts_col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                arrow::datatypes::TimeUnit::Second => {
                    downcast_and_compute!(TimestampSecondArray)
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    downcast_and_compute!(TimestampMillisecondArray)
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    downcast_and_compute!(TimestampMicrosecondArray)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    downcast_and_compute!(TimestampNanosecondArray)
                }
            },
            DataType::Int64 => downcast_and_compute!(PrimitiveArray<Int64Type>),
            _ => {
                unreachable!()
            }
        }
    }
}

/// [PlainTimestampRowFilter] iterates each element in timestamp column, build a [Timestamp] struct
/// and checks if given time range contains the timestamp.
struct PlainTimestampRowFilter {
    time_range: TimestampRange,
    projection: ProjectionMask,
}

impl PlainTimestampRowFilter {
    fn new(time_range: TimestampRange, projection: ProjectionMask) -> Self {
        Self {
            time_range,
            projection,
        }
    }
}

impl ArrowPredicate for PlainTimestampRowFilter {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> std::result::Result<BooleanArray, ArrowError> {
        // the projection has only timestamp column, so we can safely take the first column in batch.
        let ts_col = batch.column(0);

        macro_rules! downcast_and_compute {
            ($array_ty: ty, $unit: ident) => {{
                    let ts_col = ts_col
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap(); // safety: we've checked the data type of timestamp column.
                    Ok(BooleanArray::from_iter(ts_col.iter().map(|ts| {
                        ts.map(|val| {
                            Timestamp::new(val, TimeUnit::$unit)
                        }).map(|ts| {
                            self.time_range.contains(&ts)
                        })
                    })))

            }};
        }

        match ts_col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                arrow::datatypes::TimeUnit::Second => {
                    downcast_and_compute!(TimestampSecondArray, Second)
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    downcast_and_compute!(TimestampMillisecondArray, Millisecond)
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    downcast_and_compute!(TimestampMicrosecondArray, Microsecond)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    downcast_and_compute!(TimestampNanosecondArray, Nanosecond)
                }
            },
            DataType::Int64 => {
                downcast_and_compute!(PrimitiveArray<Int64Type>, Millisecond)
            }
            _ => {
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_unit_lossy(range_unit: TimeUnit, col_unit: TimeUnit, expect: bool) {
        assert_eq!(
            expect,
            time_unit_lossy(
                &TimestampRange::with_unit(0, 1, range_unit).unwrap(),
                col_unit
            )
        )
    }

    #[test]
    fn test_time_unit_lossy() {
        // converting a range with unit second to millisecond will not cause rounding error
        check_unit_lossy(TimeUnit::Second, TimeUnit::Second, false);
        check_unit_lossy(TimeUnit::Second, TimeUnit::Millisecond, false);
        check_unit_lossy(TimeUnit::Second, TimeUnit::Microsecond, false);
        check_unit_lossy(TimeUnit::Second, TimeUnit::Nanosecond, false);

        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Second, true);
        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Millisecond, false);
        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Microsecond, false);
        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Nanosecond, false);

        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Second, true);
        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Millisecond, true);
        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Microsecond, false);
        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Nanosecond, false);

        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Second, true);
        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Millisecond, true);
        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Microsecond, true);
        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Nanosecond, false);
    }
}
