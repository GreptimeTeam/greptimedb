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

use std::time::{SystemTime, UNIX_EPOCH};

use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{col, Expr, LogicalPlan, LogicalPlanBuilder};
use snafu::{OptionExt, ResultExt};
use table::TableRef;

use crate::promql::error::{
    DataFusionPlanningSnafu, Result, TimeIndexNotFoundSnafu, TimestampOutOfRangeSnafu,
};

fn build_time_filter(time_index_expr: Expr, start: Timestamp, end: Timestamp) -> Expr {
    time_index_expr
        .clone()
        .gt_eq(Expr::Literal(timestamp_to_scalar_value(start), None))
        .and(time_index_expr.lt_eq(Expr::Literal(timestamp_to_scalar_value(end), None)))
}

fn timestamp_to_scalar_value(timestamp: Timestamp) -> ScalarValue {
    let value = timestamp.value();
    match timestamp.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
    }
}

/// Rewrite label values query to DataFusion logical plan.
pub fn rewrite_label_values_query(
    table: TableRef,
    scan_plan: LogicalPlan,
    mut conditions: Vec<Expr>,
    label_name: String,
    start: SystemTime,
    end: SystemTime,
) -> Result<LogicalPlan> {
    let schema = table.schema();
    let ts_column = schema
        .timestamp_column()
        .with_context(|| TimeIndexNotFoundSnafu {
            table: table.table_info().full_table_name(),
        })?;
    let unit = ts_column
        .data_type
        .as_timestamp()
        .map(|data_type| data_type.unit())
        .with_context(|| TimeIndexNotFoundSnafu {
            table: table.table_info().full_table_name(),
        })?;

    // We only support millisecond precision at most.
    let start =
        Timestamp::new_millisecond(start.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64);
    let start = start.convert_to(unit).context(TimestampOutOfRangeSnafu {
        timestamp: start.value(),
        unit,
    })?;
    let end =
        Timestamp::new_millisecond(end.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64);
    let end = end.convert_to(unit).context(TimestampOutOfRangeSnafu {
        timestamp: end.value(),
        unit,
    })?;
    let time_index_expr = col(Column::from_name(ts_column.name.clone()));

    conditions.push(build_time_filter(time_index_expr, start, end));
    // Safety: `conditions` is not empty.
    let filter = conjunction(conditions).unwrap();

    // Builds time filter
    let logical_plan = LogicalPlanBuilder::from(scan_plan)
        .filter(filter)
        .context(DataFusionPlanningSnafu)?
        .project(vec![col(Column::from_name(label_name))])
        .context(DataFusionPlanningSnafu)?
        .distinct()
        .context(DataFusionPlanningSnafu)?
        .build()
        .context(DataFusionPlanningSnafu)?;

    Ok(logical_plan)
}
