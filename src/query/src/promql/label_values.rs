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

use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::Alias;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{col, Cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_sql::TableReference;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use datatypes::prelude::ConcreteDataType;
use snafu::{OptionExt, ResultExt};
use table::TableRef;

use crate::promql::error::{DataFusionPlanningSnafu, Result, TimeIndexNotFoundSnafu};

fn build_time_filter(time_index_expr: Expr, start: i64, end: i64) -> Expr {
    time_index_expr
        .clone()
        .gt_eq(Expr::Literal(ScalarValue::TimestampMillisecond(
            Some(start),
            None,
        )))
        .and(
            time_index_expr.lt_eq(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(end),
                None,
            ))),
        )
}

/// Rewrite label values query to DataFusion logical plan.
pub fn rewrite_label_values_query(
    table: TableRef,
    mut scan_plan: LogicalPlan,
    mut conditions: Vec<Expr>,
    label_name: String,
    start: SystemTime,
    end: SystemTime,
) -> Result<LogicalPlan> {
    let table_ref = TableReference::partial(
        table.table_info().schema_name.as_str(),
        table.table_info().name.as_str(),
    );
    let schema = table.schema();
    let ts_column = schema
        .timestamp_column()
        .with_context(|| TimeIndexNotFoundSnafu {
            table: table.table_info().full_table_name(),
        })?;

    let is_time_index_ms =
        ts_column.data_type == ConcreteDataType::timestamp_millisecond_datatype();
    let time_index_expr = col(Column::from_name(ts_column.name.clone()));

    if !is_time_index_ms {
        // cast to ms if time_index not in Millisecond precision
        let expr = vec![
            col(Column::from_name(label_name.clone())),
            Expr::Alias(Alias {
                expr: Box::new(Expr::Cast(Cast {
                    expr: Box::new(time_index_expr.clone()),
                    data_type: ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                })),
                relation: Some(table_ref),
                name: ts_column.name.clone(),
            }),
        ];
        scan_plan = LogicalPlanBuilder::from(scan_plan)
            .project(expr)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
    };

    let start = start.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let end = end.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

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
