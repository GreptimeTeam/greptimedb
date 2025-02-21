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

use datafusion_common::Column;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_sql::TableReference;
use snafu::ResultExt;

use crate::promql::error::{DataFusionPlanningSnafu, Result};

/// Generates a list of expressions that can be used to project out the specified columns from the input relation.
///
/// # Arguments
///
/// * `qualifier` - The qualifier of the input relation.
/// * `columns` - A list of columns to project out.
pub fn project_columns<'a, I: Iterator<Item = &'a String>>(
    qualifier: Option<TableReference>,
    columns: I,
) -> impl Iterator<Item = Expr> + use<'a, I> {
    columns.map(move |col| Expr::Column(Column::new(qualifier.clone(), col.to_string())))
}

/// Generates an expression that can be used to project out the specified time index column from the input relation.
///
/// # Arguments
///
/// * `qualifier` - The qualifier of the input relation.
/// * `time_index_column` - The name of the time index column to project out.
/// * `time_index_alias` - The alias of the time index column to project out.
pub fn project_time_index_column(
    qualifier: Option<TableReference>,
    time_index_column: Option<&String>,
    time_index_alias: Option<&String>,
) -> Option<Expr> {
    time_index_column.map(|c| {
        let expr = Expr::Column(Column::new(qualifier.clone(), c));
        if let Some(alias) = time_index_alias {
            expr.alias(alias)
        } else {
            expr
        }
    })
}

/// Projects the specified expressions and aliases them with the specified alias.
///
/// # Arguments
///
/// * `plan` - The input relation to project.
/// * `qualifier` - The qualifier of the input relation.
/// * `exprs` - The expressions to project.
pub fn with_project_and_alias(
    plan: LogicalPlan,
    alias: String,
    exprs: impl Iterator<Item = Expr>,
) -> Result<LogicalPlan> {
    LogicalPlanBuilder::from(plan)
        .project(exprs)
        .context(DataFusionPlanningSnafu)?
        .alias(alias)
        .context(DataFusionPlanningSnafu)?
        .build()
        .context(DataFusionPlanningSnafu)
}
