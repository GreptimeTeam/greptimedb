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

use catalog::system_schema::information_schema::tables::{
    ENGINE as TABLE_ENGINE, TABLE_CATALOG, TABLE_NAME, TABLE_SCHEMA,
};
use common_telemetry::tracing;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{Expr, col, lit, regexp_match};
use datafusion_expr::LogicalPlan;
use promql_parser::label::{MatchOp, Matcher};
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{self, Result};

/// The maximum number of metrics at one time.
const MAX_METRICS_NUM: usize = 1024;

/// Create a DataFrame from promql `__name__` matchers.
/// # Panics
///  Panic when the machers contains `MatchOp::Equal`.
#[tracing::instrument(skip_all)]
pub fn metric_name_matchers_to_plan(
    dataframe: DataFrame,
    matchers: Vec<Matcher>,
    ctx: &QueryContextRef,
) -> Result<LogicalPlan> {
    assert!(!matchers.is_empty());

    let mut conditions = Vec::with_capacity(matchers.len() + 3);

    conditions.push(col(TABLE_CATALOG).eq(lit(ctx.current_catalog())));
    conditions.push(col(TABLE_SCHEMA).eq(lit(ctx.current_schema())));
    // Must be metric engine
    conditions.push(col(TABLE_ENGINE).eq(lit("metric")));

    for m in matchers {
        let value = &m.value;

        match &m.op {
            MatchOp::NotEqual => {
                conditions.push(col(TABLE_NAME).not_eq(lit(value)));
            }
            // Case sensitive regexp match
            MatchOp::Re(regex) => {
                conditions.push(
                    regexp_match(col(TABLE_NAME), lit(regex.to_string()), None).is_not_null(),
                );
            }
            // Case sensitive regexp not match
            MatchOp::NotRe(regex) => {
                conditions
                    .push(regexp_match(col(TABLE_NAME), lit(regex.to_string()), None).is_null());
            }
            _ => unreachable!("checked outside"),
        }
    }

    // Safety: conditions MUST not be empty, reduce always return Some(expr).
    let conditions = conditions.into_iter().reduce(Expr::and).unwrap();

    let dataframe = dataframe
        .filter(conditions)
        .context(error::DataFrameSnafu)?
        .select(vec![col(TABLE_NAME)])
        .context(error::DataFrameSnafu)?
        .limit(0, Some(MAX_METRICS_NUM))
        .context(error::DataFrameSnafu)?;

    Ok(dataframe.into_parts().1)
}
