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
    CREATE_OPTIONS, ENGINE as TABLE_ENGINE, TABLE_CATALOG, TABLE_NAME, TABLE_SCHEMA,
};
use common_telemetry::tracing;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{Expr, col, lit, regexp_match};
use datafusion_expr::LogicalPlan;
use promql_parser::label::{MatchOp, Matcher};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, PHYSICAL_TABLE_METADATA_KEY};

use crate::error::{self, Result};

/// The maximum number of metric tables a single `__name__` matcher may authorize per request.
///
/// [`metric_name_matchers_to_plan`] resolves the candidate names without a cap, so callers MUST
/// filter the candidates by table permission first and then reject an *authorized* set larger
/// than this instead of answering from a truncated candidate list. The planner keeps the same
/// bound for the SQL/TQL fallback in `candidate_metric_names`.
pub const MAX_METRICS_NUM: usize = 1024;

/// Create a DataFrame from promql `__name__` matchers.
///
/// The plan yields every matching metric table name and never truncates the candidates: a caller
/// authorized for at most [`MAX_METRICS_NUM`] names must still see the complete raw set, or it
/// would answer from an incomplete candidate list. Filtering and the cap belong to the caller.
/// # Panics
///  Panic when the machers contains `MatchOp::Equal`.
#[tracing::instrument(skip_all)]
pub fn metric_name_matchers_to_plan(
    dataframe: DataFrame,
    matchers: Vec<Matcher>,
    schema: &str,
    ctx: &QueryContextRef,
) -> Result<LogicalPlan> {
    assert!(!matchers.is_empty());

    let mut conditions = Vec::with_capacity(matchers.len() + 5);

    conditions.push(col(TABLE_CATALOG).eq(lit(ctx.current_catalog())));
    conditions.push(col(TABLE_SCHEMA).eq(lit(schema)));
    // Must be metric engine
    conditions.push(col(TABLE_ENGINE).eq(lit("metric")));
    // Physical metric tables are internal. PromQL queries user-visible logical tables.
    // Keep in sync with `candidate_metric_names` in `src/query/src/promql/planner.rs` (query
    // layer), which filters the same metric engine and logical/physical flags over catalog tables.
    conditions.push(
        regexp_match(
            col(CREATE_OPTIONS),
            lit(format!("(^| ){LOGICAL_TABLE_METADATA_KEY}=")),
            None,
        )
        .is_not_null(),
    );
    conditions.push(
        regexp_match(
            col(CREATE_OPTIONS),
            lit(format!("(^| ){PHYSICAL_TABLE_METADATA_KEY}=")),
            None,
        )
        .is_null(),
    );

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
        .context(error::DataFrameSnafu)?;

    Ok(dataframe.into_parts().1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use promql_parser::label::{MatchOp, Matcher};
    use session::context::QueryContext;

    use super::*;

    #[tokio::test]
    async fn test_metric_name_plan_only_returns_logical_metric_tables() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TABLE_CATALOG, DataType::Utf8, false),
            Field::new(TABLE_SCHEMA, DataType::Utf8, false),
            Field::new(TABLE_NAME, DataType::Utf8, false),
            Field::new(TABLE_ENGINE, DataType::Utf8, false),
            Field::new(CREATE_OPTIONS, DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["greptime"; 5])),
                Arc::new(StringArray::from(vec![
                    "public", "private", "private", "private", "private",
                ])),
                Arc::new(StringArray::from(vec![
                    "logical_public",
                    "logical_private",
                    "physical",
                    "misleading",
                    "mito",
                ])),
                Arc::new(StringArray::from(vec![
                    "metric", "metric", "metric", "metric", "mito",
                ])),
                Arc::new(StringArray::from(vec![
                    "on_physical_table=physical",
                    "ttl=1d on_physical_table=physical",
                    "physical_metric_table=",
                    "foo=x on_physical_table=physical physical_metric_table=",
                    "on_physical_table=physical",
                ])),
            ],
        )
        .unwrap();

        let session = SessionContext::new();
        let dataframe = session.read_batch(batch).unwrap();
        let query_ctx = Arc::new(QueryContext::with("greptime", "public"));
        let plan = metric_name_matchers_to_plan(
            dataframe,
            vec![Matcher::new(MatchOp::NotEqual, "__name__", "")],
            "private",
            &query_ctx,
        )
        .unwrap();
        let batches = session
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut names = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            names.extend(column.iter().flatten().map(str::to_owned));
        }
        assert_eq!(vec!["logical_private"], names);
    }

    /// Candidate resolution must never truncate. The HTTP layer filters these names by table
    /// permission and only then enforces the union cap, so a matcher that matches more names than
    /// the cap can still be answerable from a smaller authorized subset.
    #[tokio::test]
    async fn test_metric_name_plan_returns_every_matching_candidate() {
        let tables = MAX_METRICS_NUM + 1;
        let names = (0..tables)
            .map(|i| format!("logical_{i}"))
            .collect::<Vec<_>>();
        let create_options = (0..tables)
            .map(|i| format!("on_physical_table=physical_{i}"))
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new(vec![
            Field::new(TABLE_CATALOG, DataType::Utf8, false),
            Field::new(TABLE_SCHEMA, DataType::Utf8, false),
            Field::new(TABLE_NAME, DataType::Utf8, false),
            Field::new(TABLE_ENGINE, DataType::Utf8, false),
            Field::new(CREATE_OPTIONS, DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["greptime"; tables])),
                Arc::new(StringArray::from(vec!["private"; tables])),
                Arc::new(StringArray::from(
                    names.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["metric"; tables])),
                Arc::new(StringArray::from(
                    create_options
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        let session = SessionContext::new();
        let dataframe = session.read_batch(batch).unwrap();
        let query_ctx = Arc::new(QueryContext::with("greptime", "public"));
        let plan = metric_name_matchers_to_plan(
            dataframe,
            vec![Matcher::new(MatchOp::NotEqual, "__name__", "")],
            "private",
            &query_ctx,
        )
        .unwrap();
        let batches = session
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut returned = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            returned.extend(column.iter().flatten().map(str::to_owned));
        }
        // All matching names are returned, including the ones past the union cap.
        assert_eq!(MAX_METRICS_NUM + 1, returned.len());

        returned.sort_unstable();
        let mut expected = names;
        expected.sort_unstable();
        assert_eq!(expected, returned);
    }
}
