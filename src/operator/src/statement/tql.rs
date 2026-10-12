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

use std::collections::HashMap;
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_query::Output;
use common_telemetry::tracing;
use datafusion_expr::{Analyze, Explain, LogicalPlan, PlanType, ToStringifiedPlan};
use query::parser::{
    ANALYZE_NODE_NAME, ANALYZE_VERBOSE_NODE_NAME, DEFAULT_LOOKBACK_STRING, EXPLAIN_NODE_NAME,
    EXPLAIN_VERBOSE_NODE_NAME, PromQuery, QueryLanguageParser,
};
use query::promql::planner::PromPlanner;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::tql::Tql;

use crate::error::{
    ExecLogicalPlanSnafu, ExternalSnafu, ParseQuerySnafu, PlanStatementSnafu, Result,
};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    /// Plan the given [Tql] query and return the [LogicalPlan].
    #[tracing::instrument(skip_all)]
    pub async fn plan_tql(&self, tql: Tql, query_ctx: &QueryContextRef) -> Result<LogicalPlan> {
        let stmt = match tql {
            Tql::Eval(eval) => {
                let promql = PromQuery {
                    start: eval.start,
                    end: eval.end,
                    step: eval.step,
                    query: eval.query,
                    lookback: eval
                        .lookback
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
                    alias: eval.alias,
                };
                QueryLanguageParser::parse_promql(&promql, query_ctx).context(ParseQuerySnafu)?
            }
            Tql::Explain(explain) => {
                if let Some(format) = &explain.format {
                    query_ctx.set_explain_format(format.to_string());
                }

                let promql = PromQuery {
                    query: explain.query,
                    start: explain.start,
                    end: explain.end,
                    step: explain.step,
                    lookback: explain
                        .lookback
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
                    alias: explain.alias,
                };
                let explain_node_name = if explain.is_verbose {
                    EXPLAIN_VERBOSE_NODE_NAME
                } else {
                    EXPLAIN_NODE_NAME
                }
                .to_string();
                let params = HashMap::from([("name".to_string(), explain_node_name)]);
                QueryLanguageParser::parse_promql(&promql, query_ctx)
                    .context(ParseQuerySnafu)?
                    .post_process(params)
                    .context(ParseQuerySnafu)?
            }
            Tql::Analyze(analyze) => {
                if let Some(format) = &analyze.format {
                    query_ctx.set_explain_format(format.to_string());
                }

                let promql = PromQuery {
                    start: analyze.start,
                    end: analyze.end,
                    step: analyze.step,
                    query: analyze.query,
                    lookback: analyze
                        .lookback
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
                    alias: analyze.alias,
                };
                let analyze_node_name = if analyze.is_verbose {
                    ANALYZE_VERBOSE_NODE_NAME
                } else {
                    ANALYZE_NODE_NAME
                }
                .to_string();
                let params = HashMap::from([("name".to_string(), analyze_node_name)]);
                QueryLanguageParser::parse_promql(&promql, query_ctx)
                    .context(ParseQuerySnafu)?
                    .post_process(params)
                    .context(ParseQuerySnafu)?
            }
        };
        let plan = self
            .query_engine
            .planner()
            .plan(&stmt, query_ctx.clone())
            .await
            .context(PlanStatementSnafu)?;
        strip_metric_name_output(plan)
    }

    /// Execute the given [Tql] query and return the result.
    #[tracing::instrument(skip_all)]
    pub(super) async fn execute_tql(&self, tql: Tql, query_ctx: QueryContextRef) -> Result<Output> {
        let plan = self.plan_tql(tql, &query_ctx).await?;
        self.query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
    }
}

/// Removes the PromQL metric-name identity from a TQL plan's result schema.
///
/// The identity is a PromQL-internal column kept in the plan for the Prometheus HTTP API; a TQL
/// statement is tabular, so its result must not expose it.
///
/// `EXPLAIN`/`ANALYZE` wrap their input and produce the two fixed planning columns, so the
/// identity is stripped from the wrapped child and the wrapper is rebuilt around it. Rebuilding
/// `EXPLAIN` also regenerates its initial stringified plan so the reported text starts from the
/// stripped child. Stripping adds a projection on top of the child; the identity still shows in
/// the plan text of the nodes below that projection, which is the plan as it was built.
fn strip_metric_name_output(plan: LogicalPlan) -> Result<LogicalPlan> {
    let strip = |plan| {
        PromPlanner::strip_metric_name_column(plan)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    };

    match plan {
        LogicalPlan::Explain(explain) => {
            let child = strip(Arc::unwrap_or_clone(explain.plan))?;
            let stringified_plans = vec![child.to_stringified(PlanType::InitialLogicalPlan)];
            Ok(LogicalPlan::Explain(Explain {
                plan: Arc::new(child),
                stringified_plans,
                ..explain
            }))
        }
        LogicalPlan::Analyze(analyze) => {
            let input = strip(Arc::unwrap_or_clone(analyze.input))?;
            Ok(LogicalPlan::Analyze(Analyze {
                input: Arc::new(input),
                ..analyze
            }))
        }
        plan => strip(plan),
    }
}
