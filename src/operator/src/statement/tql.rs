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

use common_query::Output;
use common_telemetry::tracing;
use datafusion_expr::LogicalPlan;
use query::parser::{
    PromQuery, QueryLanguageParser, ANALYZE_NODE_NAME, ANALYZE_VERBOSE_NODE_NAME,
    DEFAULT_LOOKBACK_STRING, EXPLAIN_NODE_NAME, EXPLAIN_VERBOSE_NODE_NAME,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::tql::Tql;

use crate::error::{ExecLogicalPlanSnafu, ParseQuerySnafu, PlanStatementSnafu, Result};
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
                };
                QueryLanguageParser::parse_promql(&promql, query_ctx).context(ParseQuerySnafu)?
            }
            Tql::Explain(explain) => {
                let promql = PromQuery {
                    query: explain.query,
                    lookback: explain
                        .lookback
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
                    ..PromQuery::default()
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
                    .unwrap()
            }
            Tql::Analyze(analyze) => {
                let promql = PromQuery {
                    start: analyze.start,
                    end: analyze.end,
                    step: analyze.step,
                    query: analyze.query,
                    lookback: analyze
                        .lookback
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
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
                    .unwrap()
            }
        };
        self.query_engine
            .planner()
            .plan(stmt, query_ctx.clone())
            .await
            .context(PlanStatementSnafu)
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
