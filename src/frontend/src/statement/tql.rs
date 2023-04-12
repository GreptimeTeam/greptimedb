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

use common_query::Output;
use query::parser::{PromQuery, QueryLanguageParser};
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::tql::Tql;

use crate::error::{
    ExecLogicalPlanSnafu, NotSupportedSnafu, ParseQuerySnafu, PlanStatementSnafu, Result,
};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    pub(super) async fn execute_tql(&self, tql: Tql, query_ctx: QueryContextRef) -> Result<Output> {
        let plan = match tql {
            Tql::Eval(eval) => {
                let promql = PromQuery {
                    start: eval.start,
                    end: eval.end,
                    step: eval.step,
                    query: eval.query,
                };
                let stmt = QueryLanguageParser::parse_promql(&promql).context(ParseQuerySnafu)?;
                self.query_engine
                    .planner()
                    .plan(stmt, query_ctx.clone())
                    .await
                    .context(PlanStatementSnafu)?
            }
            Tql::Explain(_) => {
                return NotSupportedSnafu {
                    feat: "TQL EXPLAIN",
                }
                .fail()
            }
        };
        self.query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
    }
}
