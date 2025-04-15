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

use common_function::scalars::matches_term::MatchesTermFunction;
use common_function::scalars::udf::create_udf;
use common_function::state::FunctionState;
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use session::context::QueryContext;

use crate::plan::ExtractExpr;

/// TranscribeAtatRule is an analyzer rule that transcribes `@@` operator
/// to `matches_term` function.
///
/// Example:
/// ```sql
/// SELECT matches_term('cat!', 'cat') as result;
///
/// SELECT matches_term(`log_message`, '/start') as `matches_start` FROM t;
/// ```
///
/// to
///
/// ```sql
/// SELECT 'cat!' @@ 'cat' as result;
///
/// SELECT `log_message` @@ '/start' as `matches_start` FROM t;
/// ```
#[derive(Debug)]
pub struct TranscribeAtatRule;

impl AnalyzerRule for TranscribeAtatRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform(Self::do_analyze).map(|x| x.data)
    }

    fn name(&self) -> &str {
        "TranscribeAtatRule"
    }
}

impl TranscribeAtatRule {
    fn do_analyze(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut rewriter = TranscribeAtatRewriter::default();
        let new_expr = plan
            .expressions_consider_join()
            .into_iter()
            .map(|e| e.rewrite(&mut rewriter).map(|x| x.data))
            .collect::<Result<Vec<_>>>()?;

        if rewriter.transcribed {
            let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
            plan.with_new_exprs(new_expr, inputs).map(Transformed::yes)
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

#[derive(Default)]
struct TranscribeAtatRewriter {
    transcribed: bool,
}

impl TreeNodeRewriter for TranscribeAtatRewriter {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::BinaryExpr(binary_expr) = &expr
            && matches!(binary_expr.op, datafusion_expr::Operator::AtAt)
        {
            self.transcribed = true;
            let scalar_udf = create_udf(
                Arc::new(MatchesTermFunction),
                QueryContext::arc(),
                Arc::new(FunctionState::default()),
            );
            let exprs = vec![
                binary_expr.left.as_ref().clone(),
                binary_expr.right.as_ref().clone(),
            ];
            Ok(Transformed::yes(Expr::ScalarFunction(
                ScalarFunction::new_udf(Arc::new(scalar_udf), exprs),
            )))
        } else {
            Ok(Transformed::no(expr))
        }
    }
}
