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

//! Pins that planning a deep flat `UNION ALL` chain stays within the release
//! worker stack size.
//!
//! Planning a flat `UNION ALL` chain recurses once per term in
//! `set_expr_to_plan`, so a long chain can overflow a small thread stack; this
//! is the planner-side crash reported in issue #9356. The pinned DataFusion
//! fork enables `recursive_protection` through the umbrella `datafusion`
//! crate's default features, and the resulting checkpoints (see
//! `datafusion/sql/src/stack.rs::maybe_grow`, used by `set_expr_to_plan` and
//! `query_to_plan`) grow the stack at those recursive entry points, so the
//! statement below plans even on a deliberately small thread stack. This test
//! guards that behavior at the release stack size so the protection cannot
//! silently disappear, e.g. across a pin change.

use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result as DfResult, TableReference};
use datafusion_expr::{
    AggregateUDF, HigherOrderUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_sql::parser::DFParser;
use datafusion_sql::planner::{ContextProvider, SqlToRel};

/// Number of `SELECT` terms in the `UNION ALL` chain: enough to mirror the
/// over-long chains of issue #9356 (366 terms) with a wide margin.
const BRANCHES: usize = 1000;

/// Stack size of the planning thread — the same size as the release
/// `global-worker` thread stacks.
///
/// Chosen from measured debug-build thresholds (x86_64-pc-windows-gnu, in an
/// isolated harness against `datafusion-sql` alone) so that both directions
/// keep a wide margin: without `recursive_protection` requested, planning this
/// chain overflows stacks up to 16 MiB (it only completes at 32 MiB), while
/// with the feature it plans on stacks as small as 256 KiB.
const PLANNING_STACK_SIZE: usize = 2 * 1024 * 1024;

/// A context provider for statements that reference neither tables nor
/// functions, mirroring what an [`SqlToRel`] planner needs for literals.
struct TestContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for TestContextProvider {
    fn get_table_source(&self, name: TableReference) -> DfResult<Arc<dyn TableSource>> {
        Err(DataFusionError::NotImplemented(format!(
            "no tables in this test: {name}"
        )))
    }

    fn create_cte_work_table(
        &self,
        name: &str,
        _schema: SchemaRef,
    ) -> DfResult<Arc<dyn TableSource>> {
        Err(DataFusionError::NotImplemented(format!("no CTEs: {name}")))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        vec![]
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        vec![]
    }

    fn udaf_names(&self) -> Vec<String> {
        vec![]
    }

    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }
}

fn build_union_all_sql(branches: usize) -> String {
    let mut sql = String::new();
    for i in 0..branches {
        if i > 0 {
            sql.push_str(" UNION ALL ");
        }
        sql.push_str("SELECT 1");
    }
    sql
}

fn count_unions(plan: &LogicalPlan) -> usize {
    let mut unions = 0;
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if matches!(node, LogicalPlan::Union(_)) {
            unions += 1;
        }
        stack.extend(node.inputs());
    }
    unions
}

#[test]
fn planning_deep_union_all_completes_on_small_stack() {
    let sql = build_union_all_sql(BRANCHES);

    // Parsing a flat chain is iterative; the AST is deep but only traversed
    // and dropped recursively, which a test-harness thread handles fine.
    let mut statements = DFParser::parse_sql(&sql).expect("parse failed");
    assert_eq!(statements.len(), 1);
    let statement = statements.pop_front().unwrap();

    // Planning recurses once per `UNION ALL` term over the whole chain: on
    // this deliberately small stack it aborts with a stack overflow unless the
    // `recursive_protection` feature grows the stack at the planner's
    // recursive entry points.
    let plan = std::thread::Builder::new()
        .name("deep-sql-plan".to_string())
        .stack_size(PLANNING_STACK_SIZE)
        .spawn(move || -> std::result::Result<LogicalPlan, String> {
            let provider = TestContextProvider {
                options: ConfigOptions::default(),
            };
            SqlToRel::new(&provider)
                .statement_to_plan(statement)
                .map_err(|e| e.to_string())
        })
        .unwrap()
        .join()
        .unwrap()
        .unwrap_or_else(|e| panic!("planning failed: {e}"));

    assert_eq!(count_unions(&plan), BRANCHES - 1);

    // Dropping the deep plan recurses as well; do it on a generous stack.
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || drop(plan))
        .unwrap()
        .join()
        .unwrap();
}
