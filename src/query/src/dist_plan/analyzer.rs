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

use std::sync::{Arc, Mutex};

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_expr::{Extension, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;

use crate::dist_plan::commutativity::{
    partial_commutative_transformer, Categorizer, Commutativity,
};
use crate::dist_plan::merge_scan::MergeScanLogicalPlan;
use crate::dist_plan::utils;

pub struct DistPlannerAnalyzer;

impl AnalyzerRule for DistPlannerAnalyzer {
    fn name(&self) -> &str {
        "DistPlannerAnalyzer"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<LogicalPlan> {
        // (1) add merge scan
        let plan = plan.transform(&Self::add_merge_scan)?;

        // (2) transform up merge scan
        let mut visitor = CommutativeVisitor::new();
        plan.visit(&mut visitor)?;
        let state = ExpandState::new();
        let plan = plan.transform_down(&|plan| Self::expand(plan, &visitor, &state))?;

        // (3) remove placeholder merge scan
        let plan = plan.transform(&Self::remove_placeholder_merge_scan)?;

        Ok(plan)
    }
}

impl DistPlannerAnalyzer {
    /// Add [MergeScanLogicalPlan] before the table scan
    fn add_merge_scan(plan: LogicalPlan) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        Ok(match plan {
            LogicalPlan::TableScan(table_scan) => {
                let ext_plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(MergeScanLogicalPlan::new(
                        LogicalPlan::TableScan(table_scan),
                        true,
                    )),
                });
                Transformed::Yes(ext_plan)
            }
            _ => Transformed::No(plan),
        })
    }

    /// Remove placeholder [MergeScanLogicalPlan]
    fn remove_placeholder_merge_scan(
        plan: LogicalPlan,
    ) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        Ok(match &plan {
            LogicalPlan::Extension(extension) => {
                if extension.node.name() == MergeScanLogicalPlan::name() {
                    let merge_scan = extension
                        .node
                        .as_any()
                        .downcast_ref::<MergeScanLogicalPlan>()
                        .unwrap();
                    if merge_scan.is_placeholder() {
                        Transformed::Yes(merge_scan.input().clone())
                    } else {
                        Transformed::No(plan)
                    }
                } else {
                    Transformed::No(plan)
                }
            }
            _ => Transformed::No(plan),
        })
    }

    /// Expand stages on the stop node
    fn expand(
        mut plan: LogicalPlan,
        visitor: &CommutativeVisitor,
        state: &ExpandState,
    ) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        if state.is_transformed() {
            // only transform once
            return Ok(Transformed::No(plan));
        }
        if let Some(stop_node) = visitor.stop_node && utils::hash_plan(&plan) != stop_node {
            // only act with the stop node or the root (the first node seen by this closure) if no stop node
            return Ok(Transformed::No(plan));
        }

        // add merge scan
        plan = MergeScanLogicalPlan::new(plan, false).into_logical_plan();

        // add stages
        for new_stage in &visitor.next_stage {
            plan = new_stage.with_new_inputs(&[plan])?
        }

        state.set_transformed();
        Ok(Transformed::Yes(plan))
    }
}

struct ExpandState {
    transformed: Mutex<bool>,
}

impl ExpandState {
    pub fn new() -> Self {
        Self {
            transformed: Mutex::new(false),
        }
    }

    pub fn is_transformed(&self) -> bool {
        *self.transformed.lock().unwrap()
    }

    /// Set the state to transformed
    pub fn set_transformed(&self) {
        *self.transformed.lock().unwrap() = true;
    }
}

struct CommutativeVisitor {
    next_stage: Vec<LogicalPlan>,
    // hash of the stop node
    stop_node: Option<u64>,
}

impl TreeNodeVisitor for CommutativeVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> datafusion_common::Result<VisitRecursion> {
        // find the first merge scan and stop traversing down
        // todo: check if it works for join
        Ok(match plan {
            LogicalPlan::Extension(ext) => {
                if ext.node.name() == MergeScanLogicalPlan::name() {
                    VisitRecursion::Skip
                } else {
                    VisitRecursion::Continue
                }
            }
            _ => VisitRecursion::Continue,
        })
    }

    fn post_visit(&mut self, plan: &LogicalPlan) -> datafusion_common::Result<VisitRecursion> {
        match Categorizer::check_plan(plan) {
            Commutativity::Commutative => {}
            Commutativity::PartialCommutative => {
                if let Some(plan) = partial_commutative_transformer(plan) {
                    self.next_stage.push(plan)
                }
            }
            Commutativity::ConditionalCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan) {
                    self.next_stage.push(plan)
                }
            },
            Commutativity::TransformedCommutative(transformer) => {
                if let Some(transformer) = transformer
                    && let Some(plan) = transformer(plan) {
                    self.next_stage.push(plan)
                }
            },
            Commutativity::NonCommutative
            | Commutativity::Unimplemented
            | Commutativity::Unsupported => {
                self.stop_node = Some(utils::hash_plan(plan));
                return Ok(VisitRecursion::Stop);
            }
        }

        Ok(VisitRecursion::Continue)
    }
}

impl CommutativeVisitor {
    pub fn new() -> Self {
        Self {
            next_stage: vec![],
            stop_node: None,
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{col, lit, LogicalPlanBuilder};
    use table::table::adapter::DfTableProviderAdapter;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[test]
    fn see_how_analyzer_works() {
        let numbers_table = Arc::new(NumbersTable::new(0)) as _;
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(numbers_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .filter(col("number").lt(lit(10)))
            .unwrap()
            .project(vec![col("number")])
            .unwrap()
            .distinct()
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
        let expected = String::from(
            "Distinct:\
            \n  MergeScan [is_placeholder=false]\
            \n    Distinct:\
            \n      Projection: t.number\
            \n        Filter: t.number < Int32(10)\
            \n          MergeScan [is_placeholder=true]\
            \n            TableScan: t",
        );
        assert_eq!(expected, format!("{:?}", result));
    }
}
