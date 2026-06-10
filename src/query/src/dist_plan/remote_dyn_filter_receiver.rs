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

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::utils::conjunction;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::utils::collect_columns;
use session::context::QueryContextRef;

type InjectRemoteDynFilterReceiver =
    dyn Fn(LogicalPlan, QueryContextRef) -> LogicalPlan + Send + Sync + 'static;

/// Injects a logical remote dynamic filter receiver into a query plan.
pub struct RemoteDynFilterReceiverInjector {
    inject: Box<InjectRemoteDynFilterReceiver>,
}

impl RemoteDynFilterReceiverInjector {
    pub fn new(
        inject: impl Fn(LogicalPlan, QueryContextRef) -> LogicalPlan + Send + Sync + 'static,
    ) -> Self {
        Self {
            inject: Box::new(inject),
        }
    }

    pub fn maybe_inject(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> LogicalPlan {
        (self.inject)(plan, query_ctx)
    }
}

pub type RemoteDynFilterReceiverInjectorRef = Arc<RemoteDynFilterReceiverInjector>;

/// A logical marker that is converted to a [`FilterExec`] carrying remote dynamic filters.
#[derive(Clone)]
pub struct RemoteDynFilterReceiverLogicalPlan {
    input: Arc<LogicalPlan>,
    dyn_filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl RemoteDynFilterReceiverLogicalPlan {
    pub fn new(input: LogicalPlan, dyn_filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            input: Arc::new(input),
            dyn_filters,
        }
    }

    pub fn name() -> &'static str {
        "RemoteDynFilterReceiver"
    }

    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    fn dyn_filters(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.dyn_filters
    }

    fn ord_key(&self) -> String {
        format!("input={:?}, dyn_filters={:?}", self.input, self.dyn_filters)
    }
}

impl fmt::Debug for RemoteDynFilterReceiverLogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl Hash for RemoteDynFilterReceiverLogicalPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.dyn_filters.hash(state);
    }
}

impl PartialEq for RemoteDynFilterReceiverLogicalPlan {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input && self.dyn_filters == other.dyn_filters
    }
}

impl Eq for RemoteDynFilterReceiverLogicalPlan {}

impl PartialOrd for RemoteDynFilterReceiverLogicalPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ord_key().cmp(&other.ord_key()))
    }
}

impl UserDefinedLogicalNodeCore for RemoteDynFilterReceiverLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let mut required = output_columns.to_vec();

        for filter in &self.dyn_filters {
            required.extend(
                collect_columns(filter)
                    .into_iter()
                    .map(|column| column.index()),
            );
        }

        required.sort_unstable();
        required.dedup();
        Some(vec![required])
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: filters={}", Self::name(), self.dyn_filters.len())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let input = inputs.pop().ok_or_else(|| {
            DataFusionError::Internal(format!("Expected exactly one input with {}", Self::name()))
        })?;
        Ok(Self::new(input, self.dyn_filters.clone()))
    }
}

pub struct RemoteDynFilterReceiverExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for RemoteDynFilterReceiverExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(receiver) = node
            .as_any()
            .downcast_ref::<RemoteDynFilterReceiverLogicalPlan>()
        else {
            return Ok(None);
        };

        let input = physical_inputs.first().cloned().ok_or_else(|| {
            DataFusionError::Internal(format!("Expected exactly one input with {}", Self::name()))
        })?;
        if receiver.dyn_filters().is_empty() {
            return Ok(Some(input));
        }

        let predicate = conjunction(receiver.dyn_filters().to_vec());
        Ok(Some(Arc::new(FilterExec::try_new(predicate, input)?) as _))
    }
}

impl RemoteDynFilterReceiverExtensionPlanner {
    fn name() -> &'static str {
        RemoteDynFilterReceiverLogicalPlan::name()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::expressions::{Column, DynamicFilterPhysicalExpr, lit};
    use datafusion_common::DFSchema;
    use datafusion_expr::{EmptyRelation, UserDefinedLogicalNodeCore};

    use super::*;

    fn empty_input() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("instance", DataType::Utf8, true),
            Field::new("job", DataType::Utf8, true),
        ]));
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(schema).unwrap()),
        })
    }

    #[test]
    fn necessary_children_exprs_keeps_parent_and_filter_columns() {
        let dyn_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("ts", 0)) as Arc<_>],
            lit(true) as _,
        ));
        let plan = RemoteDynFilterReceiverLogicalPlan::new(empty_input(), vec![dyn_filter]);

        // Parent only needs `value`, but the receiver must still keep `ts` for
        // evaluating its dynamic filter after logical projection pruning.
        let required = UserDefinedLogicalNodeCore::necessary_children_exprs(&plan, &[1]).unwrap();
        assert_eq!(required, vec![vec![0, 1]]);
    }

    #[test]
    fn necessary_children_exprs_is_transparent_without_filters() {
        let plan = RemoteDynFilterReceiverLogicalPlan::new(empty_input(), vec![]);

        let required =
            UserDefinedLogicalNodeCore::necessary_children_exprs(&plan, &[1, 3]).unwrap();
        assert_eq!(required, vec![vec![1, 3]]);
    }
}
