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
        format!("{self:?}")
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
