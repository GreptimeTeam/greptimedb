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

//! Partial aggregation of the manifest inventory on each datanode.

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;

use catalog::information_schema::{InformationExtensionRef, SSTS_MANIFEST};
use common_catalog::consts;
use common_function::aggrs::aggr_wrapper::{StateMergeHelper, get_aggr_func};
use datafusion::catalog::Session;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions_aggregate::{count::count_udaf, sum::sum_udaf};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion_optimizer::AnalyzerRule;
use store_api::sst_entry::ManifestSstEntry;
use store_api::storage::ScanRequest;
use table::table::adapter::DfTableProviderAdapter;

/// Splits supported ungrouped manifest aggregates before general distributed planning.
#[derive(Debug)]
pub(crate) struct SstManifestAggregateRule;

impl AnalyzerRule for SstManifestAggregateRule {
    fn name(&self) -> &str {
        "sst_manifest_aggregate"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|plan| {
                let LogicalPlan::Aggregate(aggregate) = &plan else {
                    return Ok(Transformed::no(plan));
                };
                if !aggregate.group_expr.is_empty()
                    || aggregate.aggr_expr.is_empty()
                    || !aggregate
                        .aggr_expr
                        .iter()
                        .all(|expr| is_supported_aggregate(expr, aggregate.input.schema()))
                    || !is_supported_input(&aggregate.input)
                {
                    return Ok(Transformed::no(plan));
                }
                let split = StateMergeHelper::split_aggr_node(aggregate.clone())?;
                let remote = split
                    .lower_state
                    .transform_up(|node| {
                        if let LogicalPlan::TableScan(scan) = node {
                            // An alias preserves the original qualified column bindings above the scan.
                            let reserved = ManifestSstEntry::build_plan(ScanRequest::default())?;
                            let replacement = LogicalPlanBuilder::from(reserved)
                                .alias(scan.table_name)?
                                .build()?;
                            Ok(Transformed::yes(replacement))
                        } else {
                            Ok(Transformed::no(node))
                        }
                    })?
                    .data;
                let leaf = LogicalPlan::Extension(Extension {
                    node: Arc::new(SstManifestAggregate { remote }),
                });
                let upper = split
                    .upper_merge
                    .with_new_exprs(split.upper_merge.expressions(), vec![leaf])?;
                Ok(Transformed::yes(upper))
            })?
            .data)
    }
}

/// Converts manifest state leaves into the catalog's distributed inspection plan.
pub(crate) struct SstManifestAggregatePlanner {
    extension: InformationExtensionRef,
}

impl SstManifestAggregatePlanner {
    /// Uses the same capability-checked extension as the analyzer registration.
    pub(crate) fn new(extension: InformationExtensionRef) -> Self {
        Self { extension }
    }
}

#[async_trait::async_trait]
impl ExtensionPlanner for SstManifestAggregatePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<SstManifestAggregate>() else {
            return Ok(None);
        };
        self.extension
            .inspect_datanode_query_plan(node.remote.clone())
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .map(Some)
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Manifest aggregation requires inspect query support".into(),
                )
            })
    }
}

/// Carries remote aggregate states without exposing them to frontend optimizer rewrites.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
struct SstManifestAggregate {
    remote: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SstManifestAggregate {
    fn name(&self) -> &str {
        "SstManifestAggregate"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.remote.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SstManifestAggregate: {}", self.remote.display())
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return Err(DataFusionError::Internal(
                "SstManifestAggregate is a leaf".into(),
            ));
        }
        Ok(self.clone())
    }
}

fn is_supported_aggregate(expr: &Expr, schema: &DFSchema) -> bool {
    let Some(function) = get_aggr_func(expr) else {
        return false;
    };
    let params = &function.params;
    if params.distinct || !params.order_by.is_empty() || params.null_treatment.is_some() {
        return false;
    }
    if params
        .filter
        .as_deref()
        .is_some_and(|filter| !is_supported_predicate(filter))
    {
        return false;
    }
    if function.func == count_udaf() {
        matches!(
            params.args.as_slice(),
            [Expr::Column(_)]
                | [Expr::Literal(
                    ScalarValue::Int64(Some(1))
                        | ScalarValue::UInt64(Some(1))
                        | ScalarValue::Int32(Some(1))
                        | ScalarValue::UInt8(Some(1)),
                    _
                )]
        )
    } else if function.func == sum_udaf() {
        matches!(params.args.as_slice(), [arg] if is_numeric_column(arg, schema))
    } else {
        false
    }
}

// SUM coerces narrow numeric columns before this rule runs. Keep those casts in
// the remote plan while rejecting computed aggregate arguments.
fn is_numeric_column(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::Column(_) => expr
            .get_type(schema)
            .is_ok_and(|data_type| data_type.is_numeric()),
        Expr::Cast(cast) if cast.field.data_type().is_numeric() => {
            is_numeric_column(&cast.expr, schema)
        }
        _ => false,
    }
}

fn is_supported_predicate(expr: &Expr) -> bool {
    let mut supported = true;
    let visited = expr.apply(|expr| {
        // The SQL parser represents TIMESTAMP_NS literals as arrow_cast calls.
        // Only this builtin with constant arguments is a supported scalar call.
        if let Expr::ScalarFunction(function) = expr
            && function.func == datafusion::functions::core::arrow_cast()
            && function
                .args
                .iter()
                .all(|arg| matches!(arg, Expr::Literal(_, _)))
        {
            return Ok(TreeNodeRecursion::Jump);
        }
        if !matches!(
            expr,
            Expr::Column(_)
                | Expr::Literal(_, _)
                | Expr::BinaryExpr(_)
                | Expr::Cast(_)
                | Expr::TryCast(_)
                | Expr::Not(_)
                | Expr::IsNull(_)
                | Expr::IsNotNull(_)
                | Expr::Between(_)
                | Expr::InList(_)
        ) {
            supported = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    visited.is_ok() && supported
}

fn is_supported_input(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(filter) => {
            is_supported_predicate(&filter.predicate) && is_supported_input(&filter.input)
        }
        LogicalPlan::SubqueryAlias(alias) => is_supported_input(&alias.input),
        LogicalPlan::TableScan(scan) => {
            if scan.fetch.is_some() || scan.projection.is_some() || !scan.filters.is_empty() {
                return false;
            }
            let Some(source) = scan.source.downcast_ref::<DefaultTableSource>() else {
                return false;
            };
            let Some(adapter) = source
                .table_provider
                .downcast_ref::<DfTableProviderAdapter>()
            else {
                return false;
            };
            let info = adapter.table().table_info();
            info.ident.table_id == consts::INFORMATION_SCHEMA_SSTS_MANIFEST_TABLE_ID
                && info.schema_name == consts::INFORMATION_SCHEMA_NAME
                && info.name == SSTS_MANIFEST
                && info.meta.schema.as_ref() == ManifestSstEntry::schema().as_ref()
        }
        _ => false,
    }
}
