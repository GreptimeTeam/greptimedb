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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DataFusionError, Result, ScalarValue, TableReference};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, LogicalTableSource, Operator, TableSource,
    UserDefinedLogicalNodeCore,
};

use crate::dist_plan::MergeScanLogicalPlan;
use crate::research_series_grid::online_plan::OnlineNode;
use crate::research_series_grid::{SeriesFragment, SeriesInstantMax, SeriesRange};

/// Contains no catalog provider or execution state. Dependency validation and
/// cache scope belong to the caller; rebinding still checks the source schemas.
#[derive(Debug, Clone)]
pub(crate) struct Template {
    plan: LogicalPlan,
    time_columns: Arc<HashSet<Column>>,
    program: Option<Arc<crate::research_series_grid::query_program::QueryProgram>>,
    program_key: Arc<str>,
}

impl Template {
    pub(crate) fn new(plan: LogicalPlan, time_columns: HashSet<Column>) -> Result<Self> {
        let program = if std::env::var_os("GT_RESEARCH_QUERY_PROGRAM").is_some() {
            crate::research_series_grid::query_program::QueryProgram::compile(&plan)?.map(Arc::new)
        } else {
            None
        };
        let program_key: Arc<str> = program.as_ref().map_or_else(
            || Arc::from(""),
            |program| Arc::from(format!("{program:?}")),
        );
        let mut sources = HashMap::new();
        detached_sources(&plan, &mut sources, &mut 0)?;
        Ok(Self {
            plan: rebind(plan, 0, &sources, &time_columns)?,
            time_columns: Arc::new(time_columns),
            program,
            program_key,
        })
    }

    pub(crate) fn bind(
        &self,
        delta: i64,
        sources: &HashMap<TableReference, Arc<dyn TableSource>>,
    ) -> Result<LogicalPlan> {
        if let Some(program) = &self.program {
            return crate::research_series_grid::query_program::ProgramNode::bind(
                program,
                self.program_key.clone(),
                self.plan.clone(),
                self.time_columns.clone(),
                delta,
                sources,
            );
        }
        rebind(self.plan.clone(), delta, sources, &self.time_columns)
    }
}

pub(crate) fn current_sources(
    plan: &LogicalPlan,
) -> Result<HashMap<TableReference, Arc<dyn TableSource>>> {
    fn visit(
        plan: &LogicalPlan,
        sources: &mut HashMap<TableReference, Arc<dyn TableSource>>,
    ) -> Result<()> {
        if let LogicalPlan::TableScan(scan) = plan {
            if let Some(previous) = sources.insert(scan.table_name.clone(), scan.source.clone())
                && previous.schema() != scan.source.schema()
            {
                return Err(invalid("Conflicting initial program source schemas"));
            }
        }
        if let LogicalPlan::Extension(ext) = plan
            && let Some(remote) = ext.node.as_any().downcast_ref::<MergeScanLogicalPlan>()
        {
            visit(remote.input(), sources)?;
        }
        for input in plan.inputs() {
            visit(input, sources)?;
        }
        Ok(())
    }
    let mut sources = HashMap::new();
    visit(plan, &mut sources)?;
    Ok(sources)
}

fn detached_sources(
    plan: &LogicalPlan,
    sources: &mut HashMap<TableReference, Arc<dyn TableSource>>,
    nodes: &mut usize,
) -> Result<()> {
    *nodes += 1;
    if *nodes > 64 {
        return Err(invalid("Series template exceeds node limit"));
    }
    if let LogicalPlan::TableScan(scan) = plan {
        let mut source = LogicalTableSource::new(scan.source.schema());
        if let Some(constraints) = scan.source.constraints() {
            source = source.with_constraints(constraints.clone());
        }
        if let Some(previous) = sources.insert(scan.table_name.clone(), Arc::new(source))
            && previous.schema() != scan.source.schema()
        {
            return Err(invalid("Conflicting table schemas in series template"));
        }
    }
    if let LogicalPlan::Extension(ext) = plan
        && let Some(remote) = ext.node.as_any().downcast_ref::<MergeScanLogicalPlan>()
    {
        detached_sources(remote.input(), sources, nodes)?;
    }
    for input in plan.inputs() {
        detached_sources(input, sources, nodes)?;
    }
    Ok(())
}

pub(crate) fn shift(time: i64, delta: i64) -> Result<i64> {
    time.checked_add(delta)
        .ok_or_else(|| DataFusionError::Plan("Series plan template timestamp overflow".into()))
}

// These bounds are not logical expressions. The caller must separately rebind
// scan predicates and fresh table sources, including hidden MergeScan inputs.
pub(crate) fn shift_extension(ext: &Extension, delta: i64) -> Result<Option<LogicalPlan>> {
    let node = ext.node.as_any();
    let node: Arc<dyn datafusion_expr::UserDefinedLogicalNode> =
        if let Some(node) = node.downcast_ref::<SeriesRange>() {
            Arc::new(node.shift_time(delta)?)
        } else if let Some(node) = node.downcast_ref::<SeriesFragment>() {
            Arc::new(node.shift_time(delta)?)
        } else if let Some(node) = node.downcast_ref::<SeriesInstantMax>() {
            Arc::new(node.shift_time(delta)?)
        } else if let Some(node) = node.downcast_ref::<OnlineNode>() {
            return node.shift_time(delta).map(Some);
        } else {
            return Ok(None);
        };
    Ok(Some(LogicalPlan::Extension(Extension { node })))
}

/// Rebinds an admitted template. Sources must come from this request's catalog
/// resolution, after checking both logical and physical table dependencies.
pub(crate) fn rebind(
    plan: LogicalPlan,
    delta: i64,
    sources: &HashMap<TableReference, Arc<dyn TableSource>>,
    time_columns: &HashSet<Column>,
) -> Result<LogicalPlan> {
    Ok(plan
        .transform_up(|plan| {
            let carries_time_filter =
                matches!(plan, LogicalPlan::TableScan(_) | LogicalPlan::Filter(_));
            let plan = match plan {
                LogicalPlan::TableScan(mut scan) => {
                    let source = sources
                        .get(&scan.table_name)
                        .ok_or_else(|| invalid("Missing fresh table source for series template"))?;
                    if source.schema() != scan.source.schema() {
                        return Err(invalid("Series template source schema changed"));
                    }
                    scan.source = if let Some(source) =
                        source.downcast_ref::<datafusion::datasource::DefaultTableSource>()
                        && let Some(adapter) = source
                            .table_provider
                            .downcast_ref::<table::table::adapter::DfTableProviderAdapter>(
                        ) {
                        datafusion::datasource::provider_as_source(Arc::new(
                            table::table::adapter::DfTableProviderAdapter::new(adapter.table()),
                        ))
                    } else {
                        source.clone()
                    };
                    LogicalPlan::TableScan(scan)
                }
                LogicalPlan::Extension(ext) => {
                    if let Some(remote) = ext.node.as_any().downcast_ref::<MergeScanLogicalPlan>() {
                        // MergeScan hides its region plan from ordinary tree traversal.
                        let input = rebind(remote.input().clone(), delta, sources, time_columns)?;
                        let mut rebound = MergeScanLogicalPlan::new(
                            input,
                            remote.is_placeholder(),
                            remote.partition_cols().clone(),
                        )
                        .with_output_schema(remote.schema().clone());
                        if let Some(id) = remote.remote_dyn_filter_producer_id() {
                            rebound = rebound.with_remote_dyn_filter_producer_id(id);
                        }
                        rebound.into_logical_plan()
                    } else {
                        shift_extension(&ext, delta)?
                            .ok_or_else(|| invalid("Unsupported series template extension"))?
                    }
                }
                plan @ (LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::SubqueryAlias(_)) => plan,
                LogicalPlan::Sort(sort) if sort.fetch.is_none() => LogicalPlan::Sort(sort),
                _ => return Err(invalid("Unsupported series template plan")),
            };
            let rebound = plan
                .map_expressions(|expr| {
                    validate_expression(&expr, carries_time_filter, false, time_columns)?;
                    expr.transform_up(|expr| {
                        if let Expr::Literal(
                            ScalarValue::TimestampMillisecond(Some(time), None),
                            metadata,
                        ) = expr
                        {
                            Ok(Transformed::yes(Expr::Literal(
                                ScalarValue::TimestampMillisecond(Some(shift(time, delta)?), None),
                                metadata,
                            )))
                        } else {
                            Ok(Transformed::no(expr))
                        }
                    })
                })?
                .data;
            Ok(Transformed::yes(rebound))
        })?
        .data)
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

fn validate_expression(
    expr: &Expr,
    filter: bool,
    timestamp: bool,
    time_columns: &HashSet<Column>,
) -> Result<()> {
    match expr {
        Expr::Literal(value, metadata) => {
            if metadata.is_some() {
                return Err(invalid("Literal metadata in series template"));
            }
            match value {
                ScalarValue::TimestampMillisecond(Some(_), None) if timestamp => {}
                ScalarValue::Null
                | ScalarValue::Boolean(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Float32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int32(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::Utf8(_)
                | ScalarValue::LargeUtf8(_)
                | ScalarValue::Utf8View(_) => {}
                ScalarValue::Dictionary(key, value)
                    if key.as_ref() == &datafusion::arrow::datatypes::DataType::UInt32
                        && matches!(value.as_ref(), ScalarValue::Utf8(_)) => {}
                _ => return Err(invalid("Unsupported literal in series template")),
            }
        }
        Expr::BinaryExpr(binary) => {
            let comparison = matches!(
                binary.op,
                Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq
            );
            let is_time =
                |expr: &Expr| matches!(expr, Expr::Column(column) if time_columns.contains(column));
            let left = filter && comparison && is_time(&binary.right);
            let right = filter && comparison && is_time(&binary.left);
            let literal = |expr: &Expr| {
                matches!(
                    expr,
                    Expr::Literal(ScalarValue::TimestampMillisecond(Some(_), None), None)
                )
            };
            if (left && !literal(&binary.left)) || (right && !literal(&binary.right)) {
                return Err(invalid("Nonliteral time bound in series template"));
            }
            validate_expression(&binary.left, filter, left, time_columns)?;
            return validate_expression(&binary.right, filter, right, time_columns);
        }
        Expr::Column(_)
        | Expr::Alias(_)
        | Expr::Cast(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::Not(_)
        | Expr::Negative(_) => {}
        // In particular, never retain request-bound UDF annotation collectors.
        _ => return Err(invalid("Unsupported expression in series template")),
    }
    expr.apply_children(|child| {
        validate_expression(child, filter, false, time_columns)?;
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_expr::{LogicalPlanBuilder, col, lit};

    use super::*;

    fn source() -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("tag", DataType::Utf8, true),
        ]))))
    }

    fn time(value: i64) -> Expr {
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(value), None), None)
    }

    #[test]
    fn rebind_enters_hidden_region_plan_and_replaces_request_source() {
        let old = source();
        let fresh = source();
        let name = TableReference::bare("physical");
        let plan = LogicalPlanBuilder::scan(name.clone(), old.clone(), None)
            .unwrap()
            .alias("m")
            .unwrap()
            .filter(col("m.ts").gt(time(10)).and(col("m.tag").eq(lit("10"))))
            .unwrap()
            .sort(vec![
                col("m.tag").sort(true, true),
                col("m.ts").sort(true, true),
            ])
            .unwrap()
            .build()
            .unwrap();
        let plan = MergeScanLogicalPlan::new(plan, false, Default::default()).into_logical_plan();
        let sources = HashMap::from([(name, fresh.clone())]);
        let columns = HashSet::from([Column::new(Some("m"), "ts")]);
        let template = Template::new(plan.clone(), columns.clone()).unwrap();
        let rebound = template.bind(50, &sources).unwrap();
        let LogicalPlan::Extension(extension) = &rebound else {
            unreachable!()
        };
        let remote = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
            .unwrap();
        let LogicalPlan::Sort(sort) = remote.input() else {
            unreachable!()
        };
        assert_eq!(
            sort.expr,
            vec![col("m.tag").sort(true, true), col("m.ts").sort(true, true)]
        );
        let LogicalPlan::Filter(filter) = sort.input.as_ref() else {
            unreachable!()
        };
        assert_eq!(
            filter.predicate,
            col("m.ts").gt(time(60)).and(col("m.tag").eq(lit("10")))
        );
        let LogicalPlan::SubqueryAlias(alias) = filter.input.as_ref() else {
            unreachable!()
        };
        assert_eq!(alias.alias, TableReference::bare("m"));
        let LogicalPlan::TableScan(scan) = alias.input.as_ref() else {
            unreachable!()
        };
        assert!(Arc::ptr_eq(&scan.source, &fresh));
        assert!(!Arc::ptr_eq(&scan.source, &old));
        assert_eq!(rebind(rebound, -50, &sources, &columns).unwrap(), plan);
        assert!(rebind(plan.clone(), 0, &HashMap::new(), &columns).is_err());
        let incompatible: Arc<dyn TableSource> =
            Arc::new(LogicalTableSource::new(Arc::new(Schema::empty())));
        assert!(
            rebind(
                plan,
                0,
                &HashMap::from([(TableReference::bare("physical"), incompatible)]),
                &columns
            )
            .is_err()
        );
    }

    #[test]
    fn template_releases_original_provider_and_binds_multiple_requests() {
        let original = source();
        let plan = LogicalPlanBuilder::scan("m", original.clone(), None)
            .unwrap()
            .filter(col("m.ts").gt(time(10)))
            .unwrap()
            .build()
            .unwrap();
        let template = Template::new(plan, HashSet::from([Column::new(Some("m"), "ts")])).unwrap();
        assert_eq!(Arc::strong_count(&original), 1);
        for delta in [-20, 30] {
            let fresh = source();
            let rebound = template
                .bind(
                    delta,
                    &HashMap::from([(TableReference::bare("m"), fresh.clone())]),
                )
                .unwrap();
            let LogicalPlan::Filter(filter) = rebound else {
                unreachable!()
            };
            assert_eq!(filter.predicate, col("m.ts").gt(time(10 + delta)));
            let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
                unreachable!()
            };
            assert!(Arc::ptr_eq(&scan.source, &fresh));
        }
        assert_eq!(Arc::strong_count(&original), 1);
    }

    #[test]
    fn dictionary_label_literals_survive_time_rebinding() {
        let label = Expr::Literal(
            ScalarValue::Dictionary(
                Box::new(DataType::UInt32),
                Box::new(ScalarValue::Utf8(Some("10".into()))),
            ),
            None,
        );
        let plan = LogicalPlanBuilder::scan("m", source(), None)
            .unwrap()
            .filter(col("m.ts").gt(time(10)).and(col("m.tag").eq(label.clone())))
            .unwrap()
            .build()
            .unwrap();
        let template = Template::new(plan, HashSet::from([Column::new(Some("m"), "ts")])).unwrap();
        let rebound = template
            .bind(50, &HashMap::from([(TableReference::bare("m"), source())]))
            .unwrap();
        let LogicalPlan::Filter(filter) = rebound else {
            unreachable!()
        };
        assert_eq!(
            filter.predicate,
            col("m.ts").gt(time(60)).and(col("m.tag").eq(label))
        );
    }

    #[test]
    fn timestamps_are_bindable_only_as_direct_time_index_bounds() {
        let columns = HashSet::from([Column::new(Some("m"), "ts")]);
        assert!(validate_expression(&col("m.ts").gt(time(10)), true, false, &columns).is_ok());
        assert!(validate_expression(&time(10).lt_eq(col("m.ts")), true, false, &columns).is_ok());
        for expr in [
            col("m.tag").eq(time(10)),
            col("other.ts").gt(time(10)),
            col("m.ts").gt(lit("10")),
            col("m.ts").gt(time(10) + lit(1_i64)),
            time(10).alias("constant"),
        ] {
            assert!(
                validate_expression(&expr, true, false, &columns).is_err(),
                "{expr}"
            );
        }
        assert!(validate_expression(&col("m.ts").gt(time(10)), false, false, &columns).is_err());
    }
}
