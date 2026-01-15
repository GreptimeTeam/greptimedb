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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow_schema::{ArrowError, DataType};
use chrono::{DateTime, Utc};
use datafusion::common::alias::AliasGenerator;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion_common::Column;
use datafusion_common::tree_node::{Transformed, TreeNode as _, TreeNodeRewriter};
use datafusion_expr::expr::Alias;
use datafusion_expr::{Expr, Extension, LogicalPlan};
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule as _};

use crate::dist_plan::merge_sort::MergeSortLogicalPlan;
use crate::plan::ExtractExpr as _;

/// The `ConstEvaluator` in `SimplifyExpressions` might evaluate some UDFs early in the
/// planning stage, by executing them directly. For example, the `database()` function.
/// So the `ConfigOptions` here (which is set from the session context) should be present
/// in the UDF's `ScalarFunctionArgs`. However, the default implementation in DataFusion
/// seems to lost track on it: the `ConfigOptions` is recreated with its default values again.
/// So we create a custom `OptimizerConfig` with the desired `ConfigOptions`
/// to walk around the issue.
/// TODO(LFC): Maybe use DataFusion's `OptimizerContext` again
///   once https://github.com/apache/datafusion/pull/17742 is merged.
pub(crate) struct PatchOptimizerContext {
    pub(crate) inner: datafusion_optimizer::OptimizerContext,
    pub(crate) config: Arc<ConfigOptions>,
}

impl OptimizerConfig for PatchOptimizerContext {
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        self.inner.query_execution_start_time()
    }

    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        self.inner.alias_generator()
    }

    fn options(&self) -> Arc<ConfigOptions> {
        self.config.clone()
    }
}

/// Simplify all expressions recursively in the plan tree
/// which keeping the output schema unchanged
pub(crate) struct PlanTreeExpressionSimplifier {
    optimizer_context: PatchOptimizerContext,
}

impl PlanTreeExpressionSimplifier {
    pub fn new(optimizer_context: PatchOptimizerContext) -> Self {
        Self { optimizer_context }
    }
}

impl TreeNodeRewriter for PlanTreeExpressionSimplifier {
    type Node = LogicalPlan;
    fn f_down(&mut self, plan: Self::Node) -> DfResult<Transformed<Self::Node>> {
        let simp = SimplifyExpressions::new()
            .rewrite(plan, &self.optimizer_context)?
            .data;
        Ok(Transformed::yes(simp))
    }
}

/// A patch for substrait simply throw timezone away, so when decoding, if columns have different timezone then expected schema, use expected schema's timezone
pub fn patch_batch_timezone(
    expected_schema: arrow_schema::SchemaRef,
    columns: Vec<ArrayRef>,
) -> Result<arrow::record_batch::RecordBatch, ArrowError> {
    let patched_columns: Vec<ArrayRef> = expected_schema
        .fields()
        .iter()
        .zip(columns.into_iter())
        .map(|(expected_field, column)| {
            let expected_type = expected_field.data_type();
            let actual_type = column.data_type();

            // Check if both are timestamp types with different timezones
            match (expected_type, actual_type) {
                (
                    DataType::Timestamp(expected_unit, expected_tz),
                    DataType::Timestamp(actual_unit, actual_tz),
                ) if expected_unit == actual_unit && expected_tz != actual_tz => {
                    // Cast the column to the expected timezone
                    arrow::compute::cast(&column, expected_type)
                }
                _ => Ok(column),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    arrow::record_batch::RecordBatch::try_new(expected_schema.clone(), patched_columns)
}

fn rewrite_column(
    mapping: &BTreeMap<Column, BTreeSet<Column>>,
    original_node: &LogicalPlan,
    alias_node: &LogicalPlan,
) -> impl Fn(Expr) -> DfResult<Transformed<Expr>> {
    move |e: Expr| {
        if let Expr::Column(col) = e {
            if let Some(aliased_cols) = mapping.get(&col) {
                // if multiple alias is available, just use first one
                if let Some(aliased_col) = aliased_cols.iter().next() {
                    Ok(Transformed::yes(Expr::Column(aliased_col.clone())))
                } else {
                    Err(datafusion_common::DataFusionError::Internal(format!(
                        "PlanRewriter: expand: column {col} from {original_node}\n has empty alias set in plan: {alias_node}\n but expect at least one alias",
                    )))
                }
            } else {
                Err(datafusion_common::DataFusionError::Internal(format!(
                    "PlanRewriter: expand: column {col} from {original_node}\n has no alias in plan: {alias_node}",
                )))
            }
        } else {
            Ok(Transformed::no(e))
        }
    }
}

/// Rewrite the expressions of the given merge sort plan from original columns(at merge sort's input plan) to aliased columns at the given aliased node
pub fn rewrite_merge_sort_exprs(
    merge_sort: &MergeSortLogicalPlan,
    aliased_node: &LogicalPlan,
) -> DfResult<LogicalPlan> {
    let merge_sort = LogicalPlan::Extension(Extension {
        node: Arc::new(merge_sort.clone()),
    });

    // tracking alias for sort exprs,
    let sort_input = merge_sort.inputs().first().cloned().ok_or_else(|| {
        datafusion_common::DataFusionError::Internal(format!(
            "PlanRewriter: expand: merge sort stage has no input: {merge_sort}"
        ))
    })?;
    let sort_exprs = merge_sort.expressions_consider_join();
    let column_refs = sort_exprs
        .iter()
        .flat_map(|e| e.column_refs().into_iter().cloned())
        .collect::<BTreeSet<_>>();
    let column_alias_mapping = aliased_columns_for(&column_refs, aliased_node, Some(sort_input))?;
    let aliased_sort_exprs = sort_exprs
        .into_iter()
        .map(|e| {
            e.transform(rewrite_column(
                &column_alias_mapping,
                &merge_sort,
                aliased_node,
            ))
        })
        .map(|e| e.map(|e| e.data))
        .collect::<DfResult<Vec<_>>>()?;
    let new_merge_sort = merge_sort.with_new_exprs(
        aliased_sort_exprs,
        merge_sort.inputs().into_iter().cloned().collect(),
    )?;
    Ok(new_merge_sort)
}

/// Return all the original columns(at original node) for the given aliased columns at the aliased node
///
/// if `original_node` is None, it means original columns are from leaf node
///
/// Return value use `BTreeMap` to have deterministic order for choose first alias when multiple alias exist
#[allow(unused)]
pub fn original_column_for(
    aliased_columns: &BTreeSet<Column>,
    aliased_node: LogicalPlan,
    original_node: Option<Arc<LogicalPlan>>,
) -> DfResult<BTreeMap<Column, Column>> {
    let schema_cols: BTreeSet<Column> = aliased_node.schema().columns().iter().cloned().collect();
    let cur_aliases: BTreeMap<Column, Column> = aliased_columns
        .iter()
        .filter(|c| schema_cols.contains(c))
        .map(|c| (c.clone(), c.clone()))
        .collect();

    if cur_aliases.is_empty() {
        return Ok(BTreeMap::new());
    }

    original_column_for_inner(cur_aliases, &aliased_node, &original_node)
}

fn original_column_for_inner(
    mut cur_aliases: BTreeMap<Column, Column>,
    node: &LogicalPlan,
    original_node: &Option<Arc<LogicalPlan>>,
) -> DfResult<BTreeMap<Column, Column>> {
    let mut current_node = node;

    loop {
        // Base case: check if we've reached the target node
        if let Some(original_node) = original_node
            && *current_node == **original_node
        {
            return Ok(cur_aliases);
        } else if current_node.inputs().is_empty() {
            // leaf node reached
            return Ok(cur_aliases);
        }

        // Validate node has exactly one child
        if current_node.inputs().len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "only accept plan with at most one child, found: {}",
                current_node
            )));
        }

        // Get alias layer and update aliases
        let layer = get_alias_layer_from_node(current_node)?;
        let mut new_aliases = BTreeMap::new();
        for (start_alias, cur_alias) in cur_aliases {
            if let Some(old_column) = layer.get_old_from_new(cur_alias.clone()) {
                new_aliases.insert(start_alias, old_column);
            }
        }

        // Move to child node and continue iteration
        cur_aliases = new_aliases;
        current_node = current_node.inputs()[0];
    }
}

/// Return all the aliased columns(at aliased node) for the given original columns(at original node)
///
/// if `original_node` is None, it means original columns are from leaf node
///
/// Return value use `BTreeMap` to have deterministic order for choose first alias when multiple alias exist
pub fn aliased_columns_for(
    original_columns: &BTreeSet<Column>,
    aliased_node: &LogicalPlan,
    original_node: Option<&LogicalPlan>,
) -> DfResult<BTreeMap<Column, BTreeSet<Column>>> {
    let initial_aliases: BTreeMap<Column, BTreeSet<Column>> = {
        if let Some(original) = &original_node {
            let schema_cols: BTreeSet<Column> = original.schema().columns().into_iter().collect();
            original_columns
                .iter()
                .filter(|c| schema_cols.contains(c))
                .map(|c| (c.clone(), [c.clone()].into()))
                .collect()
        } else {
            original_columns
                .iter()
                .map(|c| (c.clone(), [c.clone()].into()))
                .collect()
        }
    };

    if initial_aliases.is_empty() {
        return Ok(BTreeMap::new());
    }

    aliased_columns_for_inner(initial_aliases, aliased_node, original_node)
}

fn aliased_columns_for_inner(
    cur_aliases: BTreeMap<Column, BTreeSet<Column>>,
    node: &LogicalPlan,
    original_node: Option<&LogicalPlan>,
) -> DfResult<BTreeMap<Column, BTreeSet<Column>>> {
    // First, collect the path from current node to the target node
    let mut path = Vec::new();
    let mut current_node = node;

    // Descend to the target node, collecting nodes along the way
    loop {
        // Base case: check if we've reached the target node
        if let Some(original_node) = original_node
            && *current_node == *original_node
        {
            break;
        } else if current_node.inputs().is_empty() {
            // leaf node reached
            break;
        }

        // Validate node has exactly one child
        if current_node.inputs().len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "only accept plan with at most one child, found: {}",
                current_node
            )));
        }

        // Add current node to path and move to child
        path.push(current_node);
        current_node = current_node.inputs()[0];
    }

    // Now apply alias layers in reverse order (from original to aliased)
    let mut result = cur_aliases;
    for &node_in_path in path.iter().rev() {
        let layer = get_alias_layer_from_node(node_in_path)?;
        let mut new_aliases = BTreeMap::new();
        for (original_column, cur_alias_set) in result {
            let mut new_alias_set = BTreeSet::new();
            for cur_alias in cur_alias_set {
                new_alias_set.extend(layer.get_new_from_old(cur_alias.clone()));
            }
            if !new_alias_set.is_empty() {
                new_aliases.insert(original_column, new_alias_set);
            }
        }
        result = new_aliases;
    }

    Ok(result)
}

/// Return a mapping of original column to all the aliased columns in current node of the plan
/// TODO(discord9): also support merge scan node
fn get_alias_layer_from_node(node: &LogicalPlan) -> DfResult<AliasLayer> {
    match node {
        LogicalPlan::Projection(proj) => Ok(get_alias_layer_from_exprs(&proj.expr)),
        LogicalPlan::Aggregate(aggr) => Ok(get_alias_layer_from_exprs(&aggr.group_expr)),
        LogicalPlan::SubqueryAlias(subquery_alias) => {
            let mut layer = AliasLayer::default();
            let old_columns = subquery_alias.input.schema().columns();
            for old_column in old_columns {
                let new_column = Column::new(
                    Some(subquery_alias.alias.clone()),
                    old_column.name().to_string(),
                );
                // mapping from old_column to new_column
                layer.insert_alias(old_column, [new_column].into());
            }
            Ok(layer)
        }
        LogicalPlan::TableScan(scan) => {
            let columns = scan.projected_schema.columns();
            let mut layer = AliasLayer::default();
            for col in columns {
                layer.insert_alias(col.clone(), [col.clone()].into());
            }
            Ok(layer)
        }
        _ => {
            let input_schema = node
                .inputs()
                .first()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "only accept plan with at most one child, found: {}",
                        node
                    ))
                })?
                .schema();
            let output_schema = node.schema();
            // only accept at most one child plan, and if not one of the above nodes,
            // also shouldn't modify the schema or else alias scope tracker can't support them
            if node.inputs().len() > 1 {
                Err(datafusion::error::DataFusionError::Internal(format!(
                    "only accept plan with at most one child, found: {}",
                    node
                )))
            } else if node.inputs().len() == 1 {
                if input_schema != output_schema {
                    let input_columns = input_schema.columns();
                    let all_input_is_in_output = input_columns
                        .iter()
                        .all(|c| output_schema.is_column_from_schema(c));
                    if all_input_is_in_output {
                        // all input is in output, so it's just adding some columns, we can do identity mapping for input columns
                        let mut layer = AliasLayer::default();
                        for col in input_columns {
                            layer.insert_alias(col.clone(), [col.clone()].into());
                        }
                        Ok(layer)
                    } else {
                        // otherwise use the intersection of input and output
                        // TODO(discord9): maybe just make this case unsupported for now?
                        common_telemetry::debug!(
                            "Might be unsupported plan for alias tracking, track alias anyway: {}",
                            node
                        );
                        let input_columns = input_schema.columns();
                        let output_columns =
                            output_schema.columns().into_iter().collect::<HashSet<_>>();
                        let common_columns: HashSet<Column> = input_columns
                            .iter()
                            .filter(|c| output_columns.contains(c))
                            .cloned()
                            .collect();

                        let mut layer = AliasLayer::default();
                        for col in &common_columns {
                            layer.insert_alias(col.clone(), [col.clone()].into());
                        }
                        Ok(layer)
                    }
                } else {
                    // identity mapping
                    let mut layer = AliasLayer::default();
                    for col in output_schema.columns() {
                        layer.insert_alias(col.clone(), [col.clone()].into());
                    }
                    Ok(layer)
                }
            } else {
                // unknown plan with no input, error msg
                Err(datafusion::error::DataFusionError::Internal(format!(
                    "Unsupported plan with no input: {}",
                    node
                )))
            }
        }
    }
}

fn get_alias_layer_from_exprs(exprs: &[Expr]) -> AliasLayer {
    let mut alias_mapping: HashMap<Column, HashSet<Column>> = HashMap::new();
    for expr in exprs {
        if let Expr::Alias(alias) = expr {
            if let Some(column) = get_alias_original_column(alias) {
                alias_mapping
                    .entry(column.clone())
                    .or_default()
                    .insert(Column::new(alias.relation.clone(), alias.name.clone()));
            }
        } else if let Expr::Column(column) = expr {
            // identity mapping
            alias_mapping
                .entry(column.clone())
                .or_default()
                .insert(column.clone());
        }
    }
    let mut layer = AliasLayer::default();
    for (old_column, new_columns) in alias_mapping {
        layer.insert_alias(old_column, new_columns);
    }
    layer
}

#[derive(Default, Debug, Clone)]
struct AliasLayer {
    /// for convenient of querying, key is field's name
    old_to_new: BTreeMap<Column, HashSet<Column>>,
}

impl AliasLayer {
    pub fn insert_alias(&mut self, old_column: Column, new_columns: HashSet<Column>) {
        self.old_to_new
            .entry(old_column)
            .or_default()
            .extend(new_columns);
    }

    pub fn get_new_from_old(&self, old_column: Column) -> HashSet<Column> {
        let mut res_cols = HashSet::new();
        for (old, new_cols) in self.old_to_new.iter() {
            if old.name() == old_column.name() {
                match (&old.relation, &old_column.relation) {
                    (Some(o), Some(c)) => {
                        if o.resolved_eq(c) {
                            res_cols.extend(new_cols.clone());
                        }
                    }
                    _ => {
                        // if any of the two relation is None, meaning not fully qualified, just match name
                        res_cols.extend(new_cols.clone());
                    }
                }
            }
        }
        res_cols
    }

    pub fn get_old_from_new(&self, new_column: Column) -> Option<Column> {
        for (old, new_set) in &self.old_to_new {
            if new_set.iter().any(|n| {
                if n.name() != new_column.name() {
                    return false;
                }
                match (&n.relation, &new_column.relation) {
                    (Some(r1), Some(r2)) => r1.resolved_eq(r2),
                    _ => true,
                }
            }) {
                return Some(old.clone());
            }
        }
        None
    }
}

fn get_alias_original_column(alias: &Alias) -> Option<Column> {
    let mut cur_alias = alias;
    while let Expr::Alias(inner_alias) = cur_alias.expr.as_ref() {
        cur_alias = inner_alias;
    }
    if let Expr::Column(column) = cur_alias.expr.as_ref() {
        return Some(column.clone());
    }

    None
}

/// Mapping of original column in table to all the alias at current node
pub type AliasMapping = BTreeMap<String, BTreeSet<Column>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_telemetry::init_default_ut_logging;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::functions_aggregate::min_max::{max, min};
    use datafusion_expr::{LogicalPlanBuilder, col};
    use pretty_assertions::assert_eq;
    use table::table::adapter::DfTableProviderAdapter;

    use super::*;
    use crate::dist_plan::analyzer::test::TestTable;

    fn qcol(name: &str) -> Column {
        Column::from_qualified_name(name)
    }

    #[test]
    fn proj_multi_layered_alias_tracker() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "t".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .project(vec![
                col("number"),
                col("pk3").alias("pk1"),
                col("pk3").alias("pk2"),
            ])
            .unwrap()
            .project(vec![
                col("number"),
                col("pk2").alias("pk4"),
                col("pk1").alias("pk5"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let child = plan.inputs()[0].clone();

        assert_eq!(
            aliased_columns_for(&[qcol("pk1"), qcol("pk2")].into(), &plan, Some(&child)).unwrap(),
            [
                (qcol("pk1"), [qcol("pk5")].into()),
                (qcol("pk2"), [qcol("pk4")].into())
            ]
            .into()
        );

        // columns not in the plan should return empty mapping
        assert_eq!(
            aliased_columns_for(&[qcol("pk1"), qcol("pk2")].into(), &plan, Some(&plan)).unwrap(),
            [].into()
        );

        assert_eq!(
            aliased_columns_for(&[qcol("t.pk3")].into(), &plan, Some(&child)).unwrap(),
            [].into()
        );

        assert_eq!(
            original_column_for(&[qcol("pk5"), qcol("pk4")].into(), plan.clone(), None).unwrap(),
            [(qcol("pk5"), qcol("t.pk3")), (qcol("pk4"), qcol("t.pk3"))].into()
        );

        assert_eq!(
            aliased_columns_for(&[qcol("pk3")].into(), &plan, None).unwrap(),
            [(qcol("pk3"), [qcol("pk5"), qcol("pk4")].into())].into()
        );
        assert_eq!(
            original_column_for(&[qcol("pk1"), qcol("pk2")].into(), child.clone(), None).unwrap(),
            [(qcol("pk1"), qcol("t.pk3")), (qcol("pk2"), qcol("t.pk3"))].into()
        );

        assert_eq!(
            aliased_columns_for(&[qcol("pk3")].into(), &child, None).unwrap(),
            [(qcol("pk3"), [qcol("pk1"), qcol("pk2")].into())].into()
        );

        assert_eq!(
            original_column_for(
                &[qcol("pk4"), qcol("pk5")].into(),
                plan.clone(),
                Some(Arc::new(child.clone()))
            )
            .unwrap(),
            [(qcol("pk4"), qcol("pk2")), (qcol("pk5"), qcol("pk1"))].into()
        );
    }

    #[test]
    fn sort_subquery_alias_layered_tracker() {
        let test_table = TestTable::table_with_name(0, "t".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![col("t.number").sort(true, false)])
            .unwrap()
            .alias("a")
            .unwrap()
            .build()
            .unwrap();

        let sort_plan = plan.inputs()[0].clone();
        let scan_plan = sort_plan.inputs()[0].clone();

        // Test aliased_columns_for from scan to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("t.number")].into(), &plan, Some(&scan_plan)).unwrap(),
            [(qcol("t.number"), [qcol("a.number")].into())].into()
        );

        // Test aliased_columns_for from sort to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("t.number")].into(), &plan, Some(&sort_plan)).unwrap(),
            [(qcol("t.number"), [qcol("a.number")].into())].into()
        );

        // Test aliased_columns_for from leaf to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("t.number")].into(), &plan, None).unwrap(),
            [(qcol("t.number"), [qcol("a.number")].into())].into()
        );

        // Test original_column_for from final plan to scan
        assert_eq!(
            original_column_for(
                &[qcol("a.number")].into(),
                plan.clone(),
                Some(Arc::new(scan_plan.clone()))
            )
            .unwrap(),
            [(qcol("a.number"), qcol("t.number"))].into()
        );

        // Test original_column_for from final plan to sort
        assert_eq!(
            original_column_for(
                &[qcol("a.number")].into(),
                plan.clone(),
                Some(Arc::new(sort_plan.clone()))
            )
            .unwrap(),
            [(qcol("a.number"), qcol("t.number"))].into()
        );
    }

    #[test]
    fn proj_alias_layered_tracker() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "t".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .project(vec![
                col("number"),
                col("pk3").alias("pk1"),
                col("pk2").alias("pk3"),
            ])
            .unwrap()
            .project(vec![
                col("number"),
                col("pk1").alias("pk2"),
                col("pk3").alias("pk1"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let first_proj = plan.inputs()[0].clone();
        let scan_plan = first_proj.inputs()[0].clone();

        // Test original_column_for from final plan to scan
        assert_eq!(
            original_column_for(
                &[qcol("pk1")].into(),
                plan.clone(),
                Some(Arc::new(scan_plan.clone()))
            )
            .unwrap(),
            [(qcol("pk1"), qcol("t.pk2"))].into()
        );

        // Test original_column_for from final plan to first projection
        assert_eq!(
            original_column_for(
                &[qcol("pk1")].into(),
                plan.clone(),
                Some(Arc::new(first_proj.clone()))
            )
            .unwrap(),
            [(qcol("pk1"), qcol("pk3"))].into()
        );

        // Test original_column_for from final plan to leaf
        assert_eq!(
            original_column_for(
                &[qcol("pk1")].into(),
                plan.clone(),
                Some(Arc::new(plan.clone()))
            )
            .unwrap(),
            [(qcol("pk1"), qcol("pk1"))].into()
        );

        // Test aliased_columns_for from scan to first projection
        assert_eq!(
            aliased_columns_for(&[qcol("t.pk2")].into(), &first_proj, Some(&scan_plan)).unwrap(),
            [(qcol("t.pk2"), [qcol("pk3")].into())].into()
        );

        // Test aliased_columns_for from first projection to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("pk3")].into(), &plan, Some(&first_proj)).unwrap(),
            [(qcol("pk3"), [qcol("pk1")].into())].into()
        );

        // Test aliased_columns_for from scan to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("t.pk2")].into(), &plan, Some(&scan_plan)).unwrap(),
            [(qcol("t.pk2"), [qcol("pk1")].into())].into()
        );

        // Test aliased_columns_for from leaf to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("pk2")].into(), &plan, None).unwrap(),
            [(qcol("pk2"), [qcol("pk1")].into())].into()
        );
    }

    #[test]
    fn proj_alias_relation_layered_tracker() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "t".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .project(vec![
                col("number"),
                col("pk3").alias_qualified(Some("b"), "pk1"),
                col("pk2").alias_qualified(Some("a"), "pk1"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let scan_plan = plan.inputs()[0].clone();

        // Test aliased_columns_for from scan to projection
        assert_eq!(
            aliased_columns_for(&[qcol("t.pk2")].into(), &plan, Some(&scan_plan)).unwrap(),
            [(qcol("t.pk2"), [qcol("a.pk1")].into())].into()
        );
    }

    #[test]
    fn proj_alias_aliased_aggr() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "t".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .project(vec![
                col("number"),
                col("pk1").alias("pk3"),
                col("pk2").alias("pk4"),
            ])
            .unwrap()
            .project(vec![
                col("number"),
                col("pk3").alias("pk42"),
                col("pk4").alias("pk43"),
            ])
            .unwrap()
            .aggregate(vec![col("pk42"), col("pk43")], vec![min(col("number"))])
            .unwrap()
            .build()
            .unwrap();

        let aggr_plan = plan.clone();
        let second_proj = aggr_plan.inputs()[0].clone();
        let first_proj = second_proj.inputs()[0].clone();
        let scan_plan = first_proj.inputs()[0].clone();

        // Test aliased_columns_for from scan to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("t.pk1")].into(), &plan, Some(&scan_plan)).unwrap(),
            [(qcol("t.pk1"), [qcol("pk42")].into())].into()
        );

        // Test aliased_columns_for from scan to first projection
        assert_eq!(
            aliased_columns_for(&[Column::from_name("pk1")].into(), &first_proj, None).unwrap(),
            [(Column::from_name("pk1"), [qcol("pk3")].into())].into()
        );
    }

    #[test]
    fn aggr_aggr_alias() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "t".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .aggregate(vec![col("pk1"), col("pk2")], vec![max(col("number"))])
            .unwrap()
            .aggregate(
                vec![col("pk1"), col("pk2")],
                vec![min(col("max(t.number)"))],
            )
            .unwrap()
            .build()
            .unwrap();

        let second_aggr = plan.clone();
        let first_aggr = second_aggr.inputs()[0].clone();
        let scan_plan = first_aggr.inputs()[0].clone();

        // Test aliased_columns_for from scan to final plan (identity mapping for aggregates)
        assert_eq!(
            aliased_columns_for(&[qcol("t.pk1")].into(), &plan, Some(&scan_plan)).unwrap(),
            [(qcol("t.pk1"), [qcol("t.pk1")].into())].into()
        );

        // Test aliased_columns_for from scan to first aggregate
        assert_eq!(
            aliased_columns_for(&[qcol("t.pk1")].into(), &first_aggr, Some(&scan_plan)).unwrap(),
            [(qcol("t.pk1"), [qcol("t.pk1")].into())].into()
        );

        // Test aliased_columns_for from first aggregate to final plan
        assert_eq!(
            aliased_columns_for(&[qcol("t.pk1")].into(), &plan, Some(&first_aggr)).unwrap(),
            [(qcol("t.pk1"), [qcol("t.pk1")].into())].into()
        );

        // Test aliased_columns_for from leaf to final plan
        assert_eq!(
            aliased_columns_for(&[Column::from_name("pk1")].into(), &plan, None).unwrap(),
            [(Column::from_name("pk1"), [qcol("t.pk1")].into())].into()
        );
    }

    #[test]
    fn aggr_aggr_alias_projection() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "t".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .aggregate(vec![col("pk1"), col("pk2")], vec![max(col("number"))])
            .unwrap()
            .aggregate(
                vec![col("pk1"), col("pk2")],
                vec![min(col("max(t.number)"))],
            )
            .unwrap()
            .project(vec![
                col("pk1").alias("pk11"),
                col("pk2").alias("pk22"),
                col("min(max(t.number))").alias("min_max_number"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let proj_plan = plan.clone();
        let second_aggr = proj_plan.inputs()[0].clone();

        // Test original_column_for from projection to second aggregate for aggr gen column
        assert_eq!(
            original_column_for(
                &[Column::from_name("min_max_number")].into(),
                plan.clone(),
                Some(Arc::new(second_aggr.clone()))
            )
            .unwrap(),
            [(
                Column::from_name("min_max_number"),
                Column::from_name("min(max(t.number))")
            )]
            .into()
        );

        // Test aliased_columns_for from second aggregate to projection
        assert_eq!(
            aliased_columns_for(
                &[Column::from_name("min(max(t.number))")].into(),
                &plan,
                Some(&second_aggr)
            )
            .unwrap(),
            [(
                Column::from_name("min(max(t.number))"),
                [Column::from_name("min_max_number")].into()
            )]
            .into()
        );
    }
}
