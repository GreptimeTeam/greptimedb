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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use common_telemetry::info;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion_common::tree_node::TreeNodeVisitor;
use datafusion_common::Column;
use datafusion_expr::expr::Alias;
use datafusion_expr::{Expr, LogicalPlan, TableScan};
use datafusion_sql::TableReference;
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

/// Return all the original columns for the given aliased columns at the original node
pub fn original_column_for(
    aliased_columns: HashSet<Column>,
    aliased_node: LogicalPlan,
    original_node: Option<Arc<LogicalPlan>>,
) -> DfResult<HashMap<Column, Column>> {
    let cur_aliases: HashMap<Column, Column> = aliased_columns
        .iter()
        .map(|c| (c.clone(), c.clone()))
        .collect();

    original_column_for_recursive(cur_aliases, &aliased_node, &original_node)
}

fn original_column_for_recursive(
    cur_aliases: HashMap<Column, Column>,
    node: &LogicalPlan,
    original_node: &Option<Arc<LogicalPlan>>,
) -> DfResult<HashMap<Column, Column>> {
    // Base case: check if we've reached the target node
    if let Some(original_node) = original_node {
        if *node == **original_node {
            return Ok(cur_aliases);
        }
    } else if node.inputs().is_empty() {
        // leaf node reached
        return Ok(cur_aliases);
    }

    // Validate node has exactly one child
    if node.inputs().len() != 1 {
        return Err(datafusion::error::DataFusionError::Internal(
            "only accept plan with at most one child".to_string(),
        ));
    }

    // Get alias layer and update aliases
    let layer = get_alias_layer_from_node(node)?;
    let mut new_aliases = HashMap::new();
    for (start_alias, cur_alias) in cur_aliases {
        if let Some(old_column) = layer.get_old_from_new(cur_alias.clone()) {
            new_aliases.insert(start_alias, old_column);
        }
    }

    // Recursive call with child node
    original_column_for_recursive(new_aliases, node.inputs()[0], original_node)
}

/// Return all the aliased columns for the given original columns at the alias node
pub fn aliased_columns_for(
    original_columns: HashSet<Column>,
    aliased_node: LogicalPlan,
    original_node: Option<Arc<LogicalPlan>>,
) -> DfResult<HashMap<Column, HashSet<Column>>> {
    let initial_aliases: HashMap<Column, HashSet<Column>> = original_columns
        .iter()
        .map(|c| (c.clone(), HashSet::from([c.clone()])))
        .collect();

    aliased_columns_for_recursive(initial_aliases, &aliased_node, &original_node)
}

fn aliased_columns_for_recursive(
    cur_aliases: HashMap<Column, HashSet<Column>>,
    node: &LogicalPlan,
    original_node: &Option<Arc<LogicalPlan>>,
) -> DfResult<HashMap<Column, HashSet<Column>>> {
    // Base case: check if we've reached the target node
    if let Some(original_node) = original_node {
        if *node == **original_node {
            return Ok(cur_aliases);
        }
    } else if node.inputs().is_empty() {
        // leaf node reached
        return Ok(cur_aliases);
    }

    // Validate node has exactly one child
    if node.inputs().len() != 1 {
        return Err(datafusion::error::DataFusionError::Internal(
            "only accept plan with at most one child".to_string(),
        ));
    }

    // Recursive call to child node first (descend to original node)
    let child_result = aliased_columns_for_recursive(cur_aliases, node.inputs()[0], original_node)?;

    // Apply alias layer on the way back up (from original to aliased)
    let layer = get_alias_layer_from_node(node)?;
    let mut new_aliases = HashMap::new();
    for (original_column, cur_alias_set) in child_result {
        let mut new_alias_set = HashSet::new();
        for cur_alias in cur_alias_set {
            new_alias_set.extend(layer.get_new_from_old(cur_alias.clone()));
        }
        if !new_alias_set.is_empty() {
            new_aliases.insert(original_column, new_alias_set);
        }
    }

    Ok(new_aliases)
}

/// Return a mapping of original column to all the aliased columns in current node of the plan
pub fn get_alias_layer_from_node(node: &LogicalPlan) -> DfResult<AliasLayer> {
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
                layer.insert_alias(old_column, HashSet::from([new_column]));
            }
            Ok(layer)
        }
        LogicalPlan::TableScan(scan) => {
            let columns = scan.projected_schema.columns();
            let mut layer = AliasLayer::default();
            for col in columns {
                layer.insert_alias(col.clone(), HashSet::from([col.clone()]));
            }
            Ok(layer)
        }
        _ => {
            let input_schema = node.inputs()[0].schema();
            let output_schema = node.schema();
            // only accept at most one child plan, and if not one of the above nodes,
            // also shouldn't modify the schema or else alias scope tracker can't support them
            if node.inputs().len() > 1 {
                return Err(datafusion::error::DataFusionError::Internal(
                    "only accept plan with at most one child".to_string(),
                ));
            } else if node.inputs().len() == 1 {
                if input_schema != output_schema {
                    return Err(datafusion::error::DataFusionError::Internal(format!(
                        "AliasScopeTracker only accept plan that doesn't modify schema, found {}",
                        node
                    )));
                } else {
                    // identity mapping
                    let mut layer = AliasLayer::default();
                    for col in output_schema.columns() {
                        layer.insert_alias(col.clone(), HashSet::from([col.clone()]));
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
            .extend(new_columns.clone());
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

fn get_alias_mapping_from_exprs(exprs: &[Expr]) -> HashMap<Column, HashSet<Column>> {
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
    alias_mapping
}

/// Collect every alias in every level of the plan and their scope
///
/// Every plan given to it should only have at most one children
///
/// TODO(discord9): only handle `Projection` and `SubqueryAlias` nodes now, is `Aggregate` needed?
#[derive(Debug, Clone)]
pub struct LayeredAliasTracker {
    /// current distance from the root
    cur_level: usize,
    /// tracking alias mapping at every level
    /// the outer map is level -> inner map
    /// the inner map is a Mapping
    /// from `new_column after alias` to `old_column before alias`
    alias_scopes: BTreeMap<usize, AliasLayer>,
    /// if the tracker is valid, i.e. all plans given to it are valid
    /// if not valid, the results from querying it are not reliable
    /// e.g. if a plan has more than one child
    /// the tracker will be marked as invalid
    is_valid: bool,
}

impl Default for LayeredAliasTracker {
    fn default() -> Self {
        Self::new(0)
    }
}

impl LayeredAliasTracker {
    /// Create a new LayeredAliasTracker starting from `starting_level`
    ///
    /// if starting with a root node(def as level 1), use 0
    /// or if starting after a node at level n, use n - 1
    pub fn new(starting_level: usize) -> Self {
        Self {
            cur_level: starting_level,
            alias_scopes: BTreeMap::new(),
            is_valid: true,
        }
    }

    /// Tracker is only valid if all plans given to it are valid
    ///
    /// that is all plan given to it has at most one child
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }
}
impl LayeredAliasTracker {
    /// Query new column(s) from old column at old level to a new level
    ///
    /// `from_old_level` should be larger than `new_level`, both is inclusive
    ///
    /// if `from_old_level` is `None`, meaning query from `new_level` to bottom
    ///
    pub fn query_alias_at(
        &self,
        new_level: usize,
        from_old_level: Option<usize>,
        old_column: &Column,
    ) -> HashSet<Column> {
        let ranges = match from_old_level {
            Some(old) => self.alias_scopes.range(new_level..=old).rev(),
            None => self.alias_scopes.range(new_level..).rev(),
        };
        // tracking old column's final aliased column by traversing `ranges`
        let mut cur_aliases = HashSet::from([old_column.clone()]);
        for (level, alias_scope) in ranges {
            let mut new_aliases = HashSet::new();
            for cur_alias in &cur_aliases {
                let new_alias_set = alias_scope.get_new_from_old(cur_alias.clone());
                common_telemetry::trace!(
                    "At level {}, column {} has new aliases: {:?} with layer: {:?}",
                    level,
                    cur_alias,
                    new_alias_set,
                    alias_scope
                );
                new_aliases.extend(new_alias_set);
            }

            cur_aliases = new_aliases;
        }

        cur_aliases
    }

    /// Query original column from aliased column at new level to old level
    ///
    /// `to_old_level` should be larger than `new_level`, both is inclusive
    ///
    /// if return `None`, meaning the new column is not an alias of any old column
    #[allow(unused)]
    pub fn query_original_column(
        &self,
        new_level: usize,
        to_old_level: usize,
        new_column: &Column,
    ) -> Option<Column> {
        let ranges = self.alias_scopes.range(new_level..=to_old_level);

        let mut cur_column = new_column.clone();

        for (_level, alias_scope) in ranges {
            if let Some(old_column) = alias_scope.get_old_from_new(cur_column.clone()) {
                cur_column = old_column;
            } else {
                return None;
            }
        }

        Some(cur_column)
    }

    fn update_alias_for_projection(&mut self, projection: &datafusion_expr::Projection) {
        let cur_level_alias_mapping = get_alias_mapping_from_exprs(&projection.expr);

        let cur_scope = self.alias_scopes.entry(self.cur_level).or_default();

        for (old_column, new_columns) in cur_level_alias_mapping {
            // TODO(discord9): update keys of scope to new_columns
            cur_scope.insert_alias(old_column, new_columns);
        }
    }

    fn update_alias_for_aggregate(&mut self, aggregate: &datafusion_expr::Aggregate) {
        let cur_level_alias_mapping = get_alias_mapping_from_exprs(&aggregate.group_expr);

        let cur_scope = self.alias_scopes.entry(self.cur_level).or_default();

        for (old_column, new_columns) in cur_level_alias_mapping {
            // TODO(discord9): update keys of scope to new_columns
            cur_scope.insert_alias(old_column, new_columns);
        }
    }

    fn init_alias_for_table_scan(&mut self, table_scan: &TableScan) {
        if let Some(source) = table_scan
            .source
            .as_any()
            .downcast_ref::<DefaultTableSource>()
        {
            if let Some(provider) = source
                .table_provider
                .as_any()
                .downcast_ref::<DfTableProviderAdapter>()
            {
                if provider.table().table_type() == TableType::Base {
                    let info = provider.table().table_info();
                    let table_ref = TableReference::full(
                        info.catalog_name.clone(),
                        info.schema_name.clone(),
                        info.name.clone(),
                    );
                    let schema = info.meta.schema.clone();
                    let col_schema = schema.column_schemas();
                    let mut alias_scope = AliasLayer::default();
                    for col in col_schema.iter() {
                        let column = Column::new(Some(table_ref.clone()), col.name.clone());
                        alias_scope.insert_alias(column.clone(), HashSet::from_iter(vec![column]));
                    }
                    self.alias_scopes.insert(self.cur_level, alias_scope);
                    // init alias scope for table scan as `a -> a` since no alias is applied
                }
            }
        }
    }

    fn update_alias_for_subquery_alias(&mut self, subquery_alias: &datafusion_expr::SubqueryAlias) {
        let input = subquery_alias.input.as_ref();
        let table_ref = &subquery_alias.alias;
        let new_to_old_columns = input
            .schema()
            .columns()
            .into_iter()
            .map(|c| (Column::new(Some(table_ref.clone()), c.name()), c))
            .collect::<BTreeMap<_, _>>();
        let mut scope = AliasLayer::default();
        for (new_col, old_col) in new_to_old_columns {
            scope.insert_alias(old_col, HashSet::from_iter(vec![new_col]));
        }
        self.alias_scopes.insert(self.cur_level, scope);
    }
}

impl TreeNodeVisitor<'_> for LayeredAliasTracker {
    type Node = LogicalPlan;

    fn f_up(
        &mut self,
        _node: &LogicalPlan,
    ) -> datafusion::error::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        self.cur_level -= 1;
        if self.cur_level == 0 {
            common_telemetry::debug!("Final layered alias tracker: {:?}", self);
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }

    fn f_down(
        &mut self,
        node: &'_ Self::Node,
    ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        self.cur_level += 1;

        match node {
            LogicalPlan::TableScan(table_scan) => {
                self.init_alias_for_table_scan(table_scan);
            }
            LogicalPlan::Projection(projection) => self.update_alias_for_projection(projection),
            LogicalPlan::Aggregate(aggr) => self.update_alias_for_aggregate(aggr),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                self.update_alias_for_subquery_alias(subquery_alias)
            }
            _ => {
                // only accept at most one child plan, and if not one of the above nodes,
                // also shouldn't modify the schema or else alias scope tracker can't support them
                if node.inputs().len() > 1 {
                    self.is_valid = false;
                    return Ok(datafusion_common::tree_node::TreeNodeRecursion::Stop);
                } else if node.inputs().len() == 1 {
                    let input_schema = node.inputs()[0].schema();
                    let output_schema = node.schema();
                    if input_schema != output_schema {
                        return Err(datafusion::error::DataFusionError::Internal(format!(
                            "AliasScopeTracker only accept plan that doesn't modify schema, found {}",
                            node
                        )));
                    }
                }
            }
        }

        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
}

/// Mapping of original column in table to all the alias at current node
pub type AliasMapping = HashMap<String, HashSet<Column>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_telemetry::init_default_ut_logging;
    use datafusion::functions_aggregate::min_max::{max, min};
    use datafusion_common::tree_node::TreeNode;
    use datafusion_expr::{col, LogicalPlanBuilder};
    use pretty_assertions::assert_eq;

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
            aliased_columns_for(
                HashSet::from([qcol("pk1"), qcol("pk2")]),
                plan.clone(),
                Some(Arc::new(child.clone()))
            )
            .unwrap(),
            HashMap::from([
                (qcol("pk1"), HashSet::from([qcol("pk5")])),
                (qcol("pk2"), HashSet::from([qcol("pk4")]))
            ])
        );

        assert_eq!(
            aliased_columns_for(
                HashSet::from([qcol("pk1"), qcol("pk2")]),
                plan.clone(),
                Some(Arc::new(plan.clone()))
            )
            .unwrap(),
            HashMap::from([])
        );

        assert_eq!(
            aliased_columns_for(
                HashSet::from([qcol("t.pk3")]),
                plan.clone(),
                Some(Arc::new(child.clone()))
            )
            .unwrap(),
            HashMap::from([(qcol("t.pk3"), HashSet::from([qcol("pk5"), qcol("pk4")]))])
        );

        assert_eq!(
            original_column_for(
                HashSet::from([qcol("pk5"), qcol("pk4")]),
                plan.clone(),
                None
            )
            .unwrap(),
            HashMap::from([(qcol("pk5"), qcol("t.pk3")), (qcol("pk4"), qcol("t.pk3"))])
        );

        assert_eq!(
            aliased_columns_for(HashSet::from([qcol("pk3")]), plan.clone(), None).unwrap(),
            HashMap::from([(qcol("pk3"), HashSet::from([qcol("pk5"), qcol("pk4")]))])
        );
        assert_eq!(
            original_column_for(
                HashSet::from([qcol("pk1"), qcol("pk2")]),
                child.clone(),
                None
            )
            .unwrap(),
            HashMap::from([(qcol("pk1"), qcol("t.pk3")), (qcol("pk2"), qcol("t.pk3"))])
        );

        assert_eq!(
            aliased_columns_for(HashSet::from([qcol("pk3")]), child.clone(), None).unwrap(),
            HashMap::from([(qcol("pk3"), HashSet::from([qcol("pk1"), qcol("pk2")]))])
        );

        assert_eq!(
            original_column_for(
                HashSet::from([qcol("pk4"), qcol("pk5")]),
                plan.clone(),
                Some(Arc::new(child.clone()))
            )
            .unwrap(),
            HashMap::from([(qcol("pk4"), qcol("pk2")), (qcol("pk5"), qcol("pk1"))])
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

        let mut scope_tracker = LayeredAliasTracker::default();
        plan.visit(&mut scope_tracker).unwrap();

        assert_eq!(
            scope_tracker.query_alias_at(
                1,
                Some(3),
                &Column::from_qualified_name("greptime.public.t.number"),
            ),
            HashSet::from([Column::from_qualified_name("a.number")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(1, Some(2), &Column::from_qualified_name("t.number"),),
            HashSet::from([Column::from_qualified_name("a.number")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(1, None, &Column::from_qualified_name("t.number"),),
            HashSet::from([Column::from_qualified_name("a.number")])
        );

        assert_eq!(
            scope_tracker.query_original_column(1, 3, &Column::from_qualified_name("a.number")),
            Some(Column::from_qualified_name("greptime.public.t.number"))
        );

        assert_eq!(
            scope_tracker.query_original_column(1, 2, &Column::from_qualified_name("a.number")),
            Some(Column::from_qualified_name("t.number"))
        );

        // query original column
        assert_eq!(
            scope_tracker.query_original_column(1, 2, &Column::from_qualified_name("a.number")),
            Some(Column::from_qualified_name("t.number"))
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

        let mut scope_tracker = LayeredAliasTracker::default();
        plan.visit(&mut scope_tracker).unwrap();

        assert_eq!(
            scope_tracker.query_original_column(1, 3, &Column::from_qualified_name("pk1")),
            Some(Column::from_qualified_name("greptime.public.t.pk2"))
        );

        assert_eq!(
            scope_tracker.query_original_column(1, 2, &Column::from_qualified_name("pk1")),
            Some(Column::from_qualified_name("t.pk2"))
        );

        assert_eq!(
            scope_tracker.query_original_column(1, 1, &Column::from_qualified_name("pk1")),
            Some(Column::from_qualified_name("pk3"))
        );

        assert_eq!(
            scope_tracker.query_alias_at(
                2,
                Some(3),
                &Column::from_qualified_name("greptime.public.t.pk2"),
            ),
            HashSet::from([Column::from_qualified_name("pk3")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(1, Some(2), &Column::from_qualified_name("t.pk2"),),
            HashSet::from([Column::from_qualified_name("pk1")]),
            "{:#?}",
            scope_tracker
        );

        // not same column will failed to query as ambiguous reference
        // TODO(discord9): somehow fix this(too complex and might be unnecessary)
        // since we should always be able to get the original column if need to query

        assert_eq!(
            scope_tracker.query_alias_at(1, Some(2), &Column::from_qualified_name("pk2")),
            HashSet::from([Column::from_qualified_name("pk1")]),
            "{:#?}",
            scope_tracker
        );

        assert_eq!(
            scope_tracker.query_alias_at(
                1,
                Some(3),
                &Column::from_qualified_name("greptime.public.t.pk2"),
            ),
            HashSet::from([Column::from_qualified_name("pk1")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(1, Some(1), &Column::from_qualified_name("pk3"),),
            HashSet::from([Column::from_qualified_name("pk1")]),
            "{:#?}",
            scope_tracker
        );

        assert_eq!(
            scope_tracker.query_alias_at(
                3,
                Some(3),
                &Column::from_qualified_name("greptime.public.t.pk2"),
            ),
            HashSet::from([Column::from_qualified_name("greptime.public.t.pk2")])
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

        let mut scope_tracker = LayeredAliasTracker::default();
        plan.visit(&mut scope_tracker).unwrap();

        assert_eq!(
            scope_tracker.query_alias_at(
                1,
                Some(2),
                &Column::from_qualified_name("greptime.public.t.pk2"),
            ),
            HashSet::from([Column::from_qualified_name("a.pk1")])
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

        let mut scope_tracker = LayeredAliasTracker::default();
        plan.visit(&mut scope_tracker).unwrap();

        assert_eq!(
            scope_tracker.query_alias_at(4, None, &Column::from_name("pk1")),
            HashSet::from([Column::from_qualified_name("greptime.public.t.pk1")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(3, None, &Column::from_name("pk1")),
            HashSet::from([Column::from_qualified_name("pk3")])
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

        let mut scope_tracker = LayeredAliasTracker::default();
        plan.visit(&mut scope_tracker).unwrap();

        assert_eq!(
            scope_tracker.query_alias_at(4, None, &Column::from_qualified_name("t.pk1")),
            HashSet::from([Column::from_qualified_name("t.pk1")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(3, None, &Column::from_name("pk1")),
            HashSet::from([Column::from_qualified_name("greptime.public.t.pk1")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(2, None, &Column::from_name("pk1")),
            HashSet::from([Column::from_qualified_name("t.pk1")])
        );

        assert_eq!(
            scope_tracker.query_alias_at(1, None, &Column::from_name("pk1")),
            HashSet::from([Column::from_qualified_name("t.pk1")])
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

        let mut scope_tracker = LayeredAliasTracker::default();
        plan.visit(&mut scope_tracker).unwrap();

        // query original column from aliased column for aggr gen column
        assert_eq!(
            scope_tracker.query_original_column(1, 1, &Column::from_name("min_max_number")),
            Some(Column::from_name("min(max(t.number))"))
        );

        // because at level 2, min(max(t.number)) is already not an alias of any original column
        // so query original column from aliased column will return None
        assert_eq!(
            scope_tracker.query_original_column(1, 2, &Column::from_name("min_max_number")),
            None
        );
    }
}
