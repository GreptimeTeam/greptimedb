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

use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::TreeNodeVisitor;
use datafusion_common::Column;
use datafusion_expr::expr::Alias;
use datafusion_expr::{Expr, LogicalPlan, TableScan};
use datafusion_sql::TableReference;
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

#[derive(Default, Clone)]
struct AliasLayer {
    /// for convenient of querying, key is field's name
    old_to_new: BTreeMap<String, Vec<(Column, HashSet<Column>)>>,
    /// (field name, (new column, old column))
    new_to_old: BTreeMap<String, Vec<(Column, Column)>>,
}

/// a much less verbose debug impl for AliasLayer
impl std::fmt::Debug for AliasLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = BTreeMap::new();
        for mappings in self.old_to_new.values() {
            let new_cols: HashMap<String, String> = mappings
                .iter()
                .flat_map(|(old_column, new_cols)| {
                    new_cols
                        .iter()
                        .map(|c| (old_column.to_string(), c.to_string()))
                })
                .collect();
            map.extend(new_cols);
        }
        let mut new_to_old_map = BTreeMap::new();
        for (new_col_name, mappings) in &self.new_to_old {
            let old_cols: HashMap<String, String> = mappings
                .iter()
                .map(|(new, old)| (new.to_string(), old.to_string()))
                .collect();
            new_to_old_map.insert(new_col_name, old_cols);
        }
        f.debug_struct("AliasLayer")
            .field("old_to_new", &map)
            .field("new_to_old", &new_to_old_map)
            .finish()
    }
}

impl AliasLayer {
    pub fn insert_alias(&mut self, old_column: Column, new_columns: HashSet<Column>) {
        self.old_to_new
            .entry(old_column.name().to_string())
            .or_default()
            .push((old_column.clone(), new_columns.clone()));
        for new_column in new_columns {
            self.new_to_old
                .entry(new_column.name().to_string())
                .or_default()
                .push((new_column, old_column.clone()));
        }
    }

    pub fn get_new_from_old(&self, old_column: Column) -> Option<HashSet<Column>> {
        let column_mappings = self.old_to_new.get(old_column.name())?;
        for (old_col, new_cols) in column_mappings {
            match (&old_col.relation, &old_column.relation) {
                (Some(o), Some(c)) => {
                    if o.resolved_eq(c) {
                        return Some(new_cols.clone());
                    }
                }
                _ => {
                    // if any of the two relation is None, meaning not fully qualified, just match name
                    return Some(new_cols.clone());
                }
            }
        }
        None
    }

    pub fn get_old_from_new(&self, new_column: Column) -> Option<Column> {
        for (new, old) in self
            .new_to_old
            .get(new_column.name())
            .cloned()
            .unwrap_or_default()
        {
            match (&new.relation, &new_column.relation) {
                (Some(n), Some(c)) => {
                    if n.resolved_eq(c) {
                        return Some(old);
                    }
                }
                _ => {
                    // if any of the two relation is None, meaning not fully qualified, just match name
                    return Some(old);
                }
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
                let new_alias_set = alias_scope
                    .get_new_from_old(cur_alias.clone())
                    .unwrap_or_default();
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

        let mut scope_tracker = LayeredAliasTracker::default();
        plan.visit(&mut scope_tracker).unwrap();

        assert_eq!(
            scope_tracker.query_alias_at(1, None, &Column::from_name("pk3")),
            HashSet::from([
                Column::from_qualified_name("pk5"),
                Column::from_qualified_name("pk4")
            ])
        );

        assert_eq!(
            scope_tracker.query_alias_at(2, None, &Column::from_name("pk3")),
            HashSet::from([
                Column::from_qualified_name("pk1"),
                Column::from_qualified_name("pk2")
            ])
        );

        // also test query original column
        assert_eq!(
            scope_tracker.query_original_column(1, 2, &Column::from_name("pk5")),
            Some(Column::from_qualified_name("t.pk3"))
        );

        assert_eq!(
            scope_tracker.query_original_column(1, 1, &Column::from_name("pk5")),
            Some(Column::from_name("pk1"))
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
