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

use datafusion::datasource::DefaultTableSource;
use datafusion_common::Column;
use datafusion_expr::{Expr, LogicalPlan, TableScan};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

/// Mapping of original column in table to all the alias at current node
pub type AliasMapping = HashMap<String, HashSet<Column>>;

/// tracking aliases for the source table columns in the plan
#[derive(Debug, Clone)]
pub struct AliasTracker {
    /// mapping from the original table name to the alias used in the plan
    /// notice how one column might have multiple aliases in the plan
    ///
    pub mapping: AliasMapping,
}

impl AliasTracker {
    pub fn new(table_scan: &TableScan) -> Option<Self> {
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
                    let schema = info.meta.schema.clone();
                    let col_schema = schema.column_schemas();
                    let mapping = col_schema
                        .iter()
                        .map(|col| {
                            (
                                col.name.clone(),
                                HashSet::from_iter(std::iter::once(Column::new_unqualified(
                                    col.name.clone(),
                                ))),
                            )
                        })
                        .collect();
                    return Some(Self { mapping });
                }
            }
        }

        None
    }

    /// update alias for original columns
    ///
    /// only handle `Alias` with column in `Projection` node
    pub fn update_alias(&mut self, node: &LogicalPlan) {
        if let LogicalPlan::Projection(projection) = node {
            // first collect all the alias mapping, i.e. the col_a AS b AS c AS d become `a->d`
            // notice one column might have multiple aliases
            let mut alias_mapping: AliasMapping = HashMap::new();
            for expr in &projection.expr {
                if let Expr::Alias(alias) = expr {
                    let outer_alias = alias.clone();
                    let mut cur_alias = alias.clone();
                    while let Expr::Alias(alias) = *cur_alias.expr {
                        cur_alias = alias;
                    }
                    if let Expr::Column(column) = *cur_alias.expr {
                        alias_mapping
                            .entry(column.name.clone())
                            .or_default()
                            .insert(Column::new(outer_alias.relation, outer_alias.name));
                    }
                } else if let Expr::Column(column) = expr {
                    // identity mapping
                    alias_mapping
                        .entry(column.name.clone())
                        .or_default()
                        .insert(column.clone());
                }
            }

            // update mapping using `alias_mapping`
            let mut new_mapping = HashMap::new();
            for (table_col_name, cur_columns) in std::mem::take(&mut self.mapping) {
                let new_aliases = {
                    let mut new_aliases = HashSet::new();
                    for cur_column in &cur_columns {
                        let new_alias_for_cur_column = alias_mapping
                            .get(cur_column.name())
                            .cloned()
                            .unwrap_or_default();

                        for new_alias in new_alias_for_cur_column {
                            let is_table_ref_eq = match (&new_alias.relation, &cur_column.relation)
                            {
                                (Some(o), Some(c)) => o.resolved_eq(c),
                                _ => true,
                            };
                            // is the same column if both name and table ref is eq
                            if is_table_ref_eq {
                                new_aliases.insert(new_alias.clone());
                            }
                        }
                    }
                    new_aliases
                };

                new_mapping.insert(table_col_name, new_aliases);
            }

            self.mapping = new_mapping;
            common_telemetry::debug!(
                "Updating alias tracker to {:?} using node: \n{node}",
                self.mapping
            );
        }
    }

    pub fn get_all_alias_for_col(&self, col_name: &str) -> Option<&HashSet<Column>> {
        self.mapping.get(col_name)
    }

    #[allow(unused)]
    pub fn is_alias_for(&self, original_col: &str, cur_col: &Column) -> bool {
        self.mapping
            .get(original_col)
            .map(|cols| cols.contains(cur_col))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_telemetry::init_default_ut_logging;
    use datafusion::error::Result as DfResult;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
    use datafusion_expr::{col, LogicalPlanBuilder};

    use super::*;
    use crate::dist_plan::analyzer::test::TestTable;

    #[derive(Debug)]
    struct TrackerTester {
        alias_tracker: Option<AliasTracker>,
        mapping_at_each_level: Vec<AliasMapping>,
    }

    impl TreeNodeVisitor<'_> for TrackerTester {
        type Node = LogicalPlan;

        fn f_up(&mut self, node: &LogicalPlan) -> DfResult<TreeNodeRecursion> {
            if let Some(alias_tracker) = &mut self.alias_tracker {
                alias_tracker.update_alias(node);
                self.mapping_at_each_level.push(
                    self.alias_tracker
                        .as_ref()
                        .map(|a| a.mapping.clone())
                        .unwrap_or_default()
                        .clone(),
                );
            } else if let LogicalPlan::TableScan(table_scan) = node {
                self.alias_tracker = AliasTracker::new(table_scan);
                self.mapping_at_each_level.push(
                    self.alias_tracker
                        .as_ref()
                        .map(|a| a.mapping.clone())
                        .unwrap_or_default()
                        .clone(),
                );
            }
            Ok(TreeNodeRecursion::Continue)
        }
    }

    #[test]
    fn proj_alias_tracker() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "numbers".to_string());
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

        let mut tracker_tester = TrackerTester {
            alias_tracker: None,
            mapping_at_each_level: Vec::new(),
        };
        plan.visit(&mut tracker_tester).unwrap();

        assert_eq!(
            tracker_tester.mapping_at_each_level,
            vec![
                HashMap::from([
                    ("number".to_string(), HashSet::from(["number".into()])),
                    ("pk1".to_string(), HashSet::from(["pk1".into()])),
                    ("pk2".to_string(), HashSet::from(["pk2".into()])),
                    ("pk3".to_string(), HashSet::from(["pk3".into()])),
                    ("ts".to_string(), HashSet::from(["ts".into()]))
                ]),
                HashMap::from([
                    ("number".to_string(), HashSet::from(["t.number".into()])),
                    ("pk1".to_string(), HashSet::from([])),
                    ("pk2".to_string(), HashSet::from(["pk3".into()])),
                    ("pk3".to_string(), HashSet::from(["pk1".into()])),
                    ("ts".to_string(), HashSet::from([]))
                ]),
                HashMap::from([
                    ("number".to_string(), HashSet::from(["t.number".into()])),
                    ("pk1".to_string(), HashSet::from([])),
                    ("pk2".to_string(), HashSet::from(["pk1".into()])),
                    ("pk3".to_string(), HashSet::from(["pk2".into()])),
                    ("ts".to_string(), HashSet::from([]))
                ])
            ]
        );
    }

    #[test]
    fn proj_multi_alias_tracker() {
        // use logging for better debugging
        init_default_ut_logging();
        let test_table = TestTable::table_with_name(0, "numbers".to_string());
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

        let mut tracker_tester = TrackerTester {
            alias_tracker: None,
            mapping_at_each_level: Vec::new(),
        };
        plan.visit(&mut tracker_tester).unwrap();

        assert_eq!(
            tracker_tester.mapping_at_each_level,
            vec![
                HashMap::from([
                    ("number".to_string(), HashSet::from(["number".into()])),
                    ("pk1".to_string(), HashSet::from(["pk1".into()])),
                    ("pk2".to_string(), HashSet::from(["pk2".into()])),
                    ("pk3".to_string(), HashSet::from(["pk3".into()])),
                    ("ts".to_string(), HashSet::from(["ts".into()]))
                ]),
                HashMap::from([
                    ("number".to_string(), HashSet::from(["t.number".into()])),
                    ("pk1".to_string(), HashSet::from([])),
                    ("pk2".to_string(), HashSet::from([])),
                    (
                        "pk3".to_string(),
                        HashSet::from(["pk1".into(), "pk2".into()])
                    ),
                    ("ts".to_string(), HashSet::from([]))
                ]),
                HashMap::from([
                    ("number".to_string(), HashSet::from(["t.number".into()])),
                    ("pk1".to_string(), HashSet::from([])),
                    ("pk2".to_string(), HashSet::from([])),
                    (
                        "pk3".to_string(),
                        HashSet::from(["pk4".into(), "pk5".into()])
                    ),
                    ("ts".to_string(), HashSet::from([]))
                ])
            ]
        );
    }
}
