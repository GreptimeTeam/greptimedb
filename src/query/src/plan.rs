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

use catalog::CatalogManagerRef;
use datafusion::datasource::{DefaultTableSource, provider_as_source};
use datafusion_common::TableReference;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_expr::logical_plan::TableScan;
use datafusion_expr::{Expr, LogicalPlan};
use session::context::{QueryContext, QueryContextRef};
use snafu::ResultExt;
use table::TableRef;
use table::dist_table::DistTable;
pub use table::metadata::TableType;
use table::table::PartitionRules;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::error::{CatalogSnafu, Result};

struct TableNamesExtractAndRewriter {
    pub(crate) table_names: HashSet<TableName>,
    query_ctx: QueryContextRef,
}

impl TreeNodeRewriter for TableNamesExtractAndRewriter {
    type Node = LogicalPlan;

    /// descend
    fn f_down<'a>(
        &mut self,
        node: Self::Node,
    ) -> datafusion::error::Result<Transformed<Self::Node>> {
        match node {
            LogicalPlan::TableScan(mut scan) => {
                if let Some(table) = scanned_base_table(&scan) {
                    let info = table.table_info();
                    self.table_names.insert(TableName::new(
                        info.catalog_name.clone(),
                        info.schema_name.clone(),
                        info.name.clone(),
                    ));
                }
                match &scan.table_name {
                    TableReference::Full {
                        catalog,
                        schema,
                        table,
                    } => {
                        self.table_names.insert(TableName::new(
                            catalog.to_string(),
                            schema.to_string(),
                            table.to_string(),
                        ));
                    }
                    TableReference::Partial { schema, table } => {
                        self.table_names.insert(TableName::new(
                            self.query_ctx.current_catalog(),
                            schema.to_string(),
                            table.to_string(),
                        ));

                        scan.table_name = TableReference::Full {
                            catalog: self.query_ctx.current_catalog().into(),
                            schema: schema.clone(),
                            table: table.clone(),
                        };
                    }
                    TableReference::Bare { table } => {
                        self.table_names.insert(TableName::new(
                            self.query_ctx.current_catalog(),
                            self.query_ctx.current_schema(),
                            table.to_string(),
                        ));

                        scan.table_name = TableReference::Full {
                            catalog: self.query_ctx.current_catalog().into(),
                            schema: self.query_ctx.current_schema().into(),
                            table: table.clone(),
                        };
                    }
                }
                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            }
            node => Ok(Transformed::no(node)),
        }
    }
}

impl TableNamesExtractAndRewriter {
    fn new(query_ctx: QueryContextRef) -> Self {
        Self {
            query_ctx,
            table_names: HashSet::new(),
        }
    }
}

/// Extracts and rewrites the table names in the plan in the fully qualified style,
/// return the table names and new plan.
pub fn extract_and_rewrite_full_table_names(
    plan: LogicalPlan,
    query_ctx: QueryContextRef,
) -> Result<(HashSet<TableName>, LogicalPlan)> {
    let mut extractor = TableNamesExtractAndRewriter::new(query_ctx);
    let plan = plan.rewrite_with_subqueries(&mut extractor)?;
    Ok((extractor.table_names, plan.data))
}

/// Updates the scans in `plan` whose table changed its partition columns after the plan was built.
///
/// Distributed planning reads the partition columns from the table a scan holds, and the regions
/// from the current route. A plan kept across executions, such as a prepared statement's, must
/// therefore report the current partition columns, or a query that needs merging on the frontend
/// runs entirely on each region.
///
/// Returns `plan` as it is when no scanned table changed its partition columns.
pub async fn refresh_partition_columns(
    plan: LogicalPlan,
    catalog_manager: &CatalogManagerRef,
    query_ctx: &QueryContext,
) -> Result<LogicalPlan> {
    let mut scanned = HashMap::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(table) = scanned_base_table(scan)
        {
            scanned.insert(table.table_info().table_id(), table);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut refreshed = HashMap::new();
    for (table_id, table) in scanned {
        // Looks up by id to follow a rename and to not bind a table that replaced the scanned one.
        let Some(info) = catalog_manager
            .table_info_by_id(table_id)
            .await
            .context(CatalogSnafu)?
        else {
            continue;
        };
        let current = catalog_manager
            .table(
                &info.catalog_name,
                &info.schema_name,
                &info.name,
                Some(query_ctx),
            )
            .await
            .context(CatalogSnafu)?;
        let Some(current) = current.filter(|t| t.table_info().table_id() == table_id) else {
            continue;
        };
        if has_same_partition_columns(&table, &current) {
            continue;
        }
        let table = if table.schema() == current.schema() {
            current
        } else {
            with_unmapped_partition_columns(&table, &current)
        };
        refreshed.insert(table_id, table);
    }
    if refreshed.is_empty() {
        return Ok(plan);
    }

    let plan = plan.transform_with_subqueries(|node| {
        let LogicalPlan::TableScan(mut scan) = node else {
            return Ok(Transformed::no(node));
        };
        let Some(table) = scanned_base_table(&scan)
            .and_then(|table| refreshed.get(&table.table_info().table_id()))
        else {
            return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
        };
        scan.source = provider_as_source(Arc::new(DfTableProviderAdapter::new(table.clone())));
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })?;
    Ok(plan.data)
}

fn scanned_base_table(scan: &TableScan) -> Option<TableRef> {
    let source = scan.source.downcast_ref::<DefaultTableSource>()?;
    let provider = source
        .table_provider
        .downcast_ref::<DfTableProviderAdapter>()?;
    let table = provider.table();
    (table.table_type() == TableType::Base).then_some(table)
}

fn has_same_partition_columns(left: &TableRef, right: &TableRef) -> bool {
    left.table_info()
        .meta
        .partition_column_names()
        .eq(right.table_info().meta.partition_column_names())
        && extra_partition_columns(left) == extra_partition_columns(right)
}

fn extra_partition_columns(table: &TableRef) -> &[String] {
    table
        .partition_rules()
        .map(|rules| rules.extra_phy_cols_not_in_logical_table.as_slice())
        .unwrap_or_default()
}

/// Builds a table that keeps the schema of `table` and reports the partition columns of `current`
/// as absent from that schema.
///
/// A scan's projection holds indices into the schema it was built on, and Substrait sends that
/// schema with those indices, so a plan cannot move to a changed schema. Reporting the columns as
/// absent keeps distributed planning from relying on any of them.
fn with_unmapped_partition_columns(table: &TableRef, current: &TableRef) -> TableRef {
    let mut info = (*table.table_info()).clone();
    info.meta.partition_key_indices.clear();
    let current_info = current.table_info();
    let extra_phy_cols_not_in_logical_table = current_info
        .meta
        .partition_column_names()
        .chain(extra_partition_columns(current))
        .cloned()
        .collect();
    DistTable::table_partitioned(
        Arc::new(info),
        Some(PartitionRules {
            extra_phy_cols_not_in_logical_table,
        }),
    )
}

/// A trait to extract expressions from a logical plan.
pub trait ExtractExpr {
    /// Gets expressions from a logical plan.
    /// It handles [Join] specially so [LogicalPlan::with_new_exprs()] can use the expressions
    /// this method returns.
    fn expressions_consider_join(&self) -> Vec<Expr>;
}

impl ExtractExpr for LogicalPlan {
    fn expressions_consider_join(&self) -> Vec<Expr> {
        self.expressions()
    }
}

#[cfg(test)]
pub(crate) mod tests {

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use catalog::RegisterTableRequest;
    use catalog::memory::MemoryCatalogManager;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col, lit, scalar_subquery};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema as TableSchema};
    use session::context::QueryContextBuilder;
    use table::test_util::table_info::test_table_info;

    use super::*;

    const TABLE_ID: u32 = 1024;

    fn mock_table_source() -> Arc<LogicalTableSource> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]);
        Arc::new(LogicalTableSource::new(SchemaRef::new(schema)))
    }

    fn mock_plan() -> LogicalPlan {
        let table_source = mock_table_source();

        let projection = None;

        let builder = LogicalPlanBuilder::scan("devices", table_source, projection).unwrap();

        builder
            .filter(col("id").gt(lit(500)))
            .unwrap()
            .build()
            .unwrap()
    }

    fn scalar_subquery_plan(table_name: TableReference) -> LogicalPlan {
        let subquery = LogicalPlanBuilder::scan(table_name, mock_table_source(), None)
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap();

        LogicalPlanBuilder::empty(false)
            .project(vec![scalar_subquery(Arc::new(subquery))])
            .unwrap()
            .build()
            .unwrap()
    }

    fn assert_dependencies(actual: &HashSet<TableName>, expected: &[(&str, &str, &str)]) {
        let expected = expected
            .iter()
            .map(|(catalog, schema, table)| TableName::new(*catalog, *schema, *table))
            .collect::<HashSet<_>>();
        assert_eq!(&expected, actual);
    }

    fn assert_nested_scalar_subquery_table_name(plan: &LogicalPlan, expected: TableReference) {
        let LogicalPlan::Projection(projection) = plan else {
            panic!("expected scalar-subquery projection, got {plan:?}");
        };
        let [Expr::ScalarSubquery(subquery)] = projection.expr.as_slice() else {
            panic!(
                "expected one scalar-subquery expression, got {:?}",
                projection.expr
            );
        };
        let LogicalPlan::Projection(projection) = subquery.subquery.as_ref() else {
            panic!(
                "expected scalar-subquery projection, got {:?}",
                subquery.subquery
            );
        };
        let LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
            panic!(
                "expected scalar-subquery table scan, got {:?}",
                projection.input
            );
        };

        assert_eq!(expected, scan.table_name);
    }

    #[test]
    fn test_extract_full_table_names() {
        let ctx = QueryContextBuilder::default()
            .current_schema("test".to_string())
            .build();

        let (table_names, plan) =
            extract_and_rewrite_full_table_names(mock_plan(), Arc::new(ctx)).unwrap();

        assert_dependencies(&table_names, &[(DEFAULT_CATALOG_NAME, "test", "devices")]);

        assert_eq!(
            "Filter: devices.id > Int32(500)\n  TableScan: greptime.test.devices",
            plan.to_string()
        );
    }

    #[test]
    fn test_extract_full_table_names_from_scalar_subquery_bare_table_scan() {
        let ctx = QueryContextBuilder::default()
            .current_catalog("qp031_catalog".to_string())
            .current_schema("qp031_current_schema".to_string())
            .build();

        let (table_names, plan) = extract_and_rewrite_full_table_names(
            scalar_subquery_plan(TableReference::bare("lookup")),
            Arc::new(ctx),
        )
        .unwrap();

        assert_dependencies(
            &table_names,
            &[("qp031_catalog", "qp031_current_schema", "lookup")],
        );
        assert_nested_scalar_subquery_table_name(
            &plan,
            TableReference::full("qp031_catalog", "qp031_current_schema", "lookup"),
        );
    }

    #[test]
    fn test_extract_full_table_names_from_scalar_subquery_partial_table_scan() {
        let ctx = QueryContextBuilder::default()
            .current_catalog("qp031_catalog".to_string())
            .current_schema("qp031_current_schema".to_string())
            .build();

        let (table_names, plan) = extract_and_rewrite_full_table_names(
            scalar_subquery_plan(TableReference::partial("qp031_lookup_schema", "lookup")),
            Arc::new(ctx),
        )
        .unwrap();

        assert_dependencies(
            &table_names,
            &[("qp031_catalog", "qp031_lookup_schema", "lookup")],
        );
        assert_nested_scalar_subquery_table_name(
            &plan,
            TableReference::full("qp031_catalog", "qp031_lookup_schema", "lookup"),
        );
    }

    #[test]
    fn test_extract_full_table_names_from_scalar_subquery_full_table_scan() {
        let ctx = QueryContextBuilder::default()
            .current_catalog("qp031_catalog".to_string())
            .current_schema("qp031_current_schema".to_string())
            .build();

        let (table_names, plan) = extract_and_rewrite_full_table_names(
            scalar_subquery_plan(TableReference::full(
                "qp031_external_catalog",
                "qp031_external_schema",
                "lookup",
            )),
            Arc::new(ctx),
        )
        .unwrap();

        assert_dependencies(
            &table_names,
            &[("qp031_external_catalog", "qp031_external_schema", "lookup")],
        );
        assert_nested_scalar_subquery_table_name(
            &plan,
            TableReference::full("qp031_external_catalog", "qp031_external_schema", "lookup"),
        );
    }

    fn partitioned_table(
        columns: &[&str],
        partition_key_indices: Vec<usize>,
        extras: &[&str],
    ) -> TableRef {
        let columns = columns
            .iter()
            .map(|name| ColumnSchema::new(*name, ConcreteDataType::string_datatype(), true))
            .collect();
        let mut info = test_table_info(
            TABLE_ID,
            "t",
            DEFAULT_SCHEMA_NAME,
            DEFAULT_CATALOG_NAME,
            Arc::new(TableSchema::new(columns)),
        );
        info.meta.partition_key_indices = partition_key_indices;
        let rules = PartitionRules {
            extra_phy_cols_not_in_logical_table: extras.iter().map(ToString::to_string).collect(),
        };
        DistTable::table_partitioned(Arc::new(info), Some(rules))
    }

    /// Returns the plan that [refresh_partition_columns] builds for a scan of `captured` when
    /// the catalog holds `current`.
    async fn refresh_scan(captured: TableRef, current: TableRef) -> (TableScan, TableScan) {
        let source = provider_as_source(Arc::new(DfTableProviderAdapter::new(captured)));
        let filters = vec![col("a").gt(lit("x"))];
        let plan = LogicalPlanBuilder::scan_with_filters_fetch(
            "t",
            source,
            Some(vec![1]),
            filters,
            Some(5),
        )
        .unwrap()
        .build()
        .unwrap();
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "t".to_string(),
                table_id: TABLE_ID,
                table: current,
            })
            .unwrap();
        let catalog_manager: CatalogManagerRef = catalog_manager;
        let refreshed =
            refresh_partition_columns(plan.clone(), &catalog_manager, &QueryContext::arc())
                .await
                .unwrap();
        let (LogicalPlan::TableScan(original), LogicalPlan::TableScan(refreshed)) =
            (plan, refreshed)
        else {
            panic!("expected table scans");
        };
        (original, refreshed)
    }

    #[tokio::test]
    async fn test_refresh_partition_columns_compares_names() {
        let captured = partitioned_table(&["a", "b"], vec![1], &[]);
        let current = partitioned_table(&["b", "a", "c"], vec![0], &[]);

        let (original, refreshed) = refresh_scan(captured, current).await;

        assert!(Arc::ptr_eq(&original.source, &refreshed.source));
    }

    #[tokio::test]
    async fn test_refresh_partition_columns_with_equal_schema() {
        let captured = partitioned_table(&["a", "b"], vec![], &[]);
        let current = partitioned_table(&["a", "b"], vec![1], &[]);

        let (original, refreshed) = refresh_scan(captured, current.clone()).await;

        let table = scanned_base_table(&refreshed).unwrap();
        assert!(Arc::ptr_eq(&table, &current));
        assert_eq!(original.projection, refreshed.projection);
        assert_eq!(original.projected_schema, refreshed.projected_schema);
        assert_eq!(original.filters, refreshed.filters);
        assert_eq!(original.fetch, refreshed.fetch);
    }

    #[tokio::test]
    async fn test_refresh_partition_columns_with_changed_schema() {
        let captured = partitioned_table(&["a", "b"], vec![1], &[]);
        let current = partitioned_table(&["a", "b", "c"], vec![2], &["d"]);

        let (original, refreshed) = refresh_scan(captured.clone(), current).await;

        let table = scanned_base_table(&refreshed).unwrap();
        assert_eq!(table.schema(), captured.schema());
        assert!(table.table_info().meta.partition_key_indices.is_empty());
        assert_eq!(extra_partition_columns(&table), ["c", "d"]);
        assert_eq!(original.projection, refreshed.projection);
        assert_eq!(original.projected_schema, refreshed.projected_schema);
        assert_eq!(original.filters, refreshed.filters);
        assert_eq!(original.fetch, refreshed.fetch);
    }
}
