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

use std::collections::HashSet;

use datafusion::datasource::DefaultTableSource;
use datafusion_common::TableReference;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::{Expr, LogicalPlan};
use session::context::QueryContextRef;
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::error::Result;

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
                if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>()
                    && let Some(provider) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                    && provider.table().table_type() == TableType::Base
                {
                    let info = provider.table().table_info();
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
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col, lit, scalar_subquery};
    use session::context::QueryContextBuilder;

    use super::*;

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
}
