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
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::TableReference;
use datafusion_expr::LogicalPlan as DfLogicalPlan;
use session::context::QueryContextRef;
use snafu::ResultExt;
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::error::{DataFusionSnafu, Result};

struct TableNamesExtractAndRewriter {
    pub(crate) table_names: HashSet<TableName>,
    query_ctx: QueryContextRef,
}

impl TreeNodeRewriter for TableNamesExtractAndRewriter {
    type Node = DfLogicalPlan;

    /// descend
    fn f_down<'a>(
        &mut self,
        node: Self::Node,
    ) -> datafusion::error::Result<Transformed<Self::Node>> {
        match node {
            DfLogicalPlan::TableScan(mut scan) => {
                if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
                    if let Some(provider) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                    {
                        if provider.table().table_type() == TableType::Base {
                            let info = provider.table().table_info();
                            self.table_names.insert(TableName::new(
                                info.catalog_name.clone(),
                                info.schema_name.clone(),
                                info.name.clone(),
                            ));
                        }
                    }
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
                Ok(Transformed::yes(DfLogicalPlan::TableScan(scan)))
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
    plan: DfLogicalPlan,
    query_ctx: QueryContextRef,
) -> Result<(HashSet<TableName>, DfLogicalPlan)> {
    let mut extractor = TableNamesExtractAndRewriter::new(query_ctx);
    let plan = plan.rewrite(&mut extractor).context(DataFusionSnafu)?;
    Ok((extractor.table_names, plan.data))
}

#[cfg(test)]
pub(crate) mod tests {

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
    use session::context::QueryContextBuilder;

    use super::*;

    pub(crate) fn mock_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]);
        let table_source = LogicalTableSource::new(SchemaRef::new(schema));

        let projection = None;

        let builder =
            LogicalPlanBuilder::scan("devices", Arc::new(table_source), projection).unwrap();

        builder
            .filter(col("id").gt(lit(500)))
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_extract_full_table_names() {
        let ctx = QueryContextBuilder::default()
            .current_schema("test".to_string())
            .build();

        let (table_names, plan) =
            extract_and_rewrite_full_table_names(mock_plan(), Arc::new(ctx)).unwrap();

        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&TableName::new(
            DEFAULT_CATALOG_NAME.to_string(),
            "test".to_string(),
            "devices".to_string()
        )));

        assert_eq!(
            "Filter: devices.id > Int32(500)\n  TableScan: greptime.test.devices",
            format!("{:?}", plan)
        );
    }
}
