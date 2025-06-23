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

use ahash::{HashMap, HashMapExt, HashSet};
use api::v1::CreateTableExpr;
use catalog::CatalogManagerRef;
use common_meta::key::table_route::TableRouteManagerRef;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use snafu::ResultExt;
use table::metadata::{TableId, TableInfoRef};
use table::TableRef;

use crate::error;
use crate::prom_row_builder::{PromCtx, TableBuilder};

pub struct MetricsBatchBuilder {
    table_route_manager: TableRouteManagerRef,
    catalog_manager: CatalogManagerRef,
}

impl MetricsBatchBuilder {
    /// Detected the DDL requirements according to the staged table rows.
    pub async fn create_or_alter_physical_tables(
        &self,
        tables: &HashMap<PromCtx, HashMap<String, TableBuilder>>,
    ) -> error::Result<()> {
        // Physical table id -> all tags
        let mut existing_tables: HashMap<TableId, HashSet<_>> = HashMap::new();
        // Physical table name to create -> all tags.
        let mut tables_to_create: HashMap<String, HashSet<String>> = HashMap::new();

        for (ctx, tables) in tables {
            for (table_name, table_builder) in tables {
                let physical_table = self
                    .determine_physical_table(table_name, &ctx.physical_table, "todo", "todo")
                    .await?;
                if let Some(physical_table) = physical_table {
                    let tags_in_table = existing_tables
                        .entry(physical_table.table_info().ident.table_id)
                        .or_insert_with(|| {
                            physical_table
                                .table_info()
                                .meta
                                .primary_key_names()
                                .cloned()
                                .collect::<HashSet<_>>()
                        });
                    tags_in_table.extend(table_builder.tags().cloned());
                } else {
                    // physical table not exist, build create expr according to logical table tags.
                    if let Some(tags) = tables_to_create.get_mut(table_name) {
                        tags.extend(table_builder.tags().cloned());
                    } else {
                        // populate tags for table.
                        tables_to_create.insert(
                            table_name.to_string(),
                            table_builder.tags().cloned().collect(),
                        );
                    }
                }
            }
        }
        todo!()
    }

    /// Builds create table expr from provided tag set.
    fn build_create_table_expr(tags: HashMap<String, HashSet<String>>) -> Vec<CreateTableExpr> {
        todo!()
    }

    /// Builds [AlterTableExpr] by finding new tags.
    fn build_alter_table_expr(table: TableInfoRef, all_tags: HashMap<String, HashSet<String>>) {
        // todo
        // 1. Find new added tags according to `all_tags` and existing table schema
        // 2. Build AlterTableExpr
    }

    /// Finds physical table id for logical table.
    async fn determine_physical_table(
        &self,
        logical_table_name: &str,
        physical_table_name: &Option<String>,
        current_catalog: &str,
        current_schema: &str,
    ) -> error::Result<Option<TableRef>> {
        let logical_table = self
            .catalog_manager
            .table(current_catalog, current_schema, logical_table_name, None)
            .await
            .context(error::CatalogSnafu)?;
        if let Some(logical_table) = logical_table {
            // logical table already exist, just return the physical table
            let logical_table_id = logical_table.table_info().table_id();
            let physical_table_id = self
                .table_route_manager
                .get_physical_table_id(logical_table_id)
                .await
                .context(error::CommonMetaSnafu)?;
            let physical_table = self
                .catalog_manager
                .tables_by_ids(current_catalog, current_schema, &[physical_table_id])
                .await
                .context(error::CatalogSnafu)?
                .swap_remove(0);
            return Ok(Some(physical_table));
        }

        // Logical table not exist, try assign logical table to a physical table.
        let physical_table_name = physical_table_name
            .as_deref()
            .unwrap_or(GREPTIME_PHYSICAL_TABLE);

        self.catalog_manager
            .table(current_catalog, current_schema, physical_table_name, None)
            .await
            .context(error::CatalogSnafu)
    }
}
