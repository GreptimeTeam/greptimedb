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
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::key::table_route::TableRouteManagerRef;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use snafu::ResultExt;
use table::metadata::{TableId, TableInfoRef};
use table::table_name::TableName;
use table::TableRef;

use crate::error;
use crate::prom_row_builder::{PromCtx, TableBuilder};

#[allow(dead_code)]
pub struct MetricsBatchBuilder {
    table_route_manager: TableRouteManagerRef,
    catalog_manager: CatalogManagerRef,
}

impl MetricsBatchBuilder {
    /// Detected the DDL requirements according to the staged table rows.
    pub async fn create_or_alter_physical_tables(
        &self,
        tables: &HashMap<PromCtx, HashMap<String, TableBuilder>>,
        current_catalog: Option<String>,
        current_schema: Option<String>,
    ) -> error::Result<()> {
        // Physical table id -> all tags
        let mut existing_tables: HashMap<TableId, HashSet<_>> = HashMap::new();
        // Physical table name to create -> all tags.
        let mut tables_to_create: HashMap<String, HashSet<String>> = HashMap::new();
        // Logical table name -> physical table ref.
        let mut physical_tables: HashMap<TableName, TableRef> = HashMap::new();

        for (ctx, tables) in tables {
            for (logical_table_name, table_builder) in tables {
                // use session catalog.
                let catalog = current_catalog.as_deref().unwrap_or(DEFAULT_CATALOG_NAME);
                // schema in PromCtx precedes session schema.
                let schema = ctx
                    .schema
                    .as_deref()
                    .or(current_schema.as_deref())
                    .unwrap_or(DEFAULT_SCHEMA_NAME);

                let physical_table = self
                    .determine_physical_table(
                        logical_table_name,
                        &ctx.physical_table,
                        catalog,
                        schema,
                    )
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
                    physical_tables.insert(
                        TableName::new(catalog, schema, logical_table_name),
                        physical_table,
                    );
                } else {
                    // physical table not exist, build create expr according to logical table tags.
                    if let Some(tags) = tables_to_create.get_mut(logical_table_name) {
                        tags.extend(table_builder.tags().cloned());
                    } else {
                        // populate tags for table.
                        tables_to_create.insert(
                            logical_table_name.to_string(),
                            table_builder.tags().cloned().collect(),
                        );
                    }
                }
            }
        }
        todo!()
        // Generate create table and alter table requests and submit DDL procedure to ensure
        // schema compatibility. Store the created table reference into [physical_tables]
    }

    /// Builds create table expr from provided tag set. We should also add the timestamp and field
    /// columns because `tags` only contains primary key.
    fn build_create_table_expr(_tags: HashMap<String, HashSet<String>>) -> Vec<CreateTableExpr> {
        todo!()
    }

    /// Builds [AlterTableExpr] by finding new tags.
    fn build_alter_table_expr(_table: TableInfoRef, _all_tags: HashMap<String, HashSet<String>>) {
        // todo
        // 1. Find new added tags according to `all_tags` and existing table schema
        // 2. Build AlterTableExpr
    }

    /// Finds physical table id for logical table.
    async fn determine_physical_table(
        &self,
        logical_table_name: &str,
        physical_table_name: &Option<String>,
        catalog: &str,
        schema: &str,
    ) -> error::Result<Option<TableRef>> {
        let logical_table = self
            .catalog_manager
            .table(catalog, schema, logical_table_name, None)
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
                .tables_by_ids(catalog, schema, &[physical_table_id])
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
            .table(catalog, schema, physical_table_name, None)
            .await
            .context(error::CatalogSnafu)
    }

    /// Builds [RecordBatch] from rows with primary key encoded.
    /// Potentially we also need to modify the column name of timestamp and value field to
    /// match the schema of physical tables.
    /// Note:
    /// Make sure all logical table and physical table are created when reach here and the mapping
    /// from logical table name to physical table ref is stored in [physical_tables].
    fn rows_to_batch(
        &self,
        current_catalog: Option<String>,
        current_schema: Option<String>,
        table_data: &HashMap<PromCtx, HashMap<String, TableBuilder>>,
        physical_tables: &HashMap<TableName, TableRef>,
    ) -> error::Result<()> {
        for (ctx, tables_in_schema) in table_data {
            for (logical_table_name, _table) in tables_in_schema {
                // use session catalog.
                let catalog = current_catalog.as_deref().unwrap_or(DEFAULT_CATALOG_NAME);
                // schema in PromCtx precedes session schema.
                let schema = ctx
                    .schema
                    .as_deref()
                    .or(current_schema.as_deref())
                    .unwrap_or(DEFAULT_SCHEMA_NAME);
                let logical_table = TableName::new(catalog, schema, logical_table_name);
                let Some(_physical_table) = physical_tables.get(&logical_table) else {
                    // all physical tables must be created when reach here.
                    return error::TableNotFoundSnafu {
                        catalog,
                        schema,
                        table: logical_table_name,
                    }
                    .fail();
                };
            }
        }

        todo!()
    }
}
