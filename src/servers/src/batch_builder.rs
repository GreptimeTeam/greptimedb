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

use api::v1::{ColumnDataType, ColumnSchema, SemanticType};
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::key::table_route::TableRouteManagerRef;
use common_query::prelude::{GREPTIME_PHYSICAL_TABLE, GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use operator::schema_helper::{
    ensure_logical_tables_for_metrics, LogicalSchema, LogicalSchemas, SchemaHelper,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::table_name::TableName;
use table::TableRef;

use crate::error;
use crate::prom_row_builder::{PromCtx, TableBuilder};

#[allow(dead_code)]
pub struct MetricsBatchBuilder {
    schema_helper: SchemaHelper,
}

impl MetricsBatchBuilder {
    /// Detected the DDL requirements according to the staged table rows.
    pub async fn create_or_alter_physical_tables(
        &self,
        tables: &HashMap<PromCtx, HashMap<String, TableBuilder>>,
        query_ctx: &QueryContextRef,
    ) -> error::Result<()> {
        // Physical table name -> logical tables -> tags in logical table
        let mut tags: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::new();
        let catalog = query_ctx.current_catalog();
        let schema = query_ctx.current_schema();

        for (ctx, tables) in tables {
            for (logical_table_name, table_builder) in tables {
                let physical_table_name = self
                    .determine_physical_table_name(
                        logical_table_name,
                        &ctx.physical_table,
                        catalog,
                        &schema,
                    )
                    .await?;
                tags.entry(physical_table_name)
                    .or_default()
                    .entry(logical_table_name.clone())
                    .or_default()
                    .extend(table_builder.tags().cloned());
            }
        }
        let logical_schemas = tags_to_logical_schemas(tags);
        ensure_logical_tables_for_metrics(&self.schema_helper, &logical_schemas, query_ctx)
            .await
            .context(error::OperatorSnafu)?;

        Ok(())
    }

    /// Finds physical table id for logical table.
    async fn determine_physical_table_name(
        &self,
        logical_table_name: &str,
        physical_table_name: &Option<String>,
        catalog: &str,
        schema: &str,
    ) -> error::Result<String> {
        let logical_table = self
            .schema_helper
            .get_table(catalog, schema, logical_table_name)
            .await
            .context(error::OperatorSnafu)?;
        if let Some(logical_table) = logical_table {
            // logical table already exist, just return the physical table
            let logical_table_id = logical_table.table_info().table_id();
            let physical_table_id = self
                .schema_helper
                .table_route_manager()
                .get_physical_table_id(logical_table_id)
                .await
                .context(error::CommonMetaSnafu)?;
            let physical_table = self
                .schema_helper
                .catalog_manager()
                .tables_by_ids(catalog, schema, &[physical_table_id])
                .await
                .context(error::CatalogSnafu)?
                .swap_remove(0);
            return Ok(physical_table.table_info().name.clone());
        }

        // Logical table not exist, try assign logical table to a physical table.
        let physical_table_name = physical_table_name
            .as_deref()
            .unwrap_or(GREPTIME_PHYSICAL_TABLE);
        Ok(physical_table_name.to_string())
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

fn tags_to_logical_schemas(
    tags: HashMap<String, HashMap<String, HashSet<String>>>,
) -> LogicalSchemas {
    let schemas: HashMap<String, Vec<LogicalSchema>> = tags
        .into_iter()
        .map(|(physical, logical_tables)| {
            let schemas: Vec<_> = logical_tables
                .into_iter()
                .map(|(logical, tags)| {
                    let mut columns: Vec<_> = tags
                        .into_iter()
                        .map(|tag_name| ColumnSchema {
                            column_name: tag_name,
                            datatype: ColumnDataType::String as i32,
                            semantic_type: SemanticType::Tag as i32,
                            ..Default::default()
                        })
                        .collect();
                    columns.push(ColumnSchema {
                        column_name: GREPTIME_TIMESTAMP.to_string(),
                        datatype: ColumnDataType::TimestampNanosecond as i32,
                        semantic_type: SemanticType::Timestamp as i32,
                        ..Default::default()
                    });
                    columns.push(ColumnSchema {
                        column_name: GREPTIME_VALUE.to_string(),
                        datatype: ColumnDataType::Float64 as i32,
                        semantic_type: SemanticType::Field as i32,
                        ..Default::default()
                    });
                    LogicalSchema {
                        name: logical,
                        columns,
                    }
                })
                .collect();
            (physical, schemas)
        })
        .collect();

    LogicalSchemas { schemas }
}
