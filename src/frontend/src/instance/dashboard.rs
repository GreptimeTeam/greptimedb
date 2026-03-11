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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema as PbColumnSchema, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
use async_trait::async_trait;
use common_catalog::consts::{DEFAULT_PRIVATE_SCHEMA_NAME, default_engine};
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_recordbatch::util as record_util;
use common_telemetry::info;
use common_time::FOREVER;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::col;
use datafusion::sql::TableReference;
use datafusion_expr::{DmlStatement, LogicalPlan, lit};
use datatypes::arrow::array::{Array, AsArray};
use servers::error::{
    CatalogSnafu, CollectRecordbatchSnafu, DataFusionSnafu, ExecuteQuerySnafu, NotSupportedSnafu,
    TableNotFoundSnafu,
};
use servers::query_handler::DashboardDefinition;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use table::TableRef;
use table::metadata::TableInfo;
use table::requests::TTL_KEY;
use table::table::adapter::DfTableProviderAdapter;

use crate::instance::Instance;

pub const DASHBOARD_TABLE_NAME: &str = "dashboard";
pub const DASHBOARD_TABLE_NAME_COLUMN_NAME: &str = "name";
pub const DASHBOARD_TABLE_DEFINITION_COLUMN_NAME: &str = "definition";
pub const DASHBOARD_TABLE_CREATED_AT_COLUMN_NAME: &str = "created_at";

impl Instance {
    /// Build a schema for dashboard table.
    /// Returns the (time index, primary keys, column) definitions.
    fn build_dashboard_schema() -> (String, Vec<String>, Vec<ColumnDef>) {
        (
            DASHBOARD_TABLE_CREATED_AT_COLUMN_NAME.to_string(),
            vec![DASHBOARD_TABLE_NAME_COLUMN_NAME.to_string()],
            vec![
                ColumnDef {
                    name: DASHBOARD_TABLE_NAME_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: String::new(),
                    datatype_extension: None,
                    options: None,
                },
                ColumnDef {
                    name: DASHBOARD_TABLE_DEFINITION_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: String::new(),
                    datatype_extension: None,
                    options: None,
                },
                ColumnDef {
                    name: DASHBOARD_TABLE_CREATED_AT_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::TimestampNanosecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Timestamp as i32,
                    comment: String::new(),
                    datatype_extension: None,
                    options: None,
                },
            ],
        )
    }

    /// Build a column schemas for inserting a row into the dashboard table.
    fn build_dashboard_insert_column_schemas() -> Vec<PbColumnSchema> {
        vec![
            PbColumnSchema {
                column_name: DASHBOARD_TABLE_NAME_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: DASHBOARD_TABLE_DEFINITION_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: DASHBOARD_TABLE_CREATED_AT_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampNanosecond.into(),
                semantic_type: SemanticType::Timestamp.into(),
                ..Default::default()
            },
        ]
    }

    fn dashboard_query_ctx(table_info: &TableInfo) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(table_info.catalog_name.clone())
            .current_schema(table_info.schema_name.clone())
            .build()
            .into()
    }

    async fn create_dashboard_table_if_not_exists(
        &self,
        ctx: QueryContextRef,
    ) -> servers::error::Result<TableRef> {
        let catalog = ctx.current_catalog();

        if let Some(table) = self
            .catalog_manager
            .table(
                catalog,
                DEFAULT_PRIVATE_SCHEMA_NAME,
                DASHBOARD_TABLE_NAME,
                Some(&ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            return Ok(table);
        }

        let (time_index, primary_keys, column_defs) = Self::build_dashboard_schema();

        let mut table_options = HashMap::new();
        table_options.insert(TTL_KEY.to_string(), FOREVER.to_string());

        let mut create_table_expr = api::v1::CreateTableExpr {
            catalog_name: catalog.to_string(),
            schema_name: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            table_name: DASHBOARD_TABLE_NAME.to_string(),
            desc: "GreptimeDB dashboard table".to_string(),
            column_defs,
            time_index,
            primary_keys,
            create_if_not_exists: true,
            table_options,
            table_id: None,
            engine: default_engine().to_string(),
        };

        self.statement_executor
            .create_table_inner(&mut create_table_expr, None, ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        let table = self
            .catalog_manager
            .table(
                catalog,
                DEFAULT_PRIVATE_SCHEMA_NAME,
                DASHBOARD_TABLE_NAME,
                Some(&ctx),
            )
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu {
                catalog: catalog.to_string(),
                schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
                table: DASHBOARD_TABLE_NAME.to_string(),
            })?;

        Ok(table)
    }

    /// Insert a dashboard into the dashboard table.
    async fn insert_dashboard(
        &self,
        name: &str,
        definition: &str,
        query_ctx: QueryContextRef,
    ) -> servers::error::Result<()> {
        let table = self
            .create_dashboard_table_if_not_exists(query_ctx.clone())
            .await?;
        let table_info = table.table_info();

        let insert = RowInsertRequest {
            table_name: DASHBOARD_TABLE_NAME.to_string(),
            rows: Some(Rows {
                schema: Self::build_dashboard_insert_column_schemas(),
                rows: vec![Row {
                    values: vec![
                        ValueData::StringValue(name.to_string()).into(),
                        ValueData::StringValue(definition.to_string()).into(),
                        ValueData::TimestampNanosecondValue(0).into(),
                    ],
                }],
            }),
        };

        let requests = RowInsertRequests {
            inserts: vec![insert],
        };

        let output = self
            .inserter
            .handle_row_inserts(
                requests,
                Self::dashboard_query_ctx(&table_info),
                &self.statement_executor,
                false,
                false,
            )
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        info!(
            "Insert dashboard success, name: {}, table: {}, output: {:?}",
            name,
            table_info.full_table_name(),
            output
        );

        Ok(())
    }

    /// List all dashboards.
    async fn list_dashboards(
        &self,
        query_ctx: QueryContextRef,
    ) -> servers::error::Result<Vec<DashboardDefinition>> {
        let table = if let Some(table) = self
            .catalog_manager
            .table(
                query_ctx.current_catalog(),
                DEFAULT_PRIVATE_SCHEMA_NAME,
                DASHBOARD_TABLE_NAME,
                Some(&query_ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            table
        } else {
            return Ok(vec![]);
        };

        let table_info = table.table_info();

        let dataframe = self
            .query_engine
            .read_table(table.clone())
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        let dataframe = dataframe
            .select_columns(&[
                DASHBOARD_TABLE_NAME_COLUMN_NAME,
                DASHBOARD_TABLE_DEFINITION_COLUMN_NAME,
            ])
            .context(DataFusionSnafu)?;

        let plan = dataframe.into_parts().1;

        let output = self
            .query_engine
            .execute(plan, Self::dashboard_query_ctx(&table_info))
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(record_batches) => record_batches.as_stream(),
            _ => unreachable!(),
        };

        let records = record_util::collect(stream)
            .await
            .context(CollectRecordbatchSnafu)?;

        let mut dashboards = Vec::new();

        for r in &records {
            let name_column = r.column(0);
            let definition_column = r.column(1);

            let name = name_column
                .as_string_opt::<i32>()
                .context(NotSupportedSnafu {
                    feat: "Invalid data type for greptime_private.dashboard.name",
                })?;

            let definition =
                definition_column
                    .as_string_opt::<i32>()
                    .context(NotSupportedSnafu {
                        feat: "Invalid data type for greptime_private.dashboard.definition",
                    })?;

            for i in 0..name.len() {
                dashboards.push(DashboardDefinition {
                    name: name.value(i).to_string(),
                    definition: definition.value(i).to_string(),
                });
            }
        }

        Ok(dashboards)
    }

    /// Delete a dashboard by name.
    async fn delete_dashboard(
        &self,
        name: &str,
        query_ctx: QueryContextRef,
    ) -> servers::error::Result<()> {
        let table = self
            .create_dashboard_table_if_not_exists(query_ctx.clone())
            .await?;
        let table_info = table.table_info();

        let dataframe = self
            .query_engine
            .read_table(table.clone())
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        let name_condition = col(DASHBOARD_TABLE_NAME_COLUMN_NAME).eq(lit(name));

        let dataframe = dataframe.filter(name_condition).context(DataFusionSnafu)?;

        let table_name = TableReference::full(
            table_info.catalog_name.clone(),
            table_info.schema_name.clone(),
            table_info.name.clone(),
        );

        let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        let stmt = DmlStatement::new(
            table_name,
            table_source,
            datafusion_expr::WriteOp::Delete,
            Arc::new(dataframe.into_parts().1),
        );

        let plan = LogicalPlan::Dml(stmt);

        let output = self
            .query_engine
            .execute(plan, Self::dashboard_query_ctx(&table_info))
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        info!(
            "Delete dashboard success, name: {}, table: {}, output: {:?}",
            name,
            table_info.full_table_name(),
            output
        );

        Ok(())
    }
}

#[async_trait]
impl servers::query_handler::DashboardHandler for Instance {
    async fn save(
        &self,
        name: &str,
        definition: &str,
        ctx: QueryContextRef,
    ) -> servers::error::Result<()> {
        self.insert_dashboard(name, definition, ctx).await
    }

    async fn list(&self, ctx: QueryContextRef) -> servers::error::Result<Vec<DashboardDefinition>> {
        self.list_dashboards(ctx).await
    }

    async fn delete(&self, name: &str, ctx: QueryContextRef) -> servers::error::Result<()> {
        self.delete_dashboard(name, ctx).await
    }
}
