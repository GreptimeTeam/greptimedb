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
use std::sync::{Arc, RwLock};

use api::v1::CreateTableExpr;
use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use common_catalog::consts::{default_engine, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_telemetry::info;
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use query::QueryEngineRef;
use session::context::{QueryContext, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use table::TableRef;

use crate::error::{CatalogSnafu, CreateTableSnafu, PipelineTableNotFoundSnafu, Result};
use crate::table::{PipelineTable, PipelineTableRef};
use crate::{GreptimeTransformer, Pipeline};

pub const PIPELINE_TABLE_NAME: &str = "pipelines";

pub struct PipelineOperator {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    tables: RwLock<HashMap<String, PipelineTableRef>>,
}

impl PipelineOperator {
    pub fn create_table_request(&self, catalog: &str) -> RegisterSystemTableRequest {
        let (time_index, primary_keys, column_defs) = PipelineTable::build_pipeline_schema();

        let create_table_expr = CreateTableExpr {
            catalog_name: catalog.to_string(),
            schema_name: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            table_name: PIPELINE_TABLE_NAME.to_string(),
            desc: "GreptimeDB scripts table for Python".to_string(),
            column_defs,
            time_index,
            primary_keys,
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: None, // Should and will be assigned by Meta.
            engine: default_engine().to_string(),
        };

        RegisterSystemTableRequest {
            create_table_expr,
            open_hook: None,
        }
    }

    fn add_pipeline_table_to_cache(&self, catalog: &str, table: TableRef) {
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(catalog) {
            return;
        }
        tables.insert(
            catalog.to_string(),
            Arc::new(PipelineTable::new(
                self.inserter.clone(),
                self.statement_executor.clone(),
                table,
                self.query_engine.clone(),
            )),
        );
    }

    async fn create_pipeline_table_if_not_exists(&self, catalog: &str) -> Result<()> {
        if self.get_pipeline_table_from_cache(catalog).is_some() {
            return Ok(());
        }

        let RegisterSystemTableRequest {
            create_table_expr: mut expr,
            open_hook: _,
        } = self.create_table_request(catalog);

        if let Some(table) = self
            .catalog_manager
            .table(&expr.catalog_name, &expr.schema_name, &expr.table_name)
            .await
            .context(CatalogSnafu)?
        {
            self.add_pipeline_table_to_cache(catalog, table);
            return Ok(());
        }

        let schema = expr.schema_name.clone();
        let table_name = expr.table_name.clone();

        self.statement_executor
            .create_table_inner(
                &mut expr,
                None,
                Arc::new(QueryContext::with(catalog, &schema)),
            )
            .await
            .context(CreateTableSnafu)?;

        let table = self
            .catalog_manager
            .table(catalog, &schema, &table_name)
            .await
            .context(CatalogSnafu)?
            .context(PipelineTableNotFoundSnafu)?;

        info!(
            "Created scripts table {}.",
            table.table_info().full_table_name()
        );

        self.add_pipeline_table_to_cache(catalog, table);

        Ok(())
    }

    pub fn get_pipeline_table_from_cache(&self, catalog: &str) -> Option<PipelineTableRef> {
        self.tables.read().unwrap().get(catalog).cloned()
    }

    async fn insert_and_compile(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> Result<Pipeline<GreptimeTransformer>> {
        self.get_pipeline_table_from_cache(catalog)
            .context(PipelineTableNotFoundSnafu)?
            .insert_and_compile(schema, name, content_type, pipeline)
            .await
    }
}

impl PipelineOperator {
    pub fn new(
        inserter: InserterRef,
        statement_executor: StatementExecutorRef,
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
    ) -> Self {
        Self {
            inserter,
            statement_executor,
            catalog_manager,
            tables: RwLock::new(HashMap::new()),
            query_engine,
        }
    }

    pub async fn get_pipeline(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
    ) -> Result<Pipeline<GreptimeTransformer>> {
        self.create_pipeline_table_if_not_exists(query_ctx.current_catalog())
            .await?;
        self.get_pipeline_table_from_cache(query_ctx.current_catalog())
            .context(PipelineTableNotFoundSnafu)?
            .get_pipeline(query_ctx.current_schema(), name)
            .await
    }

    pub async fn insert_pipeline(
        &self,
        name: &str,
        content_type: &str,
        pipeline: &str,
        query_ctx: QueryContextRef,
    ) -> Result<()> {
        self.create_pipeline_table_if_not_exists(query_ctx.current_catalog())
            .await?;

        self.insert_and_compile(
            query_ctx.current_catalog(),
            query_ctx.current_schema(),
            name,
            content_type,
            pipeline,
        )
        .await
        .map(|_| ())
    }
}
