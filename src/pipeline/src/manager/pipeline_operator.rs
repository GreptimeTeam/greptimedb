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
use std::time::Instant;

use api::v1::CreateTableExpr;
use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use common_catalog::consts::{default_engine, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_telemetry::info;
use datatypes::timestamp::TimestampNanosecond;
use futures::FutureExt;
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::TableRef;

use crate::error::{CatalogSnafu, CreateTableSnafu, PipelineTableNotFoundSnafu, Result};
use crate::manager::{PipelineInfo, PipelineTableRef, PipelineVersion};
use crate::metrics::{
    METRIC_PIPELINE_CREATE_HISTOGRAM, METRIC_PIPELINE_DELETE_HISTOGRAM,
    METRIC_PIPELINE_RETRIEVE_HISTOGRAM,
};
use crate::table::{PipelineTable, PIPELINE_TABLE_NAME};
use crate::Pipeline;

/// PipelineOperator is responsible for managing pipelines.
/// It provides the ability to:
/// - Create a pipeline table if it does not exist
/// - Get a pipeline from the pipeline table
/// - Insert a pipeline into the pipeline table
/// - Compile a pipeline
/// - Add a pipeline table to the cache
/// - Get a pipeline table from the cache
pub struct PipelineOperator {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    tables: RwLock<HashMap<String, PipelineTableRef>>,
}

impl PipelineOperator {
    /// Create a table request for the pipeline table.
    fn create_table_request(&self, catalog: &str) -> RegisterSystemTableRequest {
        let (time_index, primary_keys, column_defs) = PipelineTable::build_pipeline_schema();

        let create_table_expr = CreateTableExpr {
            catalog_name: catalog.to_string(),
            schema_name: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            table_name: PIPELINE_TABLE_NAME.to_string(),
            desc: "GreptimeDB pipeline table for Log".to_string(),
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

    async fn create_pipeline_table_if_not_exists(&self, ctx: QueryContextRef) -> Result<()> {
        let catalog = ctx.current_catalog();

        // exist in cache
        if self.get_pipeline_table_from_cache(catalog).is_some() {
            return Ok(());
        }

        let RegisterSystemTableRequest {
            create_table_expr: mut expr,
            open_hook: _,
        } = self.create_table_request(catalog);

        // exist in catalog, just open
        if let Some(table) = self
            .catalog_manager
            .table(
                &expr.catalog_name,
                &expr.schema_name,
                &expr.table_name,
                Some(&ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            self.add_pipeline_table_to_cache(catalog, table);
            return Ok(());
        }

        // create table
        self.statement_executor
            .create_table_inner(&mut expr, None, ctx.clone())
            .await
            .context(CreateTableSnafu)?;

        let schema = &expr.schema_name;
        let table_name = &expr.table_name;

        // get from catalog
        let table = self
            .catalog_manager
            .table(catalog, schema, table_name, Some(&ctx))
            .await
            .context(CatalogSnafu)?
            .context(PipelineTableNotFoundSnafu)?;

        info!(
            "Created pipelines table {} with table id {}.",
            table.table_info().full_table_name(),
            table.table_info().table_id()
        );

        // put to cache
        self.add_pipeline_table_to_cache(catalog, table);

        Ok(())
    }

    /// Get a pipeline table from the cache.
    pub fn get_pipeline_table_from_cache(&self, catalog: &str) -> Option<PipelineTableRef> {
        self.tables.read().unwrap().get(catalog).cloned()
    }
}

impl PipelineOperator {
    /// Create a new PipelineOperator.
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

    /// Get a pipeline from the pipeline table.
    pub async fn get_pipeline(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Arc<Pipeline>> {
        let schema = query_ctx.current_schema();
        self.create_pipeline_table_if_not_exists(query_ctx.clone())
            .await?;

        let timer = Instant::now();
        self.get_pipeline_table_from_cache(query_ctx.current_catalog())
            .context(PipelineTableNotFoundSnafu)?
            .get_pipeline(&schema, name, version)
            .inspect(|re| {
                METRIC_PIPELINE_RETRIEVE_HISTOGRAM
                    .with_label_values(&[&re.is_ok().to_string()])
                    .observe(timer.elapsed().as_secs_f64())
            })
            .await
    }

    /// Get a original pipeline by name.
    pub async fn get_pipeline_str(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> Result<((String, TimestampNanosecond), String)> {
        let schema = query_ctx.current_schema();
        self.create_pipeline_table_if_not_exists(query_ctx.clone())
            .await?;

        let timer = Instant::now();
        self.get_pipeline_table_from_cache(query_ctx.current_catalog())
            .context(PipelineTableNotFoundSnafu)?
            .get_pipeline_str(&schema, name, version)
            .inspect(|re| {
                METRIC_PIPELINE_RETRIEVE_HISTOGRAM
                    .with_label_values(&[&re.is_ok().to_string()])
                    .observe(timer.elapsed().as_secs_f64())
            })
            .await
    }

    /// Insert a pipeline into the pipeline table.
    pub async fn insert_pipeline(
        &self,
        name: &str,
        content_type: &str,
        pipeline: &str,
        query_ctx: QueryContextRef,
    ) -> Result<PipelineInfo> {
        self.create_pipeline_table_if_not_exists(query_ctx.clone())
            .await?;

        let timer = Instant::now();
        self.get_pipeline_table_from_cache(query_ctx.current_catalog())
            .context(PipelineTableNotFoundSnafu)?
            .insert_and_compile(name, content_type, pipeline)
            .inspect(|re| {
                METRIC_PIPELINE_CREATE_HISTOGRAM
                    .with_label_values(&[&re.is_ok().to_string()])
                    .observe(timer.elapsed().as_secs_f64())
            })
            .await
    }

    /// Delete a pipeline by name from pipeline table.
    pub async fn delete_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> Result<Option<()>> {
        // trigger load pipeline table
        self.create_pipeline_table_if_not_exists(query_ctx.clone())
            .await?;

        let timer = Instant::now();
        self.get_pipeline_table_from_cache(query_ctx.current_catalog())
            .context(PipelineTableNotFoundSnafu)?
            .delete_pipeline(name, version)
            .inspect(|re| {
                METRIC_PIPELINE_DELETE_HISTOGRAM
                    .with_label_values(&[&re.is_ok().to_string()])
                    .observe(timer.elapsed().as_secs_f64())
            })
            .await
    }

    /// Compile a pipeline.
    pub fn build_pipeline(pipeline: &str) -> Result<Pipeline> {
        PipelineTable::compile_pipeline(pipeline)
    }
}
