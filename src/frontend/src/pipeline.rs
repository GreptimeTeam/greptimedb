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

use api::v1::ddl_request::Expr;
use api::v1::greptime_request::Request;
use api::v1::{CreateTableExpr, DdlRequest};
use arc_swap::ArcSwap;
use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use common_catalog::consts::{default_engine, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_error::ext::{BoxedError, ErrorExt};
use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::{error, info};
use pipeline::table::{PipelineTable, PipelineTableRef};
use pipeline::{GreptimeTransformer, Pipeline};
use query::QueryEngineRef;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use table::TableRef;

use crate::error::{
    CatalogSnafu, Error, GetPipelineSnafu, InsertPipelineSnafu, Result, TableNotFoundSnafu,
};
use crate::instance::Instance;

type FrontendGrpcQueryHandlerRef = Arc<dyn GrpcQueryHandler<Error = Error> + Send + Sync>;

pub const PIPELINE_TABLE_NAME: &str = "pipelines";

struct DummyHandler;

impl DummyHandler {
    pub fn arc() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait::async_trait]
impl GrpcQueryHandler for DummyHandler {
    type Error = Error;

    async fn do_query(
        &self,
        _query: Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
        unreachable!();
    }
}

pub struct PipelineOperator {
    grpc_handler: ArcSwap<FrontendGrpcQueryHandlerRef>,
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    tables: RwLock<HashMap<String, PipelineTableRef<Error>>>,
}

impl PipelineOperator {
    pub fn create_table_request(&self, catalog: &str) -> RegisterSystemTableRequest {
        let (time_index, primary_keys, column_defs) =
            PipelineTable::<Error>::build_pipeline_schema();

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
                table,
                self.grpc_handler.load().as_ref().clone(),
                self.query_engine.clone(),
            )),
        );
    }

    pub async fn create_pipeline_table_if_not_exists(&self, catalog: &str) -> Result<()> {
        if self.get_pipeline_table_from_cache(catalog).is_some() {
            return Ok(());
        }

        let RegisterSystemTableRequest {
            create_table_expr: expr,
            open_hook,
        } = self.create_table_request(catalog);

        if let Some(table) = self
            .catalog_manager
            .table(&expr.catalog_name, &expr.schema_name, &expr.table_name)
            .await
            .context(CatalogSnafu)?
        {
            if let Some(open_hook) = open_hook {
                (open_hook)(table.clone()).await.context(CatalogSnafu)?;
            }

            self.add_pipeline_table_to_cache(catalog, table);

            return Ok(());
        }

        let table_name = TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);

        let _ = self
            .grpc_handler
            .load()
            .do_query(
                Request::Ddl(DdlRequest {
                    expr: Some(Expr::CreateTable(expr)),
                }),
                QueryContext::arc(),
            )
            .await?;

        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;

        if let Some(open_hook) = open_hook {
            (open_hook)(table.clone()).await.context(CatalogSnafu)?;
        }

        info!(
            "Created scripts table {}.",
            table.table_info().full_table_name()
        );

        self.add_pipeline_table_to_cache(catalog, table);

        Ok(())
    }

    pub fn get_pipeline_table_from_cache(&self, catalog: &str) -> Option<PipelineTableRef<Error>> {
        // FIXME (qtang): we should impl this
        self.tables.read().unwrap().get(catalog).cloned()
    }

    pub async fn insert_and_compile(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> Result<()> {
        let _compiled_pipeline = PipelineTable::<Error>::compile_pipeline(name, pipeline)
            .map_err(BoxedError::new)
            .context(InsertPipelineSnafu { name })?;
        self.get_pipeline_table_from_cache(catalog)
            // FIXME (qtang): we should add error handling here
            .unwrap()
            .insert_pipeline_to_pipeline_table(schema, name, content_type, pipeline)
            .await
            .map_err(|e| {
                if e.status_code().should_log_error() {
                    error!(e; "Failed to insert pipeline");
                }
                BoxedError::new(e)
            })
            .context(InsertPipelineSnafu { name })?;
        Ok(())
    }
}

impl PipelineOperator {
    pub fn new(catalog_manager: CatalogManagerRef, query_engine: QueryEngineRef) -> Self {
        let grpc_handler = ArcSwap::new(Arc::new(DummyHandler::arc() as _));
        Self {
            grpc_handler,
            catalog_manager,
            tables: RwLock::new(HashMap::new()),
            query_engine,
        }
    }

    pub fn start(&self, instance: &Instance) {
        self.grpc_handler
            .store(Arc::new(Arc::new(instance.clone()) as _));
    }

    pub async fn get_pipeline(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
    ) -> Result<Pipeline<GreptimeTransformer>> {
        let _s = self
            .create_pipeline_table_if_not_exists(query_ctx.current_catalog())
            .await;
        let table = self
            .get_pipeline_table_from_cache(query_ctx.current_catalog())
            .unwrap()
            .find_pipeline_by_name(query_ctx.current_schema(), name)
            .await
            .map_err(BoxedError::new)
            .context(GetPipelineSnafu { name })?;
        PipelineTable::<Error>::compile_pipeline(name, &table)
            .map_err(BoxedError::new)
            .context(InsertPipelineSnafu { name })
    }

    pub async fn insert_pipeline(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> servers::error::Result<()> {
        self.create_pipeline_table_if_not_exists(query_ctx.current_catalog())
            .await
            .map_err(|e| {
                if e.status_code().should_log_error() {
                    error!(e; "Failed to create pipeline table");
                }

                servers::error::InternalSnafu {
                    err_msg: e.to_string(),
                }
                .build()
            })?;

        self.insert_and_compile(
            query_ctx.current_catalog(),
            query_ctx.current_schema(),
            name,
            content_type,
            pipeline,
        )
        .await
        .map_err(|e| {
            if e.status_code().should_log_error() {
                error!(e; "Failed to insert pipeline");
            }

            BoxedError::new(e)
        })
        .context(servers::error::InsertPipelineSnafu { name })?;

        Ok(())
    }
}
