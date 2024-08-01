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

use catalog::CatalogManagerRef;
use common_query::Output;
use query::QueryEngineRef;
use session::context::QueryContextRef;

use crate::error::Result;
use crate::instance::Instance;

#[cfg(not(feature = "python"))]
mod dummy {
    use super::*;

    pub struct ScriptExecutor;

    impl ScriptExecutor {
        pub async fn new(
            _catalog_manager: CatalogManagerRef,
            _query_engine: QueryEngineRef,
        ) -> Result<Self> {
            Ok(Self {})
        }

        pub fn start(&self, _instance: &Instance) -> Result<()> {
            Ok(())
        }

        pub async fn insert_script(
            &self,
            _query_ctx: QueryContextRef,
            _name: &str,
            _script: &str,
        ) -> servers::error::Result<()> {
            servers::error::NotSupportedSnafu { feat: "script" }.fail()
        }

        pub async fn execute_script(
            &self,
            _query_ctx: QueryContextRef,
            _name: &str,
            _params: HashMap<String, String>,
        ) -> servers::error::Result<Output> {
            servers::error::NotSupportedSnafu { feat: "script" }.fail()
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use std::sync::Arc;

    use api::v1::ddl_request::Expr;
    use api::v1::greptime_request::Request;
    use api::v1::DdlRequest;
    use arc_swap::ArcSwap;
    use catalog::RegisterSystemTableRequest;
    use common_error::ext::{BoxedError, ErrorExt};
    use common_telemetry::{error, info};
    use script::manager::ScriptManager;
    use servers::query_handler::grpc::GrpcQueryHandler;
    use session::context::QueryContext;
    use snafu::{OptionExt, ResultExt};
    use table::table_name::TableName;

    use super::*;
    use crate::error::{CatalogSnafu, Error, TableNotFoundSnafu};

    type FrontendGrpcQueryHandlerRef = Arc<dyn GrpcQueryHandler<Error = Error> + Send + Sync>;

    /// A placeholder for the real gRPC handler.
    /// It is temporary and will be replaced soon.
    struct DummyHandler;

    impl DummyHandler {
        fn arc() -> Arc<Self> {
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

    pub struct ScriptExecutor {
        script_manager: ScriptManager<Error>,
        grpc_handler: ArcSwap<FrontendGrpcQueryHandlerRef>,
        catalog_manager: CatalogManagerRef,
    }

    impl ScriptExecutor {
        pub async fn new(
            catalog_manager: CatalogManagerRef,
            query_engine: QueryEngineRef,
        ) -> Result<Self> {
            let grpc_handler = DummyHandler::arc();
            Ok(Self {
                grpc_handler: ArcSwap::new(Arc::new(grpc_handler.clone() as _)),
                script_manager: ScriptManager::new(grpc_handler as _, query_engine)
                    .await
                    .context(crate::error::StartScriptManagerSnafu)?,
                catalog_manager,
            })
        }

        pub fn start(&self, instance: &Instance) -> Result<()> {
            let handler = Arc::new(instance.clone());
            self.grpc_handler.store(Arc::new(handler.clone() as _));
            self.script_manager
                .start(handler)
                .context(crate::error::StartScriptManagerSnafu)?;

            Ok(())
        }

        /// Create scripts table for the specific catalog if it's not exists.
        /// The function is idempotent and safe to be called more than once for the same catalog
        async fn create_scripts_table_if_need(&self, catalog: &str) -> Result<()> {
            let scripts_table = self.script_manager.get_scripts_table(catalog);

            if scripts_table.is_some() {
                return Ok(());
            }

            let RegisterSystemTableRequest {
                create_table_expr: expr,
                open_hook,
            } = self.script_manager.create_table_request(catalog);

            if let Some(table) = self
                .catalog_manager
                .table(&expr.catalog_name, &expr.schema_name, &expr.table_name)
                .await
                .context(CatalogSnafu)?
            {
                if let Some(open_hook) = open_hook {
                    (open_hook)(table.clone()).await.context(CatalogSnafu)?;
                }

                self.script_manager.insert_scripts_table(catalog, table);

                return Ok(());
            }

            let table_name =
                TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);

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

            self.script_manager.insert_scripts_table(catalog, table);

            Ok(())
        }

        pub async fn insert_script(
            &self,
            query_ctx: QueryContextRef,
            name: &str,
            script: &str,
        ) -> servers::error::Result<()> {
            self.create_scripts_table_if_need(query_ctx.current_catalog())
                .await
                .map_err(|e| {
                    if e.status_code().should_log_error() {
                        error!(e; "Failed to create scripts table");
                    }

                    servers::error::InternalSnafu {
                        err_msg: e.to_string(),
                    }
                    .build()
                })?;

            let _s = self
                .script_manager
                .insert_and_compile(
                    query_ctx.current_catalog(),
                    &query_ctx.current_schema(),
                    name,
                    script,
                )
                .await
                .map_err(|e| {
                    if e.status_code().should_log_error() {
                        error!(e; "Failed to insert script");
                    }

                    BoxedError::new(e)
                })
                .context(servers::error::InsertScriptSnafu { name })?;

            Ok(())
        }

        pub async fn execute_script(
            &self,
            query_ctx: QueryContextRef,
            name: &str,
            params: HashMap<String, String>,
        ) -> servers::error::Result<Output> {
            self.create_scripts_table_if_need(query_ctx.current_catalog())
                .await
                .map_err(|e| {
                    error!(e; "Failed to create scripts table");
                    servers::error::InternalSnafu {
                        err_msg: e.to_string(),
                    }
                    .build()
                })?;

            self.script_manager
                .execute(
                    query_ctx.current_catalog(),
                    &query_ctx.current_schema(),
                    name,
                    params,
                )
                .await
                .map_err(|e| {
                    if e.status_code().should_log_error() {
                        error!(e; "Failed to execute script");
                    }

                    BoxedError::new(e)
                })
                .context(servers::error::ExecuteScriptSnafu { name })
        }
    }
}

#[cfg(not(feature = "python"))]
pub use self::dummy::*;
#[cfg(feature = "python")]
pub use self::python::*;
