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

use catalog::CatalogManagerRef;
use common_error::ext::ErrorExt;
use common_query::Output;
use query::QueryEngineRef;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;

use crate::error::{Error, Result};

type FrontendGrpcQueryHandlerRef = Arc<dyn GrpcQueryHandler<Error = Error> + Send + Sync>;

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

        pub fn start(&self, instance: &Instance) -> Result<()> {
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

    use api::v1::greptime_request::Request;
    use arc_swap::ArcSwap;
    use async_trait::async_trait;
    use common_error::ext::BoxedError;
    use common_meta::rpc::lock::{LockRequest, UnlockRequest};
    use common_telemetry::error;
    use meta_client::client::MetaClientRef;
    use script::manager::ScriptManager;
    use servers::error::InternalSnafu;
    use servers::query_handler::grpc::GrpcQueryHandler;
    use snafu::ResultExt;

    use super::*;
    use crate::instance::Instance;

    /// A placeholder for the real gRPC handler.
    /// It is temporary and will be replaced soon.
    struct DummyHandler;

    impl DummyHandler {
        fn arc() -> Arc<Self> {
            Arc::new(Self {})
        }
    }

    #[async_trait]
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
        script_manager: Arc<ScriptManager<Error>>,
        grpc_handler: ArcSwap<FrontendGrpcQueryHandlerRef>,
        meta_client: Option<MetaClientRef>,
    }

    impl ScriptExecutor {
        pub async fn new(
            catalog_manager: CatalogManagerRef,
            query_engine: QueryEngineRef,
            meta_client: Option<MetaClientRef>,
        ) -> Result<Self> {
            let grpc_handler = DummyHandler::arc();
            Ok(Self {
                grpc_handler: ArcSwap::new(Arc::new(grpc_handler.clone() as _)),
                script_manager: Arc::new(
                    ScriptManager::new(catalog_manager.clone(), grpc_handler, query_engine)
                        .await
                        .context(crate::error::StartScriptManagerSnafu)?,
                ),
                meta_client,
            })
        }

        pub fn start(&self, instance: &Instance) -> Result<()> {
            let handler = Arc::new(instance.clone());
            self.grpc_handler.store(Arc::new(handler.clone() as _));
            self.script_manager
                .start(handler)
                .context(crate::error::StartScriptManagerSnafu)?;
            ScriptManager::register_as_function_provider(self.script_manager.clone());

            Ok(())
        }
        pub async fn insert_script(
            &self,
            query_ctx: QueryContextRef,
            name: &str,
            script: &str,
        ) -> servers::error::Result<()> {
            self.script_manager
                .create_table_if_need(query_ctx.current_catalog())
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

            if let Some(meta_client) = &self.meta_client {
                let lock_req = LockRequest {
                    name: format!("script-{}", name).as_bytes().to_vec(),
                    expire_secs: 3,
                };
                let lock_resullt = meta_client.lock(lock_req).await.map_err(|e| {
                    if e.status_code().should_log_error() {
                        error!(e; "Failed to lock script");
                    }
                    InternalSnafu {
                        err_msg: e.to_string(),
                    }
                    .build()
                })?;
                let key = lock_resullt.key;

                let result = self.insert_or_update_script(query_ctx, name, script).await;

                if let Err(e) = meta_client.unlock(UnlockRequest { key }).await {
                    error!(e; "Failed to unlock script");
                };
                result?;
            } else {
                // TODO: we still need to lock it in standalone mode
                self.insert_or_update_script(query_ctx, name, script)
                    .await?;
            }

            Ok(())
        }

        async fn insert_or_update_script(
            &self,
            query_ctx: QueryContextRef,
            name: &str,
            script: &str,
        ) -> servers::error::Result<()> {
            let (_, version) = self
                .script_manager
                .try_find_script(
                    query_ctx.current_catalog(),
                    query_ctx.current_schema(),
                    name,
                )
                .await
                .map_err(|e| {
                    if e.status_code().should_log_error() {
                        error!(e; "Failed to find script");
                    }
                    BoxedError::new(e)
                })
                .context(servers::error::InsertScriptSnafu { name })?
                .unwrap_or((String::default(), 0));

            let _s = self
                .script_manager
                .insert_and_compile(
                    query_ctx.current_catalog(),
                    query_ctx.current_schema(),
                    name,
                    script,
                    version + 1,
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
            self.script_manager
                .create_table_if_need(query_ctx.current_catalog())
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

            self.script_manager
                .execute(
                    query_ctx.current_catalog(),
                    query_ctx.current_schema(),
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
