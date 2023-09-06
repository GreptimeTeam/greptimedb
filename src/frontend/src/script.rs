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

use crate::error::Result;

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

        pub async fn start(&self) -> Result<()> {
            Ok(())
        }

        pub async fn insert_script(
            &self,
            _schema: &str,
            _name: &str,
            _script: &str,
        ) -> servers::error::Result<()> {
            servers::error::NotSupportedSnafu { feat: "script" }.fail()
        }

        pub async fn execute_script(
            &self,
            _schema: &str,
            _name: &str,
            _params: HashMap<String, String>,
        ) -> servers::error::Result<Output> {
            servers::error::NotSupportedSnafu { feat: "script" }.fail()
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use api::v1::ddl_request::Expr;
    use api::v1::greptime_request::Request;
    use api::v1::{CreateTableExpr, DdlRequest};
    use catalog::RegisterSystemTableRequest;
    use common_error::ext::BoxedError;
    use common_meta::table_name::TableName;
    use common_telemetry::logging::error;
    use script::manager::ScriptManager;
    use servers::query_handler::grpc::GrpcQueryHandler;
    use session::context::QueryContext;
    use snafu::{OptionExt, ResultExt};
    use table::requests::CreateTableRequest;

    use super::*;
    use crate::error::{CatalogSnafu, InvalidSystemTableDefSnafu, TableNotFoundSnafu};
    use crate::expr_factory;
    use crate::instance::Instance;

    pub struct ScriptExecutor {
        script_manager: ScriptManager,
    }

    impl ScriptExecutor {
        pub async fn new(
            catalog_manager: CatalogManagerRef,
            query_engine: QueryEngineRef,
        ) -> Result<Self> {
            Ok(Self {
                script_manager: ScriptManager::new(catalog_manager, query_engine)
                    .await
                    .context(crate::error::StartScriptManagerSnafu)?,
            })
        }

        pub async fn start(&self, instance: &Instance) -> Result<()> {
            let RegisterSystemTableRequest {
                create_table_request: request,
                open_hook,
            } = self.script_manager.create_table_request();

            if let Some(table) = instance
                .catalog_manager()
                .table(
                    &request.catalog_name,
                    &request.schema_name,
                    &request.table_name,
                )
                .await
                .context(CatalogSnafu)?
            {
                if let Some(open_hook) = open_hook {
                    (open_hook)(table).await.context(CatalogSnafu)?;
                }

                return Ok(());
            }

            let table_name = TableName::new(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            );

            let expr = Self::create_table_expr(request)?;

            let _ = instance
                .do_query(
                    Request::Ddl(DdlRequest {
                        expr: Some(Expr::CreateTable(expr)),
                    }),
                    QueryContext::arc(),
                )
                .await?;

            let table = instance
                .catalog_manager()
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
                (open_hook)(table).await.context(CatalogSnafu)?;
            }

            Ok(())
        }

        fn create_table_expr(request: CreateTableRequest) -> Result<CreateTableExpr> {
            let column_schemas = request.schema.column_schemas;

            let time_index = column_schemas
                .iter()
                .find_map(|x| {
                    if x.is_time_index() {
                        Some(x.name.clone())
                    } else {
                        None
                    }
                })
                .context(InvalidSystemTableDefSnafu {
                    err_msg: "Time index is not defined.",
                })?;

            let primary_keys = request
                .primary_key_indices
                .iter()
                // Indexing has to be safe because the create script table request is pre-defined.
                .map(|i| column_schemas[*i].name.clone())
                .collect::<Vec<_>>();

            let column_defs = expr_factory::column_schemas_to_defs(column_schemas, &primary_keys)?;

            Ok(CreateTableExpr {
                catalog_name: request.catalog_name,
                schema_name: request.schema_name,
                table_name: request.table_name,
                desc: request.desc.unwrap_or_default(),
                column_defs,
                time_index,
                primary_keys,
                create_if_not_exists: request.create_if_not_exists,
                table_options: (&request.table_options).into(),
                table_id: None, // Should and will be assigned by Meta.
                region_numbers: vec![0],
                engine: request.engine,
            })
        }

        pub async fn insert_script(
            &self,
            schema: &str,
            name: &str,
            script: &str,
        ) -> servers::error::Result<()> {
            let _s = self
                .script_manager
                .insert_and_compile(schema, name, script)
                .await
                .map_err(|e| {
                    error!(e; "Instance failed to insert script");
                    BoxedError::new(e)
                })
                .context(servers::error::InsertScriptSnafu { name })?;

            Ok(())
        }

        pub async fn execute_script(
            &self,
            schema: &str,
            name: &str,
            params: HashMap<String, String>,
        ) -> servers::error::Result<Output> {
            self.script_manager
                .execute(schema, name, params)
                .await
                .map_err(|e| {
                    error!(e; "Instance failed to execute script");
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
