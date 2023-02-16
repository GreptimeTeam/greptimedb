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
    use common_error::prelude::BoxedError;
    use common_telemetry::logging::error;
    use script::manager::ScriptManager;
    use snafu::ResultExt;

    use super::*;

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
