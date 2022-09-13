use catalog::CatalogManagerRef;
use query::Output;
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
            _name: &str,
            _script: &str,
        ) -> servers::error::Result<()> {
            servers::error::NotSupportedSnafu { feat: "script" }.fail()
        }

        pub async fn execute_script(&self, _script: &str) -> servers::error::Result<Output> {
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

        pub async fn insert_script(&self, name: &str, script: &str) -> servers::error::Result<()> {
            let _s = self
                .script_manager
                .insert_and_compile(name, script)
                .await
                .map_err(|e| {
                    error!(e; "Instance failed to insert script");
                    BoxedError::new(e)
                })
                .context(servers::error::InsertScriptSnafu { name })?;

            Ok(())
        }

        pub async fn execute_script(&self, name: &str) -> servers::error::Result<Output> {
            self.script_manager
                .execute(name)
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
