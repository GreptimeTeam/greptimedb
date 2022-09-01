use query::Output;
use query::QueryEngineRef;

#[cfg(not(feature = "python"))]
mod dummy {
    use super::*;

    pub struct ScriptExecutor;

    impl ScriptExecutor {
        pub fn new(_query_engine: QueryEngineRef) -> Self {
            Self {}
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
    use script::{
        engine::{CompileContext, EvalContext, Script, ScriptEngine},
        python::PyEngine,
    };
    use snafu::ResultExt;

    use super::*;

    pub struct ScriptExecutor {
        py_engine: PyEngine,
    }

    impl ScriptExecutor {
        pub fn new(query_engine: QueryEngineRef) -> Self {
            Self {
                py_engine: PyEngine::new(query_engine),
            }
        }

        pub async fn execute_script(&self, script: &str) -> servers::error::Result<Output> {
            let py_script = self
                .py_engine
                .compile(script, CompileContext::default())
                .await
                .map_err(|e| {
                    error!(e; "Instance failed to execute script");
                    BoxedError::new(e)
                })
                .context(servers::error::ExecuteScriptSnafu { script })?;

            py_script
                .evaluate(EvalContext::default())
                .await
                .map_err(|e| {
                    error!(e; "Instance failed to execute script");
                    BoxedError::new(e)
                })
                .context(servers::error::ExecuteScriptSnafu { script })
        }
    }
}

#[cfg(not(feature = "python"))]
pub use self::dummy::*;
#[cfg(feature = "python")]
pub use self::python::*;
