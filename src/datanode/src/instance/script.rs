use async_trait::async_trait;
use common_query::Output;
use common_telemetry::timer;
use servers::query_handler::ScriptHandler;

use crate::instance::Instance;
use crate::metric;

#[async_trait]
impl ScriptHandler for Instance {
    async fn insert_script(&self, name: &str, script: &str) -> servers::error::Result<()> {
        let _timer = timer!(metric::METRIC_HANDLE_SCRIPTS_ELAPSED);
        self.script_executor.insert_script(name, script).await
    }

    async fn execute_script(&self, name: &str) -> servers::error::Result<Output> {
        let _timer = timer!(metric::METRIC_RUN_SCRIPT_ELAPSED);
        self.script_executor.execute_script(name).await
    }
}
