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

use std::time::Duration;

use common_telemetry::info;
use snafu::ResultExt;
use sqlx::{MySql, Pool, Row};

use super::wait::wait_condition_fn;
use crate::error;

/// Fetches the state of a procedure.
pub async fn procedure_state(e: &Pool<MySql>, procedure_id: &str) -> String {
    let sql = format!("admin procedure_state(\"{procedure_id}\");");
    let result = sqlx::query(&sql)
        .fetch_one(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })
        .unwrap();
    result.try_get(0).unwrap()
}

/// Waits for a procedure to finish within a specified timeout period.
pub async fn wait_for_procedure_finish(
    greptime: &Pool<MySql>,
    timeout: Duration,
    procedure_id: String,
) {
    wait_condition_fn(
        timeout,
        || {
            let greptime = greptime.clone();
            let procedure_id = procedure_id.clone();
            Box::pin(async move { procedure_state(&greptime, &procedure_id).await })
        },
        |output| {
            info!("Procedure({procedure_id}) state: {:?}", output);
            output.to_lowercase().contains("done")
        },
        Duration::from_secs(5),
    )
    .await
}
