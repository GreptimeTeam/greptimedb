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

use std::fmt::Write;
use std::time::Duration;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;

use crate::error::Result;

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(default)]
pub struct DumpQuery {
    seconds: u64,
    path: String,
}

impl Default for DumpQuery {
    fn default() -> DumpQuery {
        DumpQuery {
            seconds: 5,
            path: "/tmp/greptimedb/dump-tasks.txt".to_string(),
        }
    }
}

#[axum_macros::debug_handler]
pub async fn dump_tasks_handler(Query(req): Query<DumpQuery>) -> Result<impl IntoResponse> {
    common_telemetry::info!("Dump start, request: {:?}", req);

    let handle = Handle::current();
    let Ok(mut file) = tokio::fs::File::create(&req.path).await.inspect_err(|e| {
        common_telemetry::error!(e; "Failed to open to {}", req.path);
    }) else {
        return Ok((StatusCode::INTERNAL_SERVER_ERROR, "Failed to open file."));
    };
    if let Ok(dump) = tokio::time::timeout(Duration::from_secs(req.seconds), handle.dump()).await {
        for (i, task) in dump.tasks().iter().enumerate() {
            let trace = task.trace();
            let mut lines = String::new();
            writeln!(lines, "TASK {i}:").unwrap();
            writeln!(lines, "{trace}\n").unwrap();
            if let Err(e) = file.write_all(lines.as_bytes()).await {
                common_telemetry::error!(e; "Failed to open to {}", req.path);
                return Ok((StatusCode::INTERNAL_SERVER_ERROR, "Failed to write file."));
            }
        }
    } else {
        common_telemetry::info!("Dump tasks timeout.");
        return Ok((StatusCode::REQUEST_TIMEOUT, "Dump tasks timeout."));
    }

    common_telemetry::info!("Dump tasks done.");
    Ok((StatusCode::OK, "Dump tasks done."))
}
