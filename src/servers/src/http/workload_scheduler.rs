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

use std::num::NonZeroU32;

use axum::Json;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::error::{InvalidParameterSnafu, ParseJsonSnafu, Result};

#[axum_macros::debug_handler]
pub(super) async fn set_enabled_handler(body: Bytes) -> Result<impl IntoResponse> {
    let enabled: bool = serde_json::from_slice(&body).context(ParseJsonSnafu)?;
    ensure!(
        common_runtime::set_workload_scheduler_enabled(enabled),
        InvalidParameterSnafu {
            reason: "workload scheduler was not constructed at startup",
        }
    );
    let change_note = format!("Workload scheduler enabled={enabled}");
    Ok((StatusCode::OK, change_note))
}

#[derive(Debug, Deserialize)]
struct SchedulerWeightsDto {
    query: NonZeroU32,
    write: NonZeroU32,
}

#[axum_macros::debug_handler]
pub(super) async fn set_weights_handler(body: Bytes) -> Result<impl IntoResponse> {
    let weights: SchedulerWeightsDto = serde_json::from_slice(&body).context(ParseJsonSnafu)?;
    ensure!(
        common_runtime::set_workload_scheduler_weights(weights.query, weights.write),
        InvalidParameterSnafu {
            reason: "workload scheduler was not constructed at startup",
        }
    );
    let change_note = format!(
        "Workload scheduler weights query={}, write={}",
        weights.query, weights.write
    );
    Ok((StatusCode::OK, change_note))
}

/// Per-class scheduler status exposed by the HTTP API.
#[derive(Debug, Serialize)]
struct ClassStatusDto {
    weight: u32,
    polls: u64,
}

/// Point-in-time workload scheduler status. Scheduler class fields are omitted
/// when the scheduler was not constructed at startup or the corresponding
/// class is unavailable.
#[derive(Debug, Serialize)]
struct SchedulerStatusDto {
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<ClassStatusDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write: Option<ClassStatusDto>,
}

/// Returns the current workload scheduler state and query/write weights and
/// poll counters. Always returns 200, with `enabled=false` when the scheduler
/// is dynamically disabled.
#[axum_macros::debug_handler]
pub(super) async fn get_status_handler() -> Result<impl IntoResponse> {
    let enabled = common_runtime::workload_scheduler_enabled();
    let Some(stats) = common_runtime::workload_scheduler_stats() else {
        return Ok(Json(SchedulerStatusDto {
            enabled: false,
            query: None,
            write: None,
        }));
    };

    let mut query = None;
    let mut write = None;
    for (class, class_stats) in &stats.classes {
        let status = ClassStatusDto {
            weight: class_stats.weight,
            polls: class_stats.polls,
        };
        match class.id() {
            1 => query = Some(status),
            2 => write = Some(status),
            _ => {}
        }
    }

    Ok(Json(SchedulerStatusDto {
        enabled,
        query,
        write,
    }))
}
