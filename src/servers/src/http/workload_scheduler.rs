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

use std::collections::BTreeMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::error::{InvalidParameterSnafu, ParseJsonSnafu, Result};

#[derive(Debug, Deserialize)]
pub struct WeightParams {
    query_weight: u32,
    write_weight: u32,
}

#[derive(Debug, Deserialize)]
pub struct MaxPollsParams {
    max_concurrent_polls: usize,
}

#[axum_macros::debug_handler]
pub async fn set_enabled_handler(body: Bytes) -> Result<impl IntoResponse> {
    let enabled: bool = serde_json::from_slice(&body).context(ParseJsonSnafu)?;
    ensure!(
        common_runtime::set_workload_scheduler_enabled(enabled),
        InvalidParameterSnafu {
            reason: "workload scheduler was not constructed at startup",
        }
    );
    let change_note = format!("Workload scheduler enabled={enabled}");
    info!("{}", change_note);
    Ok((StatusCode::OK, change_note))
}

/// Per-class scheduler counters, mirroring catio's `ClassStats` with the
/// admission wait converted to milliseconds for JSON serialization (catio does
/// not depend on serde).
#[derive(Debug, Serialize)]
pub struct ClassStatusDto {
    weight: u32,
    queued: usize,
    tasks: u64,
    wakes: u64,
    polls: u64,
    completed: u64,
    cancelled: u64,
    admitted: u64,
    total_admission_wait_ms: f64,
}

/// Point-in-time snapshot of the workload scheduler. When it was not
/// constructed at startup, the scheduler-specific fields are omitted. A
/// dynamically disabled scheduler retains its configuration and historical
/// counters.
#[derive(Debug, Serialize)]
pub struct SchedulerStatusDto {
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_concurrent_polls: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_polls: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    classes: Option<BTreeMap<String, ClassStatusDto>>,
}

#[axum_macros::debug_handler]
pub async fn set_weights_handler(Query(params): Query<WeightParams>) -> Result<impl IntoResponse> {
    ensure!(
        common_runtime::workload_scheduler_stats().is_some(),
        InvalidParameterSnafu {
            reason: "workload scheduler was not constructed at startup",
        }
    );
    ensure!(
        common_runtime::set_workload_scheduler_weights(params.query_weight, params.write_weight),
        InvalidParameterSnafu {
            reason: format!(
                "query_weight and write_weight must be greater than zero (got query_weight={}, write_weight={})",
                params.query_weight, params.write_weight
            ),
        }
    );
    let change_note = format!(
        "Workload scheduler weights updated: query_weight={}, write_weight={}",
        params.query_weight, params.write_weight
    );
    info!("{}", change_note);
    Ok((StatusCode::OK, change_note))
}

#[axum_macros::debug_handler]
pub async fn set_max_concurrent_polls_handler(
    Query(params): Query<MaxPollsParams>,
) -> Result<impl IntoResponse> {
    ensure!(
        common_runtime::workload_scheduler_stats().is_some(),
        InvalidParameterSnafu {
            reason: "workload scheduler was not constructed at startup",
        }
    );
    ensure!(
        common_runtime::set_workload_scheduler_max_concurrent_polls(params.max_concurrent_polls),
        InvalidParameterSnafu {
            reason: format!(
                "max_concurrent_polls must be greater than zero (got {})",
                params.max_concurrent_polls
            ),
        }
    );
    let change_note = format!(
        "Workload scheduler max_concurrent_polls updated: {}",
        params.max_concurrent_polls
    );
    info!("{}", change_note);
    Ok((StatusCode::OK, change_note))
}

/// Returns the current workload scheduler state (weights, concurrency limits,
/// and per-class counters). Always returns 200, with `enabled=false` when the
/// scheduler is dynamically disabled. If it was not constructed at startup,
/// scheduler-specific fields are omitted.
#[axum_macros::debug_handler]
pub async fn get_status_handler() -> Result<impl IntoResponse> {
    let enabled = common_runtime::workload_scheduler_enabled();
    let Some(stats) = common_runtime::workload_scheduler_stats() else {
        return Ok(Json(SchedulerStatusDto {
            enabled: false,
            max_concurrent_polls: None,
            active_polls: None,
            classes: None,
        }));
    };

    // Classes are keyed by catio's `TaskClass` (ordered by id). ids 1 and 2 are
    // the query and write classes; any additional class falls back to a
    // `class-{id}` label.
    let classes = stats
        .classes
        .iter()
        .map(|(class, class_stats)| {
            let label = match class.id() {
                1 => "query".to_string(),
                2 => "write".to_string(),
                id => format!("class-{id}"),
            };
            (
                label,
                ClassStatusDto {
                    weight: class_stats.weight,
                    queued: class_stats.queued,
                    tasks: class_stats.tasks,
                    wakes: class_stats.wakes,
                    polls: class_stats.polls,
                    completed: class_stats.completed,
                    cancelled: class_stats.cancelled,
                    admitted: class_stats.admitted,
                    total_admission_wait_ms: class_stats.total_admission_wait.as_secs_f64()
                        * 1000.0,
                },
            )
        })
        .collect();

    Ok(Json(SchedulerStatusDto {
        enabled,
        max_concurrent_polls: Some(stats.max_concurrent_polls),
        active_polls: Some(stats.active_polls),
        classes: Some(classes),
    }))
}
