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

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use common_telemetry::info;
use serde::Deserialize;
use snafu::ensure;

use crate::error::{InvalidParameterSnafu, Result};

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
pub async fn set_weights_handler(Query(params): Query<WeightParams>) -> Result<impl IntoResponse> {
    ensure!(
        common_runtime::workload_scheduler_stats().is_some(),
        InvalidParameterSnafu {
            reason: "workload scheduler is not enabled",
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
            reason: "workload scheduler is not enabled",
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
