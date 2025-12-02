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

use axum::http::StatusCode;
use axum::response::IntoResponse;
use common_telemetry::{TRACE_RELOAD_HANDLE, get_or_init_tracer, info};

use crate::error::{InvalidParameterSnafu, Result};

#[axum_macros::debug_handler]
pub async fn dyn_trace_handler(enable_str: String) -> Result<impl IntoResponse> {
    let enable = enable_str.parse::<bool>().map_err(|e| {
        InvalidParameterSnafu {
            reason: format!("Invalid parameter \"enable\": {e:?}"),
        }
        .build()
    })?;

    let Some(trace_reload_handle) = TRACE_RELOAD_HANDLE.get() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            "trace reload handle is not initialized".to_string(),
        ));
    };

    if enable {
        let tracer = match get_or_init_tracer() {
            Ok(tracer) => tracer,
            Err(reason) => {
                return Ok((StatusCode::SERVICE_UNAVAILABLE, reason.to_string()));
            }
        };

        let trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        trace_reload_handle.reload(Some(trace_layer));
        info!("trace enabled");
        Ok((StatusCode::OK, "trace enabled".to_string()))
    } else {
        trace_reload_handle.reload(None);
        info!("trace disabled");
        Ok((StatusCode::OK, "trace disabled".to_string()))
    }
}
