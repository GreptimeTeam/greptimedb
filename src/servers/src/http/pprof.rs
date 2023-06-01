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

use std::num::NonZeroI32;
use std::time::Duration;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use common_pprof::Profiling;
use common_telemetry::logging;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{DumpPprofSnafu, Result};

/// Output format.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Output {
    /// google’s pprof format report in protobuf.
    Proto,
    /// Simple text format.
    Text,
    /// svg flamegraph.
    Flamegraph,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(default)]
pub struct PprofQuery {
    seconds: u64,
    frequency: NonZeroI32,
    output: Output,
}

impl Default for PprofQuery {
    fn default() -> PprofQuery {
        PprofQuery {
            seconds: 5,
            // Safety: 99 is non zero.
            frequency: NonZeroI32::new(99).unwrap(),
            output: Output::Proto,
        }
    }
}

#[axum_macros::debug_handler]
pub async fn pprof_handler(Query(req): Query<PprofQuery>) -> Result<impl IntoResponse> {
    logging::info!("start pprof, request: {:?}", req);

    let profiling = Profiling::new(Duration::from_secs(req.seconds), req.frequency.into());
    let body = match req.output {
        Output::Proto => profiling.dump_proto().await.context(DumpPprofSnafu)?,
        Output::Text => {
            let report = profiling.report().await.context(DumpPprofSnafu)?;
            format!("{:?}", report).into_bytes()
        }
        Output::Flamegraph => profiling.dump_flamegraph().await.context(DumpPprofSnafu)?,
    };

    logging::info!("finish pprof");

    Ok((StatusCode::OK, body))
}
