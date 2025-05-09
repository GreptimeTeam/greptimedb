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
use serde::{Deserialize, Serialize};

/// Output format.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Output {
    /// google’s pprof format report in protobuf.
    Proto,
    /// jeheap text format. Define by jemalloc.
    #[default]
    Text,
    /// svg flamegraph.
    Flamegraph,
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn mem_prof_handler(
    Query(req): Query<Output>,
) -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    let dump = match req {
        Output::Proto => common_mem_prof::dump_pprof().await,
        Output::Text => common_mem_prof::dump_profile().await,
        Output::Flamegraph => common_mem_prof::dump_flamegraph().await,
    }
    .context(DumpProfileDataSnafu)?;

    Ok((StatusCode::OK, dump))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn mem_prof_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}
