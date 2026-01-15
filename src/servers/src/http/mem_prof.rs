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

#[cfg(feature = "mem-prof")]
use axum::Form;
#[cfg(feature = "mem-prof")]
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

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct MemPprofQuery {
    output: Output,
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn mem_prof_handler(
    Query(req): Query<MemPprofQuery>,
) -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    let dump = match req.output {
        Output::Proto => common_mem_prof::dump_pprof().await,
        Output::Text => common_mem_prof::dump_profile().await,
        Output::Flamegraph => common_mem_prof::dump_flamegraph().await,
    }
    .context(DumpProfileDataSnafu)?;

    Ok((StatusCode::OK, dump))
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn activate_heap_prof_handler() -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    common_mem_prof::activate_heap_profile().context(DumpProfileDataSnafu)?;

    Ok((StatusCode::OK, "Heap profiling activated"))
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn deactivate_heap_prof_handler() -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    common_mem_prof::deactivate_heap_profile().context(DumpProfileDataSnafu)?;

    Ok((StatusCode::OK, "Heap profiling deactivated"))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn mem_prof_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn activate_heap_prof_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn heap_prof_status_handler() -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    let is_active = common_mem_prof::is_heap_profile_active().context(DumpProfileDataSnafu)?;

    Ok((StatusCode::OK, format!("{{\"active\": {}}}", is_active)))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn deactivate_heap_prof_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn heap_prof_status_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}

#[cfg(feature = "mem-prof")]
#[derive(Deserialize)]
pub struct GdumpToggleForm {
    activate: bool,
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn gdump_toggle_handler(
    Form(form): Form<GdumpToggleForm>,
) -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    common_mem_prof::set_gdump_active(form.activate).context(DumpProfileDataSnafu)?;

    let msg = if form.activate {
        "gdump activated"
    } else {
        "gdump deactivated"
    };
    Ok((StatusCode::OK, msg))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn gdump_toggle_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn gdump_status_handler() -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    let is_active = common_mem_prof::is_gdump_active().context(DumpProfileDataSnafu)?;
    Ok((StatusCode::OK, format!("{{\"active\": {}}}", is_active)))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn gdump_status_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}

#[cfg(feature = "mem-prof")]
#[axum_macros::debug_handler]
pub async fn symbolicate_handler(
    body: axum::body::Bytes,
) -> crate::error::Result<impl IntoResponse> {
    use snafu::ResultExt;

    use crate::error::DumpProfileDataSnafu;

    let flamegraph = common_mem_prof::symbolicate_jeheap(&body).context(DumpProfileDataSnafu)?;

    Ok((StatusCode::OK, flamegraph))
}

#[cfg(not(feature = "mem-prof"))]
#[axum_macros::debug_handler]
pub async fn symbolicate_handler() -> crate::error::Result<impl IntoResponse> {
    Ok((
        StatusCode::NOT_IMPLEMENTED,
        "The 'mem-prof' feature is disabled",
    ))
}
