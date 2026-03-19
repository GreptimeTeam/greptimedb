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

use std::sync::Arc;
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::{Extension, Path, State};
use axum::http::{StatusCode, Uri, header};
use axum::response::Response;
use common_telemetry::{debug, error};
use rust_embed::RustEmbed;
use session::context::{Channel, QueryContext};
use snafu::{ResultExt, ensure};

use crate::error::{BuildHttpResponseSnafu, InvalidParameterSnafu, Result};
use crate::http::DashboardState;
use crate::http::result::greptime_manage_resp::{DashboardOutput, GreptimedbManageResponse};

#[derive(RustEmbed)]
#[folder = "dashboard/dist/"]
pub struct Assets;

#[axum_macros::debug_handler]
pub async fn static_handler(uri: Uri) -> Result<Response> {
    debug!("[dashboard] requesting: {}", uri.path());

    let mut path = uri.path().trim_start_matches("/dashboard/");
    if path.is_empty() {
        path = "index.html";
    }

    match get_assets(path) {
        Ok(response) if response.status() == StatusCode::NOT_FOUND => index_page(),
        Ok(response) => Ok(response),
        Err(e) => Err(e),
    }
}

fn index_page() -> Result<Response> {
    get_assets("index.html")
}

fn get_assets(path: &str) -> Result<Response> {
    match Assets::get(path) {
        Some(content) => {
            let body = Body::from(content.data);
            let mime = mime_guess::from_path(path).first_or_octet_stream();

            Response::builder()
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(body)
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("404")),
    }
    .context(BuildHttpResponseSnafu)
}

#[axum_macros::debug_handler]
pub async fn add_dashboard(
    State(state): State<DashboardState>,
    Path(dashboard_name): Path<String>,
    Extension(mut query_ctx): Extension<QueryContext>,
    payload: Bytes,
) -> Result<GreptimedbManageResponse> {
    let start = Instant::now();
    let handler = state.handler;
    ensure!(
        !dashboard_name.is_empty(),
        InvalidParameterSnafu {
            reason: "dashboard_name is required in path",
        }
    );

    let definition = String::from_utf8_lossy(&payload).to_string();

    query_ctx.set_channel(Channel::HttpSql);
    let query_ctx = Arc::new(query_ctx);

    handler
        .save(&dashboard_name, &definition, query_ctx)
        .await
        .map(|_| {
            GreptimedbManageResponse::from_dashboard(
                dashboard_name,
                start.elapsed().as_millis() as u64,
            )
        })
        .map_err(|e| {
            error!(e; "failed to save dashboard");
            e
        })
}

#[axum_macros::debug_handler]
pub async fn list_dashboards(
    State(state): State<DashboardState>,
    Extension(mut query_ctx): Extension<QueryContext>,
) -> Result<GreptimedbManageResponse> {
    let start = Instant::now();
    let handler = state.handler;

    query_ctx.set_channel(Channel::HttpSql);
    let query_ctx = Arc::new(query_ctx);

    handler
        .list(query_ctx)
        .await
        .map(|dashboards| {
            let outputs: Vec<DashboardOutput> = dashboards
                .into_iter()
                .map(|d| DashboardOutput {
                    name: d.name,
                    definition: d.definition,
                })
                .collect();
            GreptimedbManageResponse::from_dashboards(outputs, start.elapsed().as_millis() as u64)
        })
        .map_err(|e| {
            error!(e; "failed to list dashboards");
            e
        })
}

#[axum_macros::debug_handler]
pub async fn delete_dashboard(
    State(state): State<DashboardState>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Path(dashboard_name): Path<String>,
) -> Result<GreptimedbManageResponse> {
    let start = Instant::now();
    let handler = state.handler;
    ensure!(
        !dashboard_name.is_empty(),
        InvalidParameterSnafu {
            reason: "dashboard_name is required",
        }
    );

    query_ctx.set_channel(Channel::HttpSql);
    let query_ctx = Arc::new(query_ctx);

    handler
        .delete(&dashboard_name, query_ctx)
        .await
        .map(|_| {
            GreptimedbManageResponse::from_dashboard(
                dashboard_name,
                start.elapsed().as_millis() as u64,
            )
        })
        .map_err(|e| {
            error!(e; "failed to delete dashboard");
            e
        })
}
