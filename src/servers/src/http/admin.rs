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

use std::collections::HashMap;

use api::v1::ddl_request::Expr;
use api::v1::greptime_request::Request;
use api::v1::{DdlRequest, FlushTableExpr};
use axum::extract::{Query, RawBody, State};
use axum::http::StatusCode as HttpStatusCode;
use axum::Json;
use session::context::QueryContext;
use snafu::OptionExt;

use crate::error;
use crate::error::Result;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;

#[axum_macros::debug_handler]
pub async fn flush(
    State(grpc_handler): State<ServerGrpcQueryHandlerRef>,
    Query(params): Query<HashMap<String, String>>,
    RawBody(_): RawBody,
) -> Result<(HttpStatusCode, Json<String>)> {
    let catalog_name = params
        .get("catalog_name")
        .cloned()
        .unwrap_or("greptime".to_string());
    let schema_name =
        params
            .get("schema_name")
            .cloned()
            .context(error::InvalidFlushArgumentSnafu {
                err_msg: "schema_name is not present",
            })?;

    // if table name is not present, flush all tables inside schema
    let table_name = params.get("table_name").cloned().unwrap_or_default();

    let region_id: Option<u32> = params
        .get("region")
        .map(|v| v.parse())
        .transpose()
        .ok()
        .flatten();

    let request = Request::Ddl(DdlRequest {
        expr: Some(Expr::FlushTable(FlushTableExpr {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
            region_id,
        })),
    });

    grpc_handler.do_query(request, QueryContext::arc()).await?;
    Ok((HttpStatusCode::OK, Json::from("done".to_string())))
}
