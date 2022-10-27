use std::collections::HashMap;

use aide::axum::IntoApiResponse;
use axum::extract::{Json, Query, State};
use axum::response::IntoResponse;
use common_telemetry::metric;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::JsonResponse;
use crate::query_handler::SqlQueryHandlerRef;

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    State(sql_handler): State<SqlQueryHandlerRef>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoApiResponse {
    if let Some(sql) = params.get("sql") {
        Json(JsonResponse::from_output(sql_handler.do_query(sql).await).await)
    } else {
        Json(JsonResponse::with_error(Some(
            "sql parameter is required.".to_string(),
        )))
    }
}

/// Handler to export metrics
#[axum_macros::debug_handler]
pub async fn metrics(Query(_params): Query<HashMap<String, String>>) -> impl IntoResponse {
    if let Some(handle) = metric::try_handle() {
        handle.render()
    } else {
        "Prometheus handle not initialized.".to_owned()
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ScriptExecution {
    pub name: String,
    pub script: String,
}

/// Handler to insert and compile script
#[axum_macros::debug_handler]
pub async fn scripts(
    State(query_handler): State<SqlQueryHandlerRef>,
    Json(payload): Json<ScriptExecution>,
) -> impl IntoApiResponse {
    if payload.name.is_empty() || payload.script.is_empty() {
        return Json(JsonResponse::with_error(Some(
            "Invalid name or script".to_string(),
        )));
    }

    let body = match query_handler
        .insert_script(&payload.name, &payload.script)
        .await
    {
        Ok(()) => JsonResponse::with_output(None),
        Err(e) => JsonResponse::with_error(Some(format!("Insert script error: {}", e))),
    };

    Json(body)
}

/// Handler to execute script
#[axum_macros::debug_handler]
pub async fn run_script(
    State(query_handler): State<SqlQueryHandlerRef>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoApiResponse {
    let name = params.get("name");

    if name.is_none() || name.unwrap().is_empty() {
        return Json(JsonResponse::with_error(Some("Invalid name".to_string())));
    }

    let output = query_handler.execute_script(name.unwrap()).await;

    Json(JsonResponse::from_output(output).await)
}
