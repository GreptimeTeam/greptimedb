use std::collections::HashMap;

use axum::extract::{Extension, Json, Query};
use common_telemetry::metric;
use serde::{Deserialize, Serialize};

use crate::http::{HttpResponse, JsonResponse};
use crate::query_handler::SqlQueryHandlerRef;

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    Extension(query_handler): Extension<SqlQueryHandlerRef>,
    Query(params): Query<HashMap<String, String>>,
) -> HttpResponse {
    if let Some(sql) = params.get("sql") {
        HttpResponse::Json(JsonResponse::from_output(query_handler.do_query(sql).await).await)
    } else {
        HttpResponse::Json(JsonResponse::with_error(Some(
            "sql parameter is required.".to_string(),
        )))
    }
}

/// Handler to export metrics
#[axum_macros::debug_handler]
pub async fn metrics(
    Extension(_query_handler): Extension<SqlQueryHandlerRef>,
    Query(_params): Query<HashMap<String, String>>,
) -> HttpResponse {
    if let Some(handle) = metric::try_handle() {
        HttpResponse::Text(handle.render())
    } else {
        HttpResponse::Text("Prometheus handle not initialized.".to_string())
    }
}

#[derive(Deserialize, Serialize)]
pub struct ScriptExecution {
    pub name: String,
    pub script: String,
}

/// Handler to insert and compile script
#[axum_macros::debug_handler]
pub async fn scripts(
    Extension(query_handler): Extension<SqlQueryHandlerRef>,
    Json(payload): Json<ScriptExecution>,
) -> HttpResponse {
    if payload.name.is_empty() || payload.script.is_empty() {
        return HttpResponse::Json(JsonResponse::with_error(Some(
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

    HttpResponse::Json(body)
}

/// Handler to execute script
#[axum_macros::debug_handler]
pub async fn run_script(
    Extension(query_handler): Extension<SqlQueryHandlerRef>,
    Query(params): Query<HashMap<String, String>>,
) -> HttpResponse {
    let name = params.get("name");

    if name.is_none() || name.unwrap().is_empty() {
        return HttpResponse::Json(JsonResponse::with_error(Some("Invalid name".to_string())));
    }

    let output = query_handler.execute_script(name.unwrap()).await;

    HttpResponse::Json(JsonResponse::from_output(output).await)
}
