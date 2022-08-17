use std::collections::HashMap;

use axum::extract::{Extension, Query};
use common_telemetry::metric;

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
