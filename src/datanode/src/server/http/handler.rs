// http handlers

use std::collections::HashMap;

use axum::extract::{Extension, Query};

use crate::instance::InstanceRef;
use crate::server::http::JsonResponse;

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    Extension(instance): Extension<InstanceRef>,
    Query(params): Query<HashMap<String, String>>,
) -> JsonResponse {
    if let Some(sql) = params.get("sql") {
        JsonResponse::from(instance.execute_sql(&sql).await).await
    } else {
        JsonResponse::with_error(Some("sql parameter is required.".to_string()))
    }
}
