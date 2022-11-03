use std::collections::HashMap;

use aide::transform::TransformOperation;
use axum::extract::{Json, Query, RawBody, State};
use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use common_telemetry::metric;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::JsonResponse;
use crate::query_handler::SqlQueryHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SqlQuery {
    pub database: Option<String>,
    pub sql: Option<String>,
}

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    State(sql_handler): State<SqlQueryHandlerRef>,
    Query(params): Query<SqlQuery>,
) -> Json<JsonResponse> {
    if let Some(ref sql) = params.sql {
        Json(JsonResponse::from_output(sql_handler.do_query(sql).await).await)
    } else {
        Json(JsonResponse::with_error(
            "sql parameter is required.".to_string(),
            StatusCode::InvalidArguments,
        ))
    }
}

pub(crate) fn sql_docs(op: TransformOperation) -> TransformOperation {
    op.response::<200, Json<JsonResponse>>()
}

/// Handler to export metrics
#[axum_macros::debug_handler]
pub async fn metrics(Query(_params): Query<HashMap<String, String>>) -> String {
    if let Some(handle) = metric::try_handle() {
        handle.render()
    } else {
        "Prometheus handle not initialized.".to_owned()
    }
}

macro_rules! json_err {
    ($e: ident) => {{
        return Json(JsonResponse::with_error(
            format!("Invalid argument: {}", $e),
            common_error::status_code::StatusCode::InvalidArguments,
        ));
    }};

    ($msg: expr, $code: expr) => {{
        return Json(JsonResponse::with_error($msg.to_string(), $code));
    }};
}

macro_rules! unwrap_or_json_err {
    ($result: expr) => {
        match $result {
            Ok(result) => result,
            Err(e) => json_err!(e),
        }
    };
}

/// Handler to insert and compile script
#[axum_macros::debug_handler]
pub async fn scripts(
    State(query_handler): State<SqlQueryHandlerRef>,
    Query(params): Query<ScriptQuery>,
    RawBody(body): RawBody,
) -> Json<JsonResponse> {
    let name = params.name.as_ref();

    if name.is_none() || name.unwrap().is_empty() {
        json_err!("Invalid name", StatusCode::InvalidArguments);
    }
    let bytes = unwrap_or_json_err!(hyper::body::to_bytes(body).await);

    let script = unwrap_or_json_err!(String::from_utf8(bytes.to_vec()));

    let body = match query_handler.insert_script(name.unwrap(), &script).await {
        Ok(()) => JsonResponse::with_output(None),
        Err(e) => json_err!(format!("Insert script error: {}", e), e.status_code()),
    };

    Json(body)
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScriptQuery {
    pub name: Option<String>,
}

/// Handler to execute script
#[axum_macros::debug_handler]
pub async fn run_script(
    State(query_handler): State<SqlQueryHandlerRef>,
    Query(params): Query<ScriptQuery>,
) -> Json<JsonResponse> {
    let name = params.name.as_ref();

    if name.is_none() || name.unwrap().is_empty() {
        json_err!("Invalid name", StatusCode::InvalidArguments);
    }

    let output = query_handler.execute_script(name.unwrap()).await;

    Json(JsonResponse::from_output(output).await)
}
