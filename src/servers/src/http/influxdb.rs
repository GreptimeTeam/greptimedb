use axum::extract::State;
use axum::http::StatusCode;

use super::HttpResponse;
use crate::error::Result;
use crate::query_handler::InfluxdbProtocolLineHandlerRef;

#[axum_macros::debug_handler]
pub async fn influxdb_write(
    State(handler): State<InfluxdbProtocolLineHandlerRef>,
    payload: String,
) -> Result<(StatusCode, HttpResponse)> {
    handler.exec(&payload).await?;
    Ok((
        StatusCode::NO_CONTENT,
        HttpResponse::Text("success!".to_string()),
    ))
}
