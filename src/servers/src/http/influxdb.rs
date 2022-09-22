use axum::http::StatusCode;

use super::HttpResponse;
use crate::error::Result;
use crate::query_handler::InfluxdbProtocolLineHandlerRef;

pub async fn influxdb_write(
    payload: String,
    handler: InfluxdbProtocolLineHandlerRef,
) -> Result<(StatusCode, HttpResponse)> {
    handler.exec(&payload).await?;
    Ok((
        StatusCode::NO_CONTENT,
        HttpResponse::Text("success!".to_string()),
    ))
}
