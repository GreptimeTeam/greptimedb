use std::net::SocketAddr;
use std::time::Duration;

use axum::{
    error_handling::HandleErrorLayer,
    response::IntoResponse,
    response::{Json, Response},
    routing::get,
    BoxError, Extension, Router,
};
use common_recordbatch::{util, RecordBatch};
use query::Output;
use serde::Serialize;
use snafu::ResultExt;
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::trace::TraceLayer;

use crate::error::{HyperSnafu, Result};
mod handler;
use crate::server::InstanceRef;

/// Http server
pub struct HttpServer {
    instance: InstanceRef,
}

#[derive(Serialize)]
enum JsonOutput {
    AffectedRows(usize),
    Rows(Vec<RecordBatch>),
}

#[derive(Serialize)]
pub struct JsonResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<JsonOutput>,
}

impl IntoResponse for JsonResponse {
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}

impl JsonResponse {
    fn with_error(error: Option<String>) -> Self {
        JsonResponse {
            success: false,
            error,
            output: None,
        }
    }
    fn with_output(output: Option<JsonOutput>) -> Self {
        JsonResponse {
            success: true,
            error: None,
            output,
        }
    }

    /// Create a json response from query result
    async fn from(output: Result<Output>) -> Self {
        match output {
            Ok(Output::AffectedRows(rows)) => {
                Self::with_output(Some(JsonOutput::AffectedRows(rows)))
            }
            Ok(Output::RecordBatch(stream)) => match util::collect(stream).await {
                Ok(rows) => Self::with_output(Some(JsonOutput::Rows(rows))),
                Err(e) => Self::with_error(Some(format!("Recordbatch error: {}", e))),
            },
            Err(e) => Self::with_error(Some(format!("Query engine output error: {}", e))),
        }
    }
}

impl HttpServer {
    pub fn new(instance: InstanceRef) -> Self {
        Self { instance }
    }

    pub async fn start(&self) -> Result<()> {
        let app = Router::new().route("/sql", get(handler::sql)).layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_error))
                .layer(TraceLayer::new_for_http())
                .layer(Extension(self.instance.clone()))
                // TODO configure timeout
                .layer(TimeoutLayer::new(Duration::from_secs(30))),
        );

        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        println!("Datanode is listening on {}", addr);
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .context(HyperSnafu)?;

        Ok(())
    }
}

/// handle error middleware
async fn handle_error(err: BoxError) -> Json<JsonResponse> {
    Json(JsonResponse {
        success: false,
        error: Some(format!("Unhandled internal error: {}", err)),
        output: None,
    })
}
