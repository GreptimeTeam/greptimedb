pub mod handler;
pub mod influxdb;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    error_handling::HandleErrorLayer,
    response::IntoResponse,
    response::{Json, Response},
    routing, BoxError, Extension, Router,
};
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_telemetry::logging::info;
use serde::Serialize;
use snafu::ResultExt;
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::trace::TraceLayer;

use self::influxdb::influxdb_write;
use crate::error::{Result, StartHttpSnafu};
use crate::query_handler::SqlQueryHandlerRef;
use crate::server::Server;

const HTTP_API_VERSION: &str = "v1";

#[derive(derive_builder::Builder)]
pub struct HttpServer {
    query_handler: SqlQueryHandlerRef,
    influxdb_handler: Option<crate::query_handler::InfluxdbProtocolLineHandlerRef>,
}

#[derive(Serialize, Debug)]
pub enum JsonOutput {
    AffectedRows(usize),
    Rows(Vec<RecordBatch>),
}

#[derive(Serialize, Debug)]
pub enum HttpResponse {
    Json(JsonResponse),
    Text(String),
}

#[derive(Serialize, Debug)]
pub struct JsonResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<JsonOutput>,
}

impl IntoResponse for HttpResponse {
    fn into_response(self) -> Response {
        match self {
            HttpResponse::Json(json) => Json(json).into_response(),
            HttpResponse::Text(text) => text.into_response(),
        }
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
    async fn from_output(output: Result<Output>) -> Self {
        match output {
            Ok(Output::AffectedRows(rows)) => {
                Self::with_output(Some(JsonOutput::AffectedRows(rows)))
            }
            Ok(Output::Stream(stream)) => match util::collect(stream).await {
                Ok(rows) => Self::with_output(Some(JsonOutput::Rows(rows))),
                Err(e) => Self::with_error(Some(format!("Recordbatch error: {}", e))),
            },
            Ok(Output::RecordBatches(recordbatches)) => {
                Self::with_output(Some(JsonOutput::Rows(recordbatches.take())))
            }
            Err(e) => Self::with_error(Some(format!("Query engine output error: {}", e))),
        }
    }

    pub fn success(&self) -> bool {
        self.success
    }

    pub fn error(&self) -> Option<&String> {
        self.error.as_ref()
    }

    pub fn output(&self) -> Option<&JsonOutput> {
        self.output.as_ref()
    }
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    // It has an issue on chrome: https://github.com/sigp/lighthouse/issues/478
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

impl HttpServer {
    pub fn new(query_handler: SqlQueryHandlerRef) -> Self {
        Self {
            query_handler,
            influxdb_handler: None,
        }
    }

    pub fn make_app(&self) -> Router {
        let mut router = Router::new().nest(
            &format!("/{}", HTTP_API_VERSION),
            Router::new()
                // handlers
                .route("/sql", routing::get(handler::sql))
                .route("/scripts", routing::post(handler::scripts))
                .route("/run-script", routing::post(handler::run_script)),
        );

        if let Some(handler) = &self.influxdb_handler {
            let handler = Arc::clone(handler);
            router = router.nest(
                &format!("/{}/influxdb", HTTP_API_VERSION),
                Router::new().route(
                    "/write",
                    routing::post(move |body| influxdb_write(body, handler)),
                ),
            );
        }

        router
            .route("/metrics", routing::get(handler::metrics))
            // middlewares
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_error))
                    .layer(TraceLayer::new_for_http())
                    .layer(Extension(self.query_handler.clone()))
                    // TODO(LFC): make timeout configurable
                    .layer(TimeoutLayer::new(Duration::from_secs(30))),
            )
    }
}

#[async_trait]
impl Server for HttpServer {
    async fn shutdown(&mut self) -> Result<()> {
        // TODO(LFC): shutdown http server, and remove `shutdown_signal` above
        unimplemented!()
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let app = self.make_app();
        let server = axum::Server::bind(&listening).serve(app.into_make_service());
        let listening = server.local_addr();
        info!("HTTP server is bound to {}", listening);
        let graceful = server.with_graceful_shutdown(shutdown_signal());
        graceful.await.context(StartHttpSnafu)?;
        Ok(listening)
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
