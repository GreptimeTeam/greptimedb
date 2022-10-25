pub mod handler;
pub mod influxdb;
pub mod opentsdb;
pub mod prometheus;

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    error_handling::HandleErrorLayer,
    http::header,
    response::IntoResponse,
    response::{Json, Response},
    routing, BoxError, Router,
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
use crate::query_handler::{
    InfluxdbLineProtocolHandlerRef, OpentsdbProtocolHandlerRef, PrometheusProtocolHandlerRef,
};
use crate::server::Server;

const HTTP_API_VERSION: &str = "v1";

pub struct HttpServer {
    sql_handler: SqlQueryHandlerRef,
    influxdb_handler: Option<InfluxdbLineProtocolHandlerRef>,
    opentsdb_handler: Option<OpentsdbProtocolHandlerRef>,
    prom_handler: Option<PrometheusProtocolHandlerRef>,
}

#[derive(Serialize, Debug)]
pub enum JsonOutput {
    AffectedRows(usize),
    Rows(Vec<RecordBatch>),
}

#[derive(Serialize, Debug)]
pub struct BytesResponse {
    pub content_type: String,
    pub content_encoding: String,
    pub bytes: Vec<u8>,
}

#[derive(Serialize, Debug)]
pub enum HttpResponse {
    Json(JsonResponse),
    Text(String),
    Bytes(BytesResponse),
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
            HttpResponse::Bytes(resp) => (
                [
                    (header::CONTENT_TYPE, resp.content_type),
                    (header::CONTENT_ENCODING, resp.content_encoding),
                ],
                resp.bytes,
            )
                .into_response(),
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
    pub fn new(sql_handler: SqlQueryHandlerRef) -> Self {
        Self {
            sql_handler,
            opentsdb_handler: None,
            influxdb_handler: None,
            prom_handler: None,
        }
    }

    pub fn set_opentsdb_handler(&mut self, handler: OpentsdbProtocolHandlerRef) {
        debug_assert!(
            self.opentsdb_handler.is_none(),
            "OpenTSDB handler can be set only once!"
        );
        self.opentsdb_handler.get_or_insert(handler);
    }

    pub fn set_influxdb_handler(&mut self, handler: InfluxdbLineProtocolHandlerRef) {
        debug_assert!(
            self.influxdb_handler.is_none(),
            "Influxdb line protocol handler can be set only once!"
        );
        self.influxdb_handler.get_or_insert(handler);
    }

    pub fn set_prom_handler(&mut self, handler: PrometheusProtocolHandlerRef) {
        debug_assert!(
            self.prom_handler.is_none(),
            "Prometheus protocol handler can be set only once!"
        );
        self.prom_handler.get_or_insert(handler);
    }

    pub fn make_app(&self) -> Router {
        // TODO(LFC): Use released Axum.
        // Axum version 0.6 introduces state within router, making router methods far more elegant
        // to write. Though version 0.6 is rc, I think it's worth to upgrade.
        // Prior to version 0.6, we only have a single "Extension" to share all query
        // handlers amongst router methods. That requires us to pack all query handlers in a shared
        // state, and check-then-get the desired query handler in different router methods, which
        // is a lot of tedious work.
        let sql_router = Router::with_state(self.sql_handler.clone())
            .route("/sql", routing::get(handler::sql))
            .route("/scripts", routing::post(handler::scripts))
            .route("/run-script", routing::post(handler::run_script));

        let mut router = Router::new().nest(&format!("/{}", HTTP_API_VERSION), sql_router);

        if let Some(opentsdb_handler) = self.opentsdb_handler.clone() {
            let opentsdb_router = Router::with_state(opentsdb_handler)
                .route("/api/put", routing::post(opentsdb::put));

            router = router.nest(&format!("/{}/opentsdb", HTTP_API_VERSION), opentsdb_router);
        }

        // TODO(fys): Creating influxdb's database when we can create greptime schema.
        if let Some(influxdb_handler) = self.influxdb_handler.clone() {
            let influxdb_router =
                Router::with_state(influxdb_handler).route("/write", routing::post(influxdb_write));

            router = router.nest(&format!("/{}/influxdb", HTTP_API_VERSION), influxdb_router);
        }

        if let Some(prom_handler) = self.prom_handler.clone() {
            let prom_router = Router::with_state(prom_handler)
                .route("/remote/write", routing::post(prometheus::remote_write))
                .route("/remote/read", routing::post(prometheus::remote_read));

            router = router.nest(&format!("/{}/prometheus", HTTP_API_VERSION), prom_router);
        }

        router
            .route("/metrics", routing::get(handler::metrics))
            // middlewares
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_error))
                    .layer(TraceLayer::new_for_http())
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
