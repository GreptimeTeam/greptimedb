pub mod handler;
#[cfg(feature = "opentsdb")]
pub mod opentsdb;

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    error_handling::HandleErrorLayer,
    response::IntoResponse,
    response::{Json, Response},
    routing, BoxError, Router,
};
use cfg_if::cfg_if;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_telemetry::logging::info;
use serde::Serialize;
use snafu::ResultExt;
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::trace::TraceLayer;

use crate::error::{Result, StartHttpSnafu};
#[cfg(feature = "opentsdb")]
use crate::query_handler::OpentsdbLineProtocolHandlerRef;
use crate::query_handler::SqlQueryHandlerRef;
use crate::server::Server;

const HTTP_API_VERSION: &str = "v1";

pub struct HttpServer {
    sql_handler: SqlQueryHandlerRef,

    #[cfg(feature = "opentsdb")]
    // Because of Cargo's [feature unification](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification),
    // when "opentsdb" feature is used in Frontend (which is enable by default), Datanode also has
    // to use it, regardless of whether Datanode wants it or not. Making this Opentsdb handler
    // optional is to bypass the above cargo restriction, letting Frontend set it later.
    opentsdb_handler: Option<OpentsdbLineProtocolHandlerRef>,
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
    pub fn new(sql_handler: SqlQueryHandlerRef) -> Self {
        cfg_if! {
            if #[cfg(feature = "opentsdb")] {
                Self {
                    sql_handler,
                    opentsdb_handler: None,
                }
            } else {
                Self { sql_handler }
            }
        }
    }

    #[cfg(feature = "opentsdb")]
    pub fn set_opentsdb_handler(&mut self, handler: OpentsdbLineProtocolHandlerRef) {
        debug_assert!(
            self.opentsdb_handler.is_none(),
            "Opentsdb handler can be set only once!"
        );
        self.opentsdb_handler.get_or_insert(handler);
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

        cfg_if! {
            if #[cfg(feature = "opentsdb")] {
                if let Some(opentsdb_handler) = self.opentsdb_handler.clone() {
                    let opentsdb_router = Router::with_state(opentsdb_handler.clone())
                        .route("/api/put", routing::post(opentsdb::put));

                    router = router.nest(&format!("/{}/opentsdb", HTTP_API_VERSION), opentsdb_router);
                }
            }
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
