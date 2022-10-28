pub mod handler;
pub mod influxdb;
pub mod opentsdb;
pub mod prometheus;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aide::axum::routing as apirouting;
use aide::axum::{ApiRouter, IntoApiResponse};
use aide::openapi::{Info, OpenApi, Server as OpenAPIServer};
use async_trait::async_trait;
use axum::response::Html;
use axum::Extension;
use axum::{error_handling::HandleErrorLayer, response::Json, routing, BoxError, Router};
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_telemetry::logging::info;
use datatypes::data_type::DataType;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
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

#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnSchema {
    name: String,
    data_type: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Schema {
    column_schemas: Vec<ColumnSchema>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct HttpRecordsOutput {
    schema: Option<Schema>,
    rows: Vec<Vec<Value>>,
}

impl HttpRecordsOutput {
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn num_cols(&self) -> usize {
        self.schema
            .as_ref()
            .map(|x| x.column_schemas.len())
            .unwrap_or(0)
    }
}

impl TryFrom<Vec<RecordBatch>> for HttpRecordsOutput {
    type Error = String;

    fn try_from(
        recordbatches: Vec<RecordBatch>,
    ) -> std::result::Result<HttpRecordsOutput, Self::Error> {
        if recordbatches.is_empty() {
            Ok(HttpRecordsOutput {
                schema: None,
                rows: vec![],
            })
        } else {
            // safety ensured by previous empty check
            let first = &recordbatches[0];
            let schema = Schema {
                column_schemas: first
                    .schema
                    .column_schemas()
                    .iter()
                    .map(|cs| ColumnSchema {
                        name: cs.name.clone(),
                        data_type: cs.data_type.name().to_owned(),
                    })
                    .collect(),
            };

            let mut rows =
                Vec::with_capacity(recordbatches.iter().map(|r| r.num_rows()).sum::<usize>());

            for recordbatch in recordbatches {
                for row in recordbatch.rows() {
                    let row = row.map_err(|e| e.to_string())?;
                    let value_row = row
                        .into_iter()
                        .map(|f| Value::try_from(f).map_err(|err| err.to_string()))
                        .collect::<std::result::Result<Vec<Value>, _>>()?;

                    rows.push(value_row);
                }
            }

            Ok(HttpRecordsOutput {
                schema: Some(schema),
                rows,
            })
        }
    }
}

#[derive(Serialize, Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum JsonOutput {
    AffectedRows(usize),
    Records(HttpRecordsOutput),
}

#[derive(Serialize, Debug, JsonSchema)]
pub struct JsonResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<JsonOutput>,
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
                Ok(rows) => match HttpRecordsOutput::try_from(rows) {
                    Ok(rows) => Self::with_output(Some(JsonOutput::Records(rows))),
                    Err(err) => Self::with_error(Some(format!(": {}", err))),
                },
                Err(e) => Self::with_error(Some(format!("Recordbatch error: {}", e))),
            },
            Ok(Output::RecordBatches(recordbatches)) => {
                match HttpRecordsOutput::try_from(recordbatches.take()) {
                    Ok(rows) => Self::with_output(Some(JsonOutput::Records(rows))),
                    Err(err) => Self::with_error(Some(format!(": {}", err))),
                }
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

async fn serve_api(Extension(api): Extension<Arc<OpenApi>>) -> impl IntoApiResponse {
    Json(api)
}

async fn serve_docs() -> Html<String> {
    Html(include_str!("http/redoc.html").to_owned())
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
        let mut api = OpenApi {
            info: Info {
                title: "Greptime DB HTTP API".to_string(),
                description: Some("HTTP APIs to interact with Greptime DB".to_string()),
                version: HTTP_API_VERSION.to_string(),
                ..Info::default()
            },
            servers: vec![OpenAPIServer {
                url: "http://localhost:4000/v1".to_string(),
                ..OpenAPIServer::default()
            }],

            ..OpenApi::default()
        };

        // TODO(LFC): Use released Axum.
        // Axum version 0.6 introduces state within router, making router methods far more elegant
        // to write. Though version 0.6 is rc, I think it's worth to upgrade.
        // Prior to version 0.6, we only have a single "Extension" to share all query
        // handlers amongst router methods. That requires us to pack all query handlers in a shared
        // state, and check-then-get the desired query handler in different router methods, which
        // is a lot of tedious work.
        let sql_router = ApiRouter::with_state(self.sql_handler.clone())
            .api_route(
                "/sql",
                apirouting::get_with(handler::sql, handler::sql_docs)
                    .post_with(handler::sql, handler::sql_docs),
            )
            .api_route("/scripts", apirouting::post(handler::scripts))
            .api_route("/run-script", apirouting::post(handler::run_script))
            .route("/private/api.json", apirouting::get(serve_api))
            .route("/private/docs", apirouting::get(serve_docs))
            .finish_api(&mut api)
            .layer(Extension(Arc::new(api)));

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
                .route("/write", routing::post(prometheus::remote_write))
                .route("/read", routing::post(prometheus::remote_read));

            router = router.nest(&format!("/{}/prometheus", HTTP_API_VERSION), prom_router);
        }

        let metrics_router = Router::new().route("/", routing::get(handler::metrics));
        router = router.nest(&format!("/{}/metrics", HTTP_API_VERSION), metrics_router);

        router
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
