// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod context;
pub mod handler;
pub mod influxdb;
pub mod opentsdb;
pub mod prometheus;
pub mod script;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aide::axum::{routing as apirouting, ApiRouter, IntoApiResponse};
use aide::openapi::{Info, OpenApi, Server as OpenAPIServer};
use async_trait::async_trait;
use axum::error_handling::HandleErrorLayer;
use axum::middleware::{self};
use axum::response::{Html, Json};
use axum::{routing, BoxError, Extension, Router};
use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_telemetry::logging::info;
use datatypes::data_type::DataType;
use futures::FutureExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::Mutex;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use self::influxdb::influxdb_write;
use crate::error::{AlreadyStartedSnafu, Result, StartHttpSnafu};
use crate::query_handler::{
    InfluxdbLineProtocolHandlerRef, OpentsdbProtocolHandlerRef, PrometheusProtocolHandlerRef,
    ScriptHandlerRef, SqlQueryHandlerRef,
};
use crate::server::Server;

const HTTP_API_VERSION: &str = "v1";

pub struct HttpServer {
    sql_handler: SqlQueryHandlerRef,
    options: HttpOptions,
    influxdb_handler: Option<InfluxdbLineProtocolHandlerRef>,
    opentsdb_handler: Option<OpentsdbProtocolHandlerRef>,
    prom_handler: Option<PrometheusProtocolHandlerRef>,
    script_handler: Option<ScriptHandlerRef>,
    shutdown_tx: Mutex<Option<Sender<()>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpOptions {
    pub addr: String,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4000".to_string(),
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq)]
pub struct ColumnSchema {
    name: String,
    data_type: String,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: String) -> ColumnSchema {
        ColumnSchema { name, data_type }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq)]
pub struct Schema {
    column_schemas: Vec<ColumnSchema>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnSchema>) -> Schema {
        Schema {
            column_schemas: columns,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq)]
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

    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    pub fn rows(&self) -> &Vec<Vec<Value>> {
        &self.rows
    }
}

impl TryFrom<Vec<RecordBatch>> for HttpRecordsOutput {
    type Error = String;

    // TODO(sunng87): use schema from recordstreams when #366 fixed
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

#[derive(Serialize, Deserialize, Debug, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JsonOutput {
    AffectedRows(usize),
    Records(HttpRecordsOutput),
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct JsonResponse {
    code: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<Vec<JsonOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_time_ms: Option<u128>,
}

impl JsonResponse {
    fn with_error(error: String, error_code: StatusCode) -> Self {
        JsonResponse {
            error: Some(error),
            code: error_code as u32,
            output: None,
            execution_time_ms: None,
        }
    }

    fn with_output(output: Option<Vec<JsonOutput>>) -> Self {
        JsonResponse {
            error: None,
            code: StatusCode::Success as u32,
            output,
            execution_time_ms: None,
        }
    }

    fn with_execution_time(mut self, execution_time: u128) -> Self {
        self.execution_time_ms = Some(execution_time);
        self
    }

    /// Create a json response from query result
    async fn from_output(output: Result<Output>) -> Self {
        match output {
            Ok(Output::AffectedRows(rows)) => {
                Self::with_output(Some(vec![JsonOutput::AffectedRows(rows)]))
            }
            Ok(Output::Stream(stream)) => match util::collect(stream).await {
                Ok(rows) => match HttpRecordsOutput::try_from(rows) {
                    Ok(rows) => Self::with_output(Some(vec![JsonOutput::Records(rows)])),
                    Err(err) => Self::with_error(err, StatusCode::Internal),
                },
                Err(e) => Self::with_error(format!("Recordbatch error: {}", e), e.status_code()),
            },
            Ok(Output::RecordBatches(recordbatches)) => {
                match HttpRecordsOutput::try_from(recordbatches.take()) {
                    Ok(rows) => Self::with_output(Some(vec![JsonOutput::Records(rows)])),
                    Err(err) => Self::with_error(err, StatusCode::Internal),
                }
            }
            Err(e) => {
                Self::with_error(format!("Query engine output error: {}", e), e.status_code())
            }
        }
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn success(&self) -> bool {
        self.code == (StatusCode::Success as u32)
    }

    pub fn error(&self) -> Option<&String> {
        self.error.as_ref()
    }

    pub fn output(&self) -> Option<&[JsonOutput]> {
        self.output.as_deref()
    }

    pub fn execution_time_ms(&self) -> Option<u128> {
        self.execution_time_ms
    }
}

async fn serve_api(Extension(api): Extension<Arc<OpenApi>>) -> impl IntoApiResponse {
    Json(api)
}

async fn serve_docs() -> Html<String> {
    Html(include_str!("http/redoc.html").to_owned())
}

#[derive(Clone)]
pub struct ApiState {
    pub sql_handler: SqlQueryHandlerRef,
    pub script_handler: Option<ScriptHandlerRef>,
}

impl HttpServer {
    pub fn new(sql_handler: SqlQueryHandlerRef, options: HttpOptions) -> Self {
        Self {
            sql_handler,
            options,
            opentsdb_handler: None,
            influxdb_handler: None,
            prom_handler: None,
            script_handler: None,
            shutdown_tx: Mutex::new(None),
        }
    }

    pub fn set_opentsdb_handler(&mut self, handler: OpentsdbProtocolHandlerRef) {
        debug_assert!(
            self.opentsdb_handler.is_none(),
            "OpenTSDB handler can be set only once!"
        );
        self.opentsdb_handler.get_or_insert(handler);
    }

    pub fn set_script_handler(&mut self, handler: ScriptHandlerRef) {
        debug_assert!(
            self.script_handler.is_none(),
            "Script handler can be set only once!"
        );
        self.script_handler.get_or_insert(handler);
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
                url: format!("/{}", HTTP_API_VERSION),
                ..OpenAPIServer::default()
            }],
            ..OpenApi::default()
        };

        let sql_router = self
            .route_sql(ApiState {
                sql_handler: self.sql_handler.clone(),
                script_handler: self.script_handler.clone(),
            })
            .finish_api(&mut api)
            .layer(Extension(Arc::new(api)));

        let mut router = Router::new().nest(&format!("/{}", HTTP_API_VERSION), sql_router);

        if let Some(opentsdb_handler) = self.opentsdb_handler.clone() {
            router = router.nest(
                &format!("/{}/opentsdb", HTTP_API_VERSION),
                self.route_opentsdb(opentsdb_handler),
            );
        }

        if let Some(influxdb_handler) = self.influxdb_handler.clone() {
            router = router.nest(
                &format!("/{}/influxdb", HTTP_API_VERSION),
                self.route_influxdb(influxdb_handler),
            );
        }

        if let Some(prom_handler) = self.prom_handler.clone() {
            router = router.nest(
                &format!("/{}/prometheus", HTTP_API_VERSION),
                self.route_prom(prom_handler),
            );
        }

        router = router.route("/metrics", routing::get(handler::metrics));

        router
            // middlewares
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_error))
                    .layer(TraceLayer::new_for_http())
                    .layer(TimeoutLayer::new(self.options.timeout))
                    // custom layer
                    .layer(middleware::from_fn(context::build_ctx)),
            )
    }

    fn route_sql<S>(&self, api_state: ApiState) -> ApiRouter<S> {
        ApiRouter::new()
            .api_route(
                "/sql",
                apirouting::get_with(handler::sql, handler::sql_docs)
                    .post_with(handler::sql, handler::sql_docs),
            )
            .api_route("/scripts", apirouting::post(script::scripts))
            .api_route("/run-script", apirouting::post(script::run_script))
            .route("/private/api.json", apirouting::get(serve_api))
            .route("/private/docs", apirouting::get(serve_docs))
            .with_state(api_state)
    }

    fn route_prom<S>(&self, prom_handler: PrometheusProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/write", routing::post(prometheus::remote_write))
            .route("/read", routing::post(prometheus::remote_read))
            .with_state(prom_handler)
    }

    fn route_influxdb<S>(&self, influxdb_handler: InfluxdbLineProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/write", routing::post(influxdb_write))
            .with_state(influxdb_handler)
    }

    fn route_opentsdb<S>(&self, opentsdb_handler: OpentsdbProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/api/put", routing::post(opentsdb::put))
            .with_state(opentsdb_handler)
    }
}

#[async_trait]
impl Server for HttpServer {
    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take() {
            if tx.send(()).is_err() {
                info!("Receiver dropped, the HTTP server has already existed");
            }
        }
        info!("Shutdown HTTP server");

        Ok(())
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (tx, rx) = oneshot::channel();
        let server = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu { server: "HTTP" }
            );

            let app = self.make_app();
            let server = axum::Server::bind(&listening).serve(app.into_make_service());

            *shutdown_tx = Some(tx);

            server
        };
        let listening = server.local_addr();
        info!("HTTP server is bound to {}", listening);

        let graceful = server.with_graceful_shutdown(rx.map(drop));
        graceful.await.context(StartHttpSnafu)?;

        Ok(listening)
    }
}

/// handle error middleware
async fn handle_error(err: BoxError) -> Json<JsonResponse> {
    Json(JsonResponse::with_error(
        format!("Unhandled internal error: {}", err),
        StatusCode::Unexpected,
    ))
}

#[cfg(test)]
mod test {
    use std::future::pending;
    use std::sync::Arc;

    use axum::handler::Handler;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum_test_helper::TestClient;
    use common_recordbatch::RecordBatches;
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{StringVector, UInt32Vector};
    use session::context::QueryContextRef;
    use tokio::sync::mpsc;

    use super::*;
    use crate::query_handler::SqlQueryHandler;

    struct DummyInstance {
        _tx: mpsc::Sender<(String, Vec<u8>)>,
    }

    #[async_trait]
    impl SqlQueryHandler for DummyInstance {
        async fn do_query(&self, _: &str, _: QueryContextRef) -> Result<Output> {
            unimplemented!()
        }
    }

    fn timeout() -> TimeoutLayer {
        TimeoutLayer::new(Duration::from_millis(10))
    }

    async fn forever() {
        pending().await
    }

    fn make_test_app(tx: mpsc::Sender<(String, Vec<u8>)>) -> Router {
        let instance = Arc::new(DummyInstance { _tx: tx });
        let server = HttpServer::new(instance, HttpOptions::default());
        server.make_app().route(
            "/test/timeout",
            get(forever.layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_: BoxError| async {
                        StatusCode::REQUEST_TIMEOUT
                    }))
                    .layer(timeout()),
            )),
        )
    }

    #[test]
    fn test_http_options_default() {
        let default = HttpOptions::default();
        assert_eq!("127.0.0.1:4000".to_string(), default.addr);
        assert_eq!(Duration::from_secs(30), default.timeout)
    }

    #[tokio::test]
    async fn test_http_server_request_timeout() {
        let (tx, _rx) = mpsc::channel(100);
        let app = make_test_app(tx);
        let client = TestClient::new(app);
        let res = client.get("/test/timeout").send().await;
        assert_eq!(res.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn test_recordbatches_conversion() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
                None,
            ])),
        ];
        let recordbatch = RecordBatch::new(schema.clone(), columns).unwrap();
        let recordbatches = RecordBatches::try_new(schema.clone(), vec![recordbatch]).unwrap();

        let json_resp = JsonResponse::from_output(Ok(Output::RecordBatches(recordbatches))).await;

        let json_output = &json_resp.output.unwrap()[0];
        if let JsonOutput::Records(r) = json_output {
            assert_eq!(r.num_rows(), 4);
            assert_eq!(r.num_cols(), 2);
            let schema = r.schema.as_ref().unwrap();
            assert_eq!(schema.column_schemas[0].name, "numbers");
            assert_eq!(schema.column_schemas[0].data_type, "UInt32");
            assert_eq!(r.rows[0][0], serde_json::Value::from(1));
            assert_eq!(r.rows[0][1], serde_json::Value::Null);
        } else {
            panic!("invalid output type");
        }
    }
}
