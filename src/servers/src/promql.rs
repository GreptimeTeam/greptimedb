// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::BoxBody;
use axum::extract::{Query, State};
use axum::{routing, Json, Router};
use common_error::prelude::ErrorExt;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::info;
use futures::FutureExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};
use tower::ServiceBuilder;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use tower_http::trace::TraceLayer;

use crate::auth::UserProviderRef;
use crate::error::{AlreadyStartedSnafu, CollectRecordbatchSnafu, Result, StartHttpSnafu};
use crate::http::authorize::HttpAuth;
use crate::server::Server;

pub const PROMQL_API_VERSION: &str = "v1";

pub type PromqlHandlerRef = Arc<dyn PromqlHandler + Send + Sync>;

#[async_trait]
pub trait PromqlHandler {
    async fn do_query(&self, query: &str) -> Result<Output>;
}

pub struct PromqlServer {
    query_handler: PromqlHandlerRef,
    shutdown_tx: Mutex<Option<Sender<()>>>,
    user_provider: Option<UserProviderRef>,
}

impl PromqlServer {
    pub fn create_server(query_handler: PromqlHandlerRef) -> Box<Self> {
        Box::new(PromqlServer {
            query_handler,
            shutdown_tx: Mutex::new(None),
            user_provider: None,
        })
    }

    pub fn set_user_provider(&mut self, user_provider: UserProviderRef) {
        debug_assert!(self.user_provider.is_none());
        self.user_provider = Some(user_provider);
    }

    pub fn make_app(&self) -> Router {
        // TODO(ruihang): implement format_query, series, labels, values, query_examplars and targets methods

        let router = Router::new()
            .route("/query", routing::post(instant_query).get(instant_query))
            .route("/query_range", routing::post(range_query).get(range_query))
            .with_state(self.query_handler.clone());

        Router::new()
            .nest(&format!("/{PROMQL_API_VERSION}"), router)
            // middlewares
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    // custom layer
                    .layer(AsyncRequireAuthorizationLayer::new(
                        HttpAuth::<BoxBody>::new(self.user_provider.clone()),
                    )),
            )
    }
}

#[async_trait]
impl Server for PromqlServer {
    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take() {
            if tx.send(()).is_err() {
                info!("Receiver dropped, the PromQl server has already existed");
            }
        }
        info!("Shutdown PromQL server");

        Ok(())
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (tx, rx) = oneshot::channel();
        let server = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu { server: "PromQL" }
            );

            let app = self.make_app();
            let server = axum::Server::bind(&listening).serve(app.into_make_service());

            *shutdown_tx = Some(tx);

            server
        };
        let listening = server.local_addr();
        info!("PromQL server is bound to {}", listening);

        let graceful = server.with_graceful_shutdown(rx.map(drop));
        graceful.await.context(StartHttpSnafu)?;

        Ok(listening)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct PromqlSeries {
    metric: HashMap<String, String>,
    values: Vec<(i64, String)>,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct PromqlData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PromqlSeries>,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct PromqlJsonResponse {
    status: String,
    data: PromqlData,
    error: Option<String>,
    error_type: Option<String>,
    warnings: Option<Vec<String>>,
}

impl PromqlJsonResponse {
    pub fn error<S1, S2>(error_type: S1, reason: S2) -> Json<Self>
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Json(PromqlJsonResponse {
            status: "error".to_string(),
            data: PromqlData::default(),
            error: Some(reason.into()),
            error_type: Some(error_type.into()),
            warnings: None,
        })
    }

    pub fn success(data: PromqlData) -> Json<Self> {
        Json(PromqlJsonResponse {
            status: "success".to_string(),
            data,
            error: None,
            error_type: None,
            warnings: None,
        })
    }

    /// Convert from `Result<Output>`
    pub async fn from_query_result(result: Result<Output>) -> Json<Self> {
        let response: Result<Json<Self>> = try {
            let json = match result? {
                Output::RecordBatches(batches) => {
                    Self::success(Self::record_batches_to_data(batches)?)
                }
                Output::Stream(stream) => {
                    let record_batches = RecordBatches::try_collect(stream)
                        .await
                        .context(CollectRecordbatchSnafu)?;
                    Self::success(Self::record_batches_to_data(record_batches)?)
                }
                Output::AffectedRows(_) => Self::error(
                    "unexpected result",
                    "expected data result, but got affected rows",
                ),
            };

            json
        };

        response.unwrap_or_else(|err| Self::error(err.status_code().to_string(), err.to_string()))
    }

    /// TODO(ruihang): implement this conversion method
    fn record_batches_to_data(_: RecordBatches) -> Result<PromqlData> {
        let data = PromqlData {
            result_type: "matrix".to_string(),
            result: vec![PromqlSeries {
                metric: vec![("__name__".to_string(), "foo".to_string())]
                    .into_iter()
                    .collect(),
                values: vec![(1, "123.45".to_string())],
            }],
        };

        Ok(data)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct InstantQuery {
    query: String,
    time: Option<String>,
    timeout: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn instant_query(
    State(_handler): State<PromqlHandlerRef>,
    Query(_params): Query<InstantQuery>,
) -> Json<PromqlJsonResponse> {
    PromqlJsonResponse::error(
        "not implemented",
        "instant query api `/query` is not implemented. Use `/range_query` instead.",
    )
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct RangeQuery {
    query: String,
    start: String,
    end: String,
    step: String,
    timeout: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn range_query(
    State(handler): State<PromqlHandlerRef>,
    Query(params): Query<RangeQuery>,
) -> Json<PromqlJsonResponse> {
    let result = handler.do_query(&params.query).await;
    PromqlJsonResponse::from_query_result(result).await
}
