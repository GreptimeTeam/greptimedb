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

use aide::openapi::{Info, OpenApi, Server as OpenAPIServer};
use async_trait::async_trait;
use axum::body::BoxBody;
use axum::error_handling::HandleErrorLayer;
use axum::extract::{Query, State};
use axum::{routing, BoxError, Json, Router};
use common_query::Output;
use common_runtime::Runtime;
use common_telemetry::info;
use futures::FutureExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use tower_http::trace::TraceLayer;

use crate::auth::UserProviderRef;
use crate::error::{AlreadyStartedSnafu, Result, StartHttpSnafu};
use crate::http::authorize::HttpAuth;
use crate::http::JsonResponse;
use crate::server::{BaseTcpServer, Server};

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

    fn make_app(&self) -> Router {
        let api = OpenApi {
            info: Info {
                title: "GreptimeDB PromQL API".to_string(),
                description: Some("PromQL HTTP Api in GreptimeDB".to_string()),
                version: PROMQL_API_VERSION.to_string(),
                ..Info::default()
            },
            servers: vec![OpenAPIServer {
                url: format!("/{PROMQL_API_VERSION}"),
                ..OpenAPIServer::default()
            }],
            ..OpenApi::default()
        };

        // TODO(ruihang): implement format_query, series, labels, values, query_examplars and targets methods
        let router = Router::new()
            .route(
                &format!("/{PROMQL_API_VERSION}/query"),
                routing::post(instant_query).get(instant_query),
            )
            .route(
                &format!("/{PROMQL_API_VERSION}/range_query"),
                routing::post(range_query).get(range_query),
            )
            .with_state(self.query_handler.clone());

        router
            // middlewares
            .layer(
                ServiceBuilder::new()
                    // .layer(HandleErrorLayer::new(handle_error))
                    .layer(TraceLayer::new_for_http())
                    // .layer(TimeoutLayer::new(self.options.timeout))
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
pub struct PromqlJsonResponse {
    status: String,
    data: HashMap<(), ()>,
    error: Option<String>,
    error_type: Option<String>,
    warnings: Option<Vec<String>>,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct InstantQuery {
    query: String,
    time: Option<String>,
    timeout: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn instant_query(
    State(handler): State<PromqlHandlerRef>,
    Query(params): Query<InstantQuery>,
) -> Json<PromqlJsonResponse> {
    Json(PromqlJsonResponse {
        status: "error".to_string(),
        data: HashMap::new(),
        error: Some("not implemented".to_string()),
        error_type: Some("not implemented".to_string()),
        warnings: None,
    })
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
) -> Json<JsonResponse> {
    let result = handler.do_query(&params.query).await;

    // let request = decode_remote_read_request(body).await?;

    // handler
    //     .read(params.db.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME), request)
    //     .await

    todo!()
}

/// handle error middleware
async fn handle_error(err: BoxError) -> Json<PromqlJsonResponse> {
    Json(PromqlJsonResponse {
        status: "error".to_string(),
        data: HashMap::new(),
        error: Some(format!("Unhandled internal error: {err}")),
        error_type: Some("internal".to_string()),
        warnings: None,
    })
}
