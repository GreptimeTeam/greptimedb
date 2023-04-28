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

//! prom supply the prometheus HTTP API Server compliance
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::BoxBody;
use axum::extract::{Query, State};
use axum::{routing, Form, Json, Router};
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_error::prelude::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::info;
use common_time::util::current_time_rfc3339;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVector;
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use futures::FutureExt;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr as PromqlExpr, MatrixSelector, ParenExpr, SubqueryExpr,
    UnaryExpr, VectorSelector,
};
use query::parser::PromQuery;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use session::context::{QueryContext, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Mutex};
use tower::ServiceBuilder;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;

use crate::auth::UserProviderRef;
use crate::error::{
    AlreadyStartedSnafu, CollectRecordbatchSnafu, InternalSnafu, Result, StartHttpSnafu,
};
use crate::http::authorize::HttpAuth;
use crate::server::Server;

pub const PROM_API_VERSION: &str = "v1";

pub type PromHandlerRef = Arc<dyn PromHandler + Send + Sync>;

#[async_trait]
pub trait PromHandler {
    async fn do_query(&self, query: &PromQuery, query_ctx: QueryContextRef) -> Result<Output>;
}

/// PromServer represents PrometheusServer which handles the compliance with prometheus HTTP API
pub struct PromServer {
    query_handler: PromHandlerRef,
    shutdown_tx: Mutex<Option<Sender<()>>>,
    user_provider: Option<UserProviderRef>,
}

impl PromServer {
    pub fn create_server(query_handler: PromHandlerRef) -> Box<Self> {
        Box::new(PromServer {
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
            .nest(&format!("/api/{PROM_API_VERSION}"), router)
            // middlewares
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    .layer(CompressionLayer::new())
                    // custom layer
                    .layer(AsyncRequireAuthorizationLayer::new(
                        HttpAuth::<BoxBody>::new(self.user_provider.clone()),
                    )),
            )
    }
}

pub const PROM_SERVER: &str = "PROM_SERVER";

#[async_trait]
impl Server for PromServer {
    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take() {
            if tx.send(()).is_err() {
                info!("Receiver dropped, the Prometheus API server has already existed");
            }
        }
        info!("Shutdown Prometheus API server");

        Ok(())
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (tx, rx) = oneshot::channel();
        let server = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu {
                    server: "Prometheus"
                }
            );

            let app = self.make_app();
            let server = axum::Server::bind(&listening).serve(app.into_make_service());

            *shutdown_tx = Some(tx);

            server
        };
        let listening = server.local_addr();
        info!("Prometheus API server is bound to {}", listening);

        let graceful = server.with_graceful_shutdown(rx.map(drop));
        graceful.await.context(StartHttpSnafu)?;

        Ok(listening)
    }

    fn name(&self) -> &str {
        PROM_SERVER
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PromSeries {
    pub metric: HashMap<String, String>,
    pub values: Vec<(f64, String)>,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PromData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: Vec<PromSeries>,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PromJsonResponse {
    pub status: String,
    pub data: PromData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "errorType")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

impl PromJsonResponse {
    pub fn error<S1, S2>(error_type: S1, reason: S2) -> Json<Self>
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Json(PromJsonResponse {
            status: "error".to_string(),
            data: PromData::default(),
            error: Some(reason.into()),
            error_type: Some(error_type.into()),
            warnings: None,
        })
    }

    pub fn success(data: PromData) -> Json<Self> {
        Json(PromJsonResponse {
            status: "success".to_string(),
            data,
            error: None,
            error_type: None,
            warnings: None,
        })
    }

    /// Convert from `Result<Output>`
    pub async fn from_query_result(
        result: Result<Output>,
        metric_name: String,
        result_type: String,
    ) -> Json<Self> {
        let response: Result<Json<Self>> = try {
            let json = match result? {
                Output::RecordBatches(batches) => {
                    Self::success(Self::record_batches_to_data(batches, metric_name)?)
                }
                Output::Stream(stream) => {
                    let record_batches = RecordBatches::try_collect(stream)
                        .await
                        .context(CollectRecordbatchSnafu)?;
                    Self::success(Self::record_batches_to_data(record_batches, metric_name)?)
                }
                Output::AffectedRows(_) => Self::error(
                    "unexpected result",
                    "expected data result, but got affected rows",
                ),
            };

            json
        };

        match response {
            Ok(resp) => resp,
            Err(err) => {
                // Prometheus won't report error if querying nonexist label and metric
                if err.status_code() == StatusCode::TableNotFound
                    || err.status_code() == StatusCode::TableColumnNotFound
                {
                    Self::success(PromData {
                        result_type,
                        ..Default::default()
                    })
                } else {
                    Self::error(err.status_code().to_string(), err.to_string())
                }
            }
        }
    }

    fn record_batches_to_data(batches: RecordBatches, metric_name: String) -> Result<PromData> {
        // infer semantic type of each column from schema.
        // TODO(ruihang): wish there is a better way to do this.
        let mut timestamp_column_index = None;
        let mut tag_column_indices = Vec::new();
        let mut first_field_column_index = None;

        for (i, column) in batches.schema().column_schemas().iter().enumerate() {
            match column.data_type {
                ConcreteDataType::Timestamp(datatypes::types::TimestampType::Millisecond(_)) => {
                    if timestamp_column_index.is_none() {
                        timestamp_column_index = Some(i);
                    }
                }
                ConcreteDataType::Float64(_) => {
                    if first_field_column_index.is_none() {
                        first_field_column_index = Some(i);
                    }
                }
                ConcreteDataType::String(_) => {
                    tag_column_indices.push(i);
                }
                _ => {}
            }
        }

        let timestamp_column_index = timestamp_column_index.context(InternalSnafu {
            err_msg: "no timestamp column found".to_string(),
        })?;
        let first_field_column_index = first_field_column_index.context(InternalSnafu {
            err_msg: "no value column found".to_string(),
        })?;

        let metric_name = (METRIC_NAME.to_string(), metric_name);
        let mut buffer = BTreeMap::<Vec<(String, String)>, Vec<(f64, String)>>::new();

        for batch in batches.iter() {
            // prepare things...
            let tag_columns = tag_column_indices
                .iter()
                .map(|i| {
                    batch
                        .column(*i)
                        .as_any()
                        .downcast_ref::<StringVector>()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let tag_names = tag_column_indices
                .iter()
                .map(|c| batches.schema().column_name_by_index(*c).to_string())
                .collect::<Vec<_>>();
            let timestamp_column = batch
                .column(timestamp_column_index)
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap();
            let field_column = batch
                .column(first_field_column_index)
                .as_any()
                .downcast_ref::<Float64Vector>()
                .unwrap();

            // assemble rows
            for row_index in 0..batch.num_rows() {
                // retrieve tags
                // TODO(ruihang): push table name `__metric__`
                let mut tags = vec![metric_name.clone()];
                for (tag_column, tag_name) in tag_columns.iter().zip(tag_names.iter()) {
                    // TODO(ruihang): add test for NULL tag
                    if let Some(tag_value) = tag_column.get_data(row_index) {
                        tags.push((tag_name.to_string(), tag_value.to_string()));
                    }
                }

                // retrieve timestamp
                let timestamp_millis: i64 = timestamp_column.get_data(row_index).unwrap().into();
                let timestamp = timestamp_millis as f64 / 1000.0;

                // retrieve value
                let value =
                    Into::<f64>::into(field_column.get_data(row_index).unwrap()).to_string();

                buffer.entry(tags).or_default().push((timestamp, value));
            }
        }

        let result = buffer
            .into_iter()
            .map(|(tags, values)| PromSeries {
                metric: tags.into_iter().collect(),
                values,
            })
            .collect::<Vec<_>>();

        let data = PromData {
            result_type: "matrix".to_string(),
            result,
        };

        Ok(data)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct InstantQuery {
    query: Option<String>,
    time: Option<String>,
    timeout: Option<String>,
    db: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn instant_query(
    State(handler): State<PromHandlerRef>,
    Query(params): Query<InstantQuery>,
    Form(form_params): Form<InstantQuery>,
) -> Json<PromJsonResponse> {
    // Extract time from query string, or use current server time if not specified.
    let time = params
        .time
        .or(form_params.time)
        .unwrap_or_else(current_time_rfc3339);
    let prom_query = PromQuery {
        query: params.query.or(form_params.query).unwrap_or_default(),
        start: time.clone(),
        end: time,
        step: "1s".to_string(),
    };

    let db = &params.db.unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
    let (catalog, schema) = super::parse_catalog_and_schema_from_client_database_name(db);

    let query_ctx = QueryContext::with(catalog, schema);

    let result = handler.do_query(&prom_query, Arc::new(query_ctx)).await;
    let (metric_name, result_type) =
        retrieve_metric_name_and_result_type(&prom_query.query).unwrap_or_default();
    PromJsonResponse::from_query_result(result, metric_name, result_type).await
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct RangeQuery {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    step: Option<String>,
    timeout: Option<String>,
    db: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn range_query(
    State(handler): State<PromHandlerRef>,
    Query(params): Query<RangeQuery>,
    Form(form_params): Form<RangeQuery>,
) -> Json<PromJsonResponse> {
    let prom_query = PromQuery {
        query: params.query.or(form_params.query).unwrap_or_default(),
        start: params.start.or(form_params.start).unwrap_or_default(),
        end: params.end.or(form_params.end).unwrap_or_default(),
        step: params.step.or(form_params.step).unwrap_or_default(),
    };

    let db = &params.db.unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
    let (catalog, schema) = super::parse_catalog_and_schema_from_client_database_name(db);

    let query_ctx = QueryContext::with(catalog, schema);

    let result = handler.do_query(&prom_query, Arc::new(query_ctx)).await;
    let (metric_name, result_type) =
        retrieve_metric_name_and_result_type(&prom_query.query).unwrap_or_default();
    PromJsonResponse::from_query_result(result, metric_name, result_type).await
}

pub(crate) fn retrieve_metric_name_and_result_type(promql: &str) -> Option<(String, String)> {
    let promql_expr = promql_parser::parser::parse(promql).ok()?;
    let metric_name = promql_expr_to_metric_name(&promql_expr)?;
    let result_type = promql_expr.value_type().to_string();

    Some((metric_name, result_type))
}

fn promql_expr_to_metric_name(expr: &PromqlExpr) -> Option<String> {
    match expr {
        PromqlExpr::Aggregate(AggregateExpr { expr, .. }) => promql_expr_to_metric_name(expr),
        PromqlExpr::Unary(UnaryExpr { expr }) => promql_expr_to_metric_name(expr),
        PromqlExpr::Binary(BinaryExpr { lhs, rhs, .. }) => {
            promql_expr_to_metric_name(lhs).or(promql_expr_to_metric_name(rhs))
        }
        PromqlExpr::Paren(ParenExpr { expr }) => promql_expr_to_metric_name(expr),
        PromqlExpr::Subquery(SubqueryExpr { expr, .. }) => promql_expr_to_metric_name(expr),
        PromqlExpr::NumberLiteral(_) => None,
        PromqlExpr::StringLiteral(_) => None,
        PromqlExpr::Extension(_) => None,
        PromqlExpr::VectorSelector(VectorSelector { matchers, .. }) => {
            matchers.find_matchers(METRIC_NAME).pop().cloned()
        }
        PromqlExpr::MatrixSelector(MatrixSelector {
            vector_selector, ..
        }) => {
            let VectorSelector { matchers, .. } = vector_selector;
            matchers.find_matchers(METRIC_NAME).pop().cloned()
        }
        PromqlExpr::Call(Call { args, .. }) => {
            args.args.iter().find_map(|e| promql_expr_to_metric_name(e))
        }
    }
}
