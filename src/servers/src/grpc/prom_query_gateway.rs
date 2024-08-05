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

//! PrometheusGateway provides a gRPC interface to query Prometheus metrics
//! by PromQL. The behavior is similar to the Prometheus HTTP API.

use std::sync::Arc;

use api::v1::prometheus_gateway_server::PrometheusGateway;
use api::v1::promql_request::Promql;
use api::v1::{PromqlRequest, PromqlResponse, ResponseHeader};
use async_trait::async_trait;
use auth::UserProviderRef;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_time::util::current_time_rfc3339;
use promql_parser::parser::value::ValueType;
use query::parser::PromQuery;
use session::context::QueryContext;
use snafu::OptionExt;
use tonic::{Request, Response};

use super::greptime_handler::create_query_context;
use crate::error::InvalidQuerySnafu;
use crate::grpc::greptime_handler::auth;
use crate::grpc::TonicResult;
use crate::http::prometheus::{retrieve_metric_name_and_result_type, PrometheusJsonResponse};
use crate::prometheus_handler::PrometheusHandlerRef;

pub struct PrometheusGatewayService {
    handler: PrometheusHandlerRef,
    user_provider: Option<UserProviderRef>,
}

#[async_trait]
impl PrometheusGateway for PrometheusGatewayService {
    async fn handle(&self, req: Request<PromqlRequest>) -> TonicResult<Response<PromqlResponse>> {
        let mut is_range_query = false;
        let inner = req.into_inner();
        let prom_query = match inner.promql.context(InvalidQuerySnafu {
            reason: "Expecting non-empty PromqlRequest.",
        })? {
            Promql::RangeQuery(range_query) => {
                is_range_query = true;
                PromQuery {
                    query: range_query.query,
                    start: range_query.start,
                    end: range_query.end,
                    step: range_query.step,
                    lookback: range_query.lookback,
                }
            }
            Promql::InstantQuery(instant_query) => {
                let time = if instant_query.time.is_empty() {
                    current_time_rfc3339()
                } else {
                    instant_query.time
                };
                PromQuery {
                    query: instant_query.query,
                    start: time.clone(),
                    end: time,
                    step: String::from("1s"),
                    lookback: instant_query.lookback,
                }
            }
        };

        let header = inner.header.as_ref();
        let query_ctx = create_query_context(header, Default::default());
        let user_info = auth(self.user_provider.clone(), header, &query_ctx).await?;
        query_ctx.set_current_user(user_info);

        let json_response = self
            .handle_inner(prom_query, query_ctx, is_range_query)
            .await;
        let json_bytes = serde_json::to_string(&json_response).unwrap().into_bytes();

        let response = Response::new(PromqlResponse {
            header: Some(ResponseHeader {
                status: Some(api::v1::Status {
                    status_code: StatusCode::Success as _,
                    ..Default::default()
                }),
            }),
            body: json_bytes,
        });
        Ok(response)
    }
}

impl PrometheusGatewayService {
    pub fn new(handler: PrometheusHandlerRef, user_provider: Option<UserProviderRef>) -> Self {
        Self {
            handler,
            user_provider,
        }
    }

    async fn handle_inner(
        &self,
        query: PromQuery,
        ctx: Arc<QueryContext>,
        is_range_query: bool,
    ) -> PrometheusJsonResponse {
        let db = ctx.get_db_string();
        let _timer = crate::metrics::METRIC_SERVER_GRPC_PROM_REQUEST_TIMER
            .with_label_values(&[db.as_str()])
            .start_timer();

        let result = self.handler.do_query(&query, ctx).await;
        let (metric_name, mut result_type) =
            match retrieve_metric_name_and_result_type(&query.query) {
                Ok((metric_name, result_type)) => (metric_name.unwrap_or_default(), result_type),
                Err(err) => {
                    return PrometheusJsonResponse::error(err.status_code(), err.output_msg())
                }
            };
        // range query only returns matrix
        if is_range_query {
            result_type = ValueType::Matrix;
        };

        PrometheusJsonResponse::from_query_result(result, metric_name, result_type).await
    }
}
