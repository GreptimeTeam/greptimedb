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
use common_error::ext::ErrorExt;
use common_telemetry::timer;
use common_time::util::current_time_rfc3339;
use promql_parser::parser::ValueType;
use query::parser::PromQuery;
use session::context::QueryContext;
use snafu::OptionExt;
use tonic::{Request, Response};

use crate::error::InvalidQuerySnafu;
use crate::grpc::handler::create_query_context;
use crate::grpc::TonicResult;
use crate::prometheus::{
    retrieve_metric_name_and_result_type, PrometheusHandlerRef, PrometheusJsonResponse,
};

pub struct PrometheusGatewayService {
    handler: PrometheusHandlerRef,
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
                }
            }
        };

        let query_context = create_query_context(inner.header.as_ref());
        let json_response = self
            .handle_inner(prom_query, query_context, is_range_query)
            .await;
        let json_bytes = serde_json::to_string(&json_response).unwrap().into_bytes();

        let response = Response::new(PromqlResponse {
            header: Some(ResponseHeader {}),
            body: json_bytes,
        });
        Ok(response)
    }
}

impl PrometheusGatewayService {
    pub fn new(handler: PrometheusHandlerRef) -> Self {
        Self { handler }
    }

    async fn handle_inner(
        &self,
        query: PromQuery,
        ctx: Arc<QueryContext>,
        is_range_query: bool,
    ) -> PrometheusJsonResponse {
        let _timer = timer!(
            crate::metrics::METRIC_SERVER_GRPC_PROM_REQUEST_TIMER,
            &[(crate::metrics::METRIC_DB_LABEL, ctx.get_db_string())]
        );

        let result = self.handler.do_query(&query, ctx).await;
        let (metric_name, mut result_type) =
            match retrieve_metric_name_and_result_type(&query.query) {
                Ok((metric_name, result_type)) => (metric_name.unwrap_or_default(), result_type),
                Err(err) => {
                    return PrometheusJsonResponse::error(
                        err.status_code().to_string(),
                        err.to_string(),
                    )
                    .0
                }
            };
        // range query only returns matrix
        if is_range_query {
            result_type = ValueType::Matrix;
        };

        PrometheusJsonResponse::from_query_result(result, metric_name, result_type)
            .await
            .0
    }
}
