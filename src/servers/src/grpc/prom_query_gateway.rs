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

use api::v1::greptime_request::Request as GrpcRequest;
use api::v1::prometheus_gateway_server::PrometheusGateway;
use api::v1::promql_request::Promql;
use api::v1::{GreptimeRequest, PromqlRequest, PromqlResponse, QueryRequest, ResponseHeader};
use async_trait::async_trait;
use query::parser::PromQuery;
use session::context::QueryContext;
use tonic::{Request, Response};

use crate::grpc::{GreptimeRequestHandler, TonicResult};
use crate::http::handler::PromqlQuery;
use crate::prom::{retrieve_metric_name, PromHandlerRef, PromJsonResponse};

pub struct PrometheusGatewayService {
    handler: PromHandlerRef,
}

#[async_trait]
impl PrometheusGateway for PrometheusGatewayService {
    async fn handle(&self, req: Request<PromqlRequest>) -> TonicResult<Response<PromqlResponse>> {
        let inner = req.into_inner();
        let prom_query = match inner.promql {
            Some(Promql::RangeQuery(range_query)) => PromQuery {
                query: range_query.query,
                start: range_query.start,
                end: range_query.end,
                step: range_query.step,
            },
            _ => unimplemented!(),
        };

        // TODO(ruihang): set query context
        let result = self
            .handler
            .do_query(&prom_query, Arc::new(QueryContext::default()))
            .await;
        let metric_name = retrieve_metric_name(&prom_query.query).unwrap_or_default();
        let json_response = PromJsonResponse::from_query_result(result, metric_name)
            .await
            .0;
        let json_bytes = serde_json::to_string(&json_response).unwrap().into_bytes();

        let response = Response::new(PromqlResponse {
            header: Some(ResponseHeader {}),
            body: json_bytes,
        });
        Ok(response)
    }
}

impl PrometheusGatewayService {
    pub fn new(handler: PromHandlerRef) -> Self {
        Self { handler }
    }
}
