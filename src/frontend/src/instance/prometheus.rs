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

use api::prometheus::remote::read_request::ResponseType;
use api::prometheus::remote::{Query, QueryResult, ReadRequest, ReadResponse, WriteRequest};
use api::v1::greptime_request::Request;
use api::v1::{query_request, QueryRequest};
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::logging;
use metrics::counter;
use prometheus::{self, Metrics};
use prost::Message;
use servers::error::{self, Result as ServerResult};
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::{PrometheusProtocolHandler, PrometheusResponse};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::instance::Instance;
use crate::metrics::PROMETHEUS_REMOTE_WRITE_SAMPLES;

const SAMPLES_RESPONSE_TYPE: i32 = ResponseType::Samples as i32;

#[inline]
fn is_supported(response_type: i32) -> bool {
    // Only supports samples response right now
    response_type == SAMPLES_RESPONSE_TYPE
}

/// Negotiating the content type of the remote read response.
///
/// Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
/// implemented by server, error is returned.
/// For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
fn negotiate_response_type(accepted_response_types: &[i32]) -> ServerResult<ResponseType> {
    if accepted_response_types.is_empty() {
        return Ok(ResponseType::Samples);
    }

    let response_type = accepted_response_types
        .iter()
        .find(|t| is_supported(**t))
        .with_context(|| error::NotSupportedSnafu {
            feat: format!(
                "server does not support any of the requested response types: {accepted_response_types:?}",
            ),
        })?;

    // It's safe to unwrap here, we known that it should be SAMPLES_RESPONSE_TYPE
    Ok(ResponseType::from_i32(*response_type).unwrap())
}

async fn to_query_result(table_name: &str, output: Output) -> ServerResult<QueryResult> {
    let Output::Stream(stream) = output else { unreachable!() };
    let recordbatches = RecordBatches::try_collect(stream)
        .await
        .context(error::CollectRecordbatchSnafu)?;
    Ok(QueryResult {
        timeseries: prometheus::recordbatches_to_timeseries(table_name, recordbatches)?,
    })
}

impl Instance {
    async fn handle_remote_queries(
        &self,
        ctx: QueryContextRef,
        queries: &[Query],
    ) -> ServerResult<Vec<(String, Output)>> {
        let mut results = Vec::with_capacity(queries.len());

        for query in queries {
            let (table_name, sql) = prometheus::query_to_sql(query)?;
            logging::debug!(
                "prometheus remote read, table: {}, sql: {}",
                table_name,
                sql
            );

            let query = Request::Query(QueryRequest {
                query: Some(query_request::Query::Sql(sql.to_string())),
            });
            let output = self
                .do_query(query, ctx.clone())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?;

            results.push((table_name, output));
        }
        Ok(results)
    }
}

#[async_trait]
impl PrometheusProtocolHandler for Instance {
    async fn write(&self, request: WriteRequest, ctx: QueryContextRef) -> ServerResult<()> {
        let (requests, samples, _metrics_labels) = prometheus::to_grpc_insert_requests(request)?;
        self.handle_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        counter!(PROMETHEUS_REMOTE_WRITE_SAMPLES, samples as u64);
        Ok(())
    }

    async fn read(
        &self,
        request: ReadRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<PrometheusResponse> {
        let response_type = negotiate_response_type(&request.accepted_response_types)?;

        // TODO(dennis): use read_hints to speedup query if possible
        let results = self.handle_remote_queries(ctx, &request.queries).await?;

        match response_type {
            ResponseType::Samples => {
                let mut query_results = Vec::with_capacity(results.len());
                for (table_name, output) in results {
                    query_results.push(to_query_result(&table_name, output).await?);
                }

                let response = ReadResponse {
                    results: query_results,
                };

                // TODO(dennis): may consume too much memory, adds flow control
                Ok(PrometheusResponse {
                    content_type: "application/x-protobuf".to_string(),
                    content_encoding: "snappy".to_string(),
                    body: prometheus::snappy_compress(&response.encode_to_vec())?,
                })
            }
            ResponseType::StreamedXorChunks => error::NotSupportedSnafu {
                feat: "streamed remote read",
            }
            .fail(),
        }
    }

    async fn ingest_metrics(&self, _metrics: Metrics) -> ServerResult<()> {
        todo!();
    }
}
