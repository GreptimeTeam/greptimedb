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

use api::prometheus::remote::read_request::ResponseType;
use api::prometheus::remote::{Query, QueryResult, ReadRequest, ReadResponse, WriteRequest};
use api::v1::object_expr::Expr;
use api::v1::{query_request, ObjectExpr, QueryRequest};
use async_trait::async_trait;
use client::ObjectResult;
use common_error::prelude::BoxedError;
use common_grpc::flight;
use common_telemetry::logging;
use prost::Message;
use servers::error::{self, Result as ServerResult};
use servers::prometheus::{self, Metrics};
use servers::query_handler::{PrometheusProtocolHandler, PrometheusResponse};
use servers::Mode;
use snafu::{OptionExt, ResultExt};

use crate::instance::Instance;

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

fn object_result_to_query_result(
    table_name: &str,
    object_result: ObjectResult,
) -> ServerResult<QueryResult> {
    let ObjectResult::FlightData(flight_messages) = object_result else { unreachable!() };
    let recordbatches = flight::flight_messages_to_recordbatches(flight_messages)
        .context(error::ConvertFlightMessageSnafu)?;
    Ok(QueryResult {
        timeseries: prometheus::recordbatches_to_timeseries(table_name, recordbatches)?,
    })
}

impl Instance {
    async fn handle_remote_queries(
        &self,
        db: &str,
        queries: &[Query],
    ) -> ServerResult<Vec<(String, ObjectResult)>> {
        let mut results = Vec::with_capacity(queries.len());

        for query in queries {
            let (table_name, sql) = prometheus::query_to_sql(db, query)?;
            logging::debug!(
                "prometheus remote read, table: {}, sql: {}",
                table_name,
                sql
            );

            let query = ObjectExpr {
                header: None,
                expr: Some(Expr::Query(QueryRequest {
                    query: Some(query_request::Query::Sql(sql.to_string())),
                })),
            };
            let object_result = self
                .grpc_query_handler
                .do_query(query)
                .await?
                .try_into()
                .map_err(BoxedError::new)
                .context(error::ExecuteQuerySnafu { query: &sql })?;

            results.push((table_name, object_result));
        }
        Ok(results)
    }
}

#[async_trait]
impl PrometheusProtocolHandler for Instance {
    async fn write(&self, database: &str, request: WriteRequest) -> ServerResult<()> {
        let exprs = prometheus::write_request_to_insert_exprs(database, request.clone())?;
        match self.mode {
            Mode::Standalone => {
                self.handle_inserts(exprs)
                    .await
                    .map_err(BoxedError::new)
                    .with_context(|_| error::ExecuteInsertSnafu {
                        msg: format!("{request:?}"),
                    })?;
            }
            Mode::Distributed => {
                self.dist_insert(exprs)
                    .await
                    .map_err(BoxedError::new)
                    .with_context(|_| error::ExecuteInsertSnafu {
                        msg: format!("{request:?}"),
                    })?;
            }
        }
        Ok(())
    }

    async fn read(&self, database: &str, request: ReadRequest) -> ServerResult<PrometheusResponse> {
        let response_type = negotiate_response_type(&request.accepted_response_types)?;

        // TODO(dennis): use read_hints to speedup query if possible
        let results = self
            .handle_remote_queries(database, &request.queries)
            .await?;

        match response_type {
            ResponseType::Samples => {
                let query_results = results
                    .into_iter()
                    .map(|(table_name, object_result)| {
                        object_result_to_query_result(&table_name, object_result)
                    })
                    .collect::<ServerResult<Vec<_>>>()?;

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

#[cfg(test)]
mod tests {
    use api::prometheus::remote::label_matcher::Type as MatcherType;
    use api::prometheus::remote::{Label, LabelMatcher, Sample};
    use api::v1::CreateDatabaseExpr;

    use super::*;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_prometheus_remote_write_and_read() {
        common_telemetry::init_default_ut_logging();
        let (instance, _guard) =
            tests::create_frontend_instance("test_prometheus_remote_write_and_read").await;

        let write_request = WriteRequest {
            timeseries: prometheus::mock_timeseries(),
            ..Default::default()
        };

        let db = "prometheus";

        instance
            .handle_create_database(CreateDatabaseExpr {
                database_name: db.to_string(),
            })
            .await
            .unwrap();

        instance.write(db, write_request).await.unwrap();

        let read_request = ReadRequest {
            queries: vec![
                Query {
                    start_timestamp_ms: 1000,
                    end_timestamp_ms: 2000,
                    matchers: vec![LabelMatcher {
                        name: prometheus::METRIC_NAME_LABEL.to_string(),
                        value: "metric1".to_string(),
                        r#type: 0,
                    }],
                    ..Default::default()
                },
                Query {
                    start_timestamp_ms: 1000,
                    end_timestamp_ms: 3000,
                    matchers: vec![
                        LabelMatcher {
                            name: prometheus::METRIC_NAME_LABEL.to_string(),
                            value: "metric3".to_string(),
                            r#type: 0,
                        },
                        LabelMatcher {
                            name: "app".to_string(),
                            value: "biz".to_string(),
                            r#type: MatcherType::Eq as i32,
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let resp = instance.read(db, read_request).await.unwrap();
        assert_eq!(resp.content_type, "application/x-protobuf");
        assert_eq!(resp.content_encoding, "snappy");
        let body = prometheus::snappy_decompress(&resp.body).unwrap();
        let read_response = ReadResponse::decode(&body[..]).unwrap();
        let query_results = read_response.results;
        assert_eq!(2, query_results.len());

        assert_eq!(1, query_results[0].timeseries.len());
        let timeseries = &query_results[0].timeseries[0];

        assert_eq!(
            vec![
                Label {
                    name: prometheus::METRIC_NAME_LABEL.to_string(),
                    value: "metric1".to_string(),
                },
                Label {
                    name: "job".to_string(),
                    value: "spark".to_string(),
                },
            ],
            timeseries.labels
        );

        assert_eq!(
            timeseries.samples,
            vec![
                Sample {
                    value: 1.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 2.0,
                    timestamp: 2000,
                }
            ]
        );

        assert_eq!(1, query_results[1].timeseries.len());
        let timeseries = &query_results[1].timeseries[0];

        assert_eq!(
            vec![
                Label {
                    name: prometheus::METRIC_NAME_LABEL.to_string(),
                    value: "metric3".to_string(),
                },
                Label {
                    name: "idc".to_string(),
                    value: "z002".to_string(),
                },
                Label {
                    name: "app".to_string(),
                    value: "biz".to_string(),
                },
            ],
            timeseries.labels
        );

        assert_eq!(
            timeseries.samples,
            vec![
                Sample {
                    value: 5.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 6.0,
                    timestamp: 2000,
                },
                Sample {
                    value: 7.0,
                    timestamp: 3000,
                }
            ]
        );
    }
}
