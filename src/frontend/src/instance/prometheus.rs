use api::prometheus::remote::{
    read_request::ResponseType, Query, QueryResult, ReadRequest, ReadResponse, WriteRequest,
};
use async_trait::async_trait;
use client::ObjectResult;
use client::{Database, Select};
use common_error::prelude::BoxedError;
use common_telemetry::logging;
use prost::Message;
use servers::error::{self, Result as ServerResult};
use servers::prometheus::{self, Metrics};
use servers::query_handler::{PrometheusProtocolHandler, PrometheusResponse};
use snafu::{OptionExt, ResultExt};

use crate::frontend::Mode;
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
                "server does not support any of the requested response types: {:?}",
                accepted_response_types
            ),
        })?;

    // It's safe to unwrap here, we known that it should be SAMPLES_RESPONSE_TYPE
    Ok(ResponseType::from_i32(*response_type).unwrap())
}

fn object_result_to_query_result(
    table_name: &str,
    object_result: ObjectResult,
) -> ServerResult<QueryResult> {
    let select_result = match object_result {
        ObjectResult::Select(result) => result,
        _ => unreachable!(),
    };

    Ok(QueryResult {
        timeseries: prometheus::select_result_to_timeseries(table_name, select_result)?,
    })
}

async fn handle_remote_queries(
    db: &Database,
    queries: &[Query],
) -> ServerResult<Vec<(String, ObjectResult)>> {
    let mut results = Vec::with_capacity(queries.len());

    for q in queries {
        let (table_name, sql) = prometheus::query_to_sql(q)?;

        logging::debug!(
            "prometheus remote read, table: {}, sql: {}",
            table_name,
            sql
        );

        let object_result = db
            .select(Select::Sql(sql.clone()))
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteQuerySnafu { query: sql })?;

        results.push((table_name, object_result));
    }

    Ok(results)
}

#[async_trait]
impl PrometheusProtocolHandler for Instance {
    async fn write(&self, request: WriteRequest) -> ServerResult<()> {
        match self.mode {
            Mode::Standalone => {
                let exprs = prometheus::write_request_to_insert_exprs(request)?;
                let futures = exprs
                    .iter()
                    .map(|e| self.handle_insert(e))
                    .collect::<Vec<_>>();
                let res = futures_util::future::join_all(futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, crate::error::Error>>();
                res.map_err(BoxedError::new)
                    .context(error::ExecuteInsertSnafu {
                        msg: "failed to write prometheus remote request",
                    })?;
            }
            Mode::Distributed(_) => {
                let inserts = prometheus::write_request_to_insert_reqs(request)?;

                self.dist_insert(inserts)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteInsertSnafu {
                        msg: "execute insert failed",
                    })?;
            }
        }

        Ok(())
    }

    async fn read(&self, request: ReadRequest) -> ServerResult<PrometheusResponse> {
        let response_type = negotiate_response_type(&request.accepted_response_types)?;

        // TODO(dennis): use read_hints to speedup query if possible
        let results = handle_remote_queries(&self.database(), &request.queries).await?;

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
    use api::prometheus::remote::{
        label_matcher::Type as MatcherType, Label, LabelMatcher, Sample,
    };

    use super::*;
    use crate::tests;

    #[tokio::test]
    async fn test_prometheus_remote_write_and_read() {
        common_telemetry::init_default_ut_logging();
        let instance = tests::create_frontend_instance().await;

        let write_request = WriteRequest {
            timeseries: prometheus::mock_timeseries(),
            ..Default::default()
        };

        instance.write(write_request).await.unwrap();

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

        let resp = instance.read(read_request).await.unwrap();
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
