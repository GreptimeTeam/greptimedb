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

use std::collections::HashSet;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use api::prometheus::remote::read_request::ResponseType;
use api::prometheus::remote::{Query, QueryResult, ReadRequest, ReadResponse, WriteRequest};
use api::v1::greptime_request::Request;
use api::v1::{query_request, QueryRequest};
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::logging;
use datatypes::value::Value;
use metrics::counter;
use prometheus::labels_cache::LabelsCache;
use prometheus::{self, labels_table, Metrics, MetricsLabelsMap};
use prost::Message;
use servers::error::{self, Result as ServerResult};
use servers::query_handler::grpc::{GrpcQueryHandler, GrpcQueryHandlerRef};
use servers::query_handler::{PrometheusProtocolHandler, PrometheusResponse};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::error::Error;
use crate::instance::Instance;
use crate::metrics::PROMETHEUS_REMOTE_WRITE_SAMPLES;

const SAMPLES_RESPONSE_TYPE: i32 = ResponseType::Samples as i32;
const LABELS_CACHE_CAPACITY: usize = 100000;

/// Manager to manage Prometheus metrics, such as schema etc.
#[derive(Clone)]
pub(crate) struct PromMetricManager {
    state: Arc<AtomicU8>,
    labels_cache: LabelsCache,
    grpc_query_handler: GrpcQueryHandlerRef<Error>,
}

fn string_value(v: Value) -> ServerResult<String> {
    match v {
        Value::String(s) => Ok(s.as_utf8().to_string()),
        other => error::UnexpectedResultSnafu {
            reason: format!("unexpected value: {:?}", other),
        }
        .fail(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    // The metrics and labels are not loaded
    Unload,
    // The metrics and labels are loading.
    Loading,
    // The metrics and labels are loaded.
    Loaded,
}

impl State {
    fn as_u8(self) -> u8 {
        match self {
            State::Unload => 0,
            State::Loading => 1,
            State::Loaded => 2,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => State::Unload,
            1 => State::Loading,
            2 => State::Loaded,
            _ => unreachable!(),
        }
    }
}

impl PromMetricManager {
    pub fn new(grpc_query_handler: GrpcQueryHandlerRef<Error>) -> Self {
        Self {
            labels_cache: LabelsCache::new(LABELS_CACHE_CAPACITY),
            grpc_query_handler,
            state: Arc::new(AtomicU8::new(State::Unload.as_u8())),
        }
    }

    async fn try_load(&self, ctx: &QueryContextRef) -> ServerResult<()> {
        let state = State::from_u8(self.state.load(Ordering::Relaxed));

        let can_load = match state {
            State::Loaded => return Ok(()),
            State::Loading => {
                return error::TryLaterSnafu {
                    reason: "loading prometheus metrics and labels",
                }
                .fail()
            }
            State::Unload => self
                .state
                .compare_exchange(
                    State::Unload.as_u8(),
                    State::Loading.as_u8(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok(),
        };

        if !can_load {
            return error::TryLaterSnafu {
                reason: "loading prometheus metrics and labels",
            }
            .fail();
        }

        if let Err(e) = self.load_metrics_labels(ctx).await {
            logging::error!(e; "Failed to load metrics and labels from {}.{}",
                            ctx.current_catalog(),
                            ctx.current_schema());
            self.state.store(State::Unload.as_u8(), Ordering::Relaxed);
        }

        Ok(())
    }

    async fn load_metrics_labels(&self, ctx: &QueryContextRef) -> ServerResult<()> {
        let sql = labels_table::load_table_sql();
        let query = Request::Query(QueryRequest {
            query: Some(query_request::Query::Sql(sql)),
        });

        let output = self
            .grpc_query_handler
            .do_query(query, ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        let record_batches = match output {
            Output::RecordBatches(batches) => batches,
            Output::Stream(s) => RecordBatches::try_collect(s)
                .await
                .context(error::CollectRecordbatchSnafu)?,
            Output::AffectedRows(_) => unreachable!(),
        };

        let mut metrics_num = 0;
        let mut metric = "".to_string();
        let mut labels: HashSet<String> = HashSet::new();

        for record_batch in record_batches {
            assert_eq!(2, record_batch.num_columns());
            // Safety: these two columns must be present
            let metric_column = record_batch
                .column_by_name(labels_table::METRIC_COLUMN)
                .unwrap();
            let label_column = record_batch
                .column_by_name(labels_table::LABEL_COLUMN)
                .unwrap();
            for row in 0..record_batch.num_rows() {
                let metric_name = string_value(metric_column.get(row))?;
                let label_name = string_value(label_column.get(row))?;

                if metric != metric_name {
                    if !labels.is_empty() {
                        metrics_num += 1;
                        self.labels_cache.put(&metric, labels);
                    }

                    metric = metric_name;
                    labels = HashSet::new();
                }
                labels.insert(label_name.to_string());
            }
        }

        if !labels.is_empty() {
            metrics_num += 1;
            self.labels_cache.put(&metric, labels);
        }

        logging::info!(
            "Loaded {} metrics from {}.{} for prometheus.",
            metrics_num,
            ctx.current_catalog(),
            ctx.current_schema(),
        );

        assert!(self
            .state
            .compare_exchange(
                State::Loading.as_u8(),
                State::Loaded.as_u8(),
                Ordering::Relaxed,
                Ordering::Relaxed
            )
            .is_ok());

        Ok(())
    }

    fn add_labels(&self, metrics_label: MetricsLabelsMap) {
        for (metric, labels) in metrics_label {
            if !labels.is_empty() {
                logging::info!(
                    "Adding new labels: {:?} to metric: {} for prometheus.",
                    labels,
                    metric
                );
                self.labels_cache.add(&metric, labels);
            }
        }
    }

    /// Reserve only new labels in metrics_map.
    fn reserve_new_labels(&self, mut metrics_labels: MetricsLabelsMap) -> MetricsLabelsMap {
        for (metric, labels) in metrics_labels.iter_mut() {
            let new_labels = self.labels_cache.diff_labels(metric, labels.iter());
            *labels = new_labels;
        }

        metrics_labels
    }

    /// Get the metric's labels, return None if not found.
    #[allow(dead_code)]
    fn get_labels(&self, _metric: &str) -> Option<HashSet<String>> {
        let _state = State::from_u8(self.state.load(Ordering::Relaxed));

        todo!();
    }
}

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
        self.prom_metric_manager.try_load(&ctx).await?;

        let (requests, samples, metrics_labels) = prometheus::to_grpc_insert_requests(request)?;
        // Insert samples
        self.handle_inserts(requests, ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;
        counter!(PROMETHEUS_REMOTE_WRITE_SAMPLES, samples as u64);

        // Insert labels
        let new_metrics_labels = self.prom_metric_manager.reserve_new_labels(metrics_labels);
        if let Some(requests) = prometheus::to_grpc_label_insert_requests(&new_metrics_labels)? {
            self.handle_inserts(requests, ctx)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?;
            self.prom_metric_manager.add_labels(new_metrics_labels);
        }

        Ok(())
    }

    async fn read(
        &self,
        request: ReadRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<PrometheusResponse> {
        let response_type = negotiate_response_type(&request.accepted_response_types)?;

        self.prom_metric_manager.try_load(&ctx).await?;

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
