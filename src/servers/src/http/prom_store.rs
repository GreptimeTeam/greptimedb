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

use std::sync::Arc;

use api::prom_store::remote::ReadRequest;
use api::v1::RowInsertRequests;
use async_trait::async_trait;
use axum::Extension;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{Extensions, HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum_extra::TypedHeader;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing;
use mime_guess::mime;
use pipeline::util::to_pipeline_version;
use pipeline::{ContextReq, PipelineDefinition};
use prometheus::HistogramTimer;
use prost::Message;
use serde::{Deserialize, Serialize};
use session::context::{Channel, QueryContext, QueryContextRef};
use snafu::prelude::*;
use table::requests::{
    METADATA_QUALITY_INFERRED, SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_PER_TABLE_INDEX_KEY,
    SEMANTIC_SIGNAL_TYPE, SEMANTIC_SOURCE, SEMANTIC_SOURCE_VERSION, SIGNAL_TYPE_METRIC,
    SOURCE_PROMETHEUS,
};

use crate::batcher::logical_table::LogicalTablePendingRowsBatcher;
use crate::error::{self, InternalSnafu, PipelineSnafu, Result};
use crate::http::extractor::PipelineInfo;
use crate::http::header::{
    CONTENT_TYPE_PROTOBUF_STR, GREPTIME_DB_HEADER_METRICS, write_cost_header_map,
};
use crate::http::memory_limit::BodyMemoryAccounting;
use crate::prom_remote_write::decode::PromSeriesProcessor;
use crate::prom_remote_write::v2::decode_remote_write_v2;
use crate::prom_remote_write::validation::PromValidationMode;
use crate::prom_remote_write::{
    REMOTE_WRITE_V1_VERSION, REMOTE_WRITE_V2_VERSION, decode_remote_write_request,
};
use crate::prom_store::{MAX_DECOMPRESSED_REQUEST_SIZE, snappy_decompress_limited};
use crate::query_handler::{PipelineHandlerRef, PromStoreProtocolHandlerRef, PromStoreResponse};
use crate::request_memory_limiter::ServerMemoryLimiter;

pub const PHYSICAL_TABLE_PARAM: &str = "physical_table";
pub const DEFAULT_ENCODING: &str = "snappy";
pub const VM_ENCODING: &str = "zstd";
pub const VM_PROTO_VERSION: &str = "1";
const REMOTE_WRITE_V1_PROTO: &str = "prometheus.WriteRequest";
const REMOTE_WRITE_V2_PROTO: &str = "io.prometheus.write.v2.Request";
const CONTENT_TYPE_PROTO_PARAM: &str = "proto";
const REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER: &str = "x-prometheus-remote-write-samples-written";
const REMOTE_WRITE_V2_HISTOGRAMS_WRITTEN_HEADER: &str =
    "x-prometheus-remote-write-histograms-written";
const REMOTE_WRITE_V2_EXEMPLARS_WRITTEN_HEADER: &str =
    "x-prometheus-remote-write-exemplars-written";

#[derive(Clone)]
pub struct PromStoreState {
    pub prom_store_handler: PromStoreProtocolHandlerRef,
    pub pipeline_handler: Option<PipelineHandlerRef>,
    pub prom_store_with_metric_engine: bool,
    pub prom_validation_mode: PromValidationMode,
    pub pending_rows_batcher: Option<Arc<LogicalTablePendingRowsBatcher>>,
    /// Shared request-memory limiter used to charge decompressed remote
    /// read/write bodies against the aggregate quota.
    pub memory_limiter: ServerMemoryLimiter,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteWriteQuery {
    pub db: Option<String>,
    /// Specify which physical table to use for storing metrics.
    /// This only works on remote write requests.
    pub physical_table: Option<String>,
    /// For VictoriaMetrics modified remote write protocol
    pub get_vm_proto_version: Option<String>,
}

impl Default for RemoteWriteQuery {
    fn default() -> RemoteWriteQuery {
        Self {
            db: Some(DEFAULT_SCHEMA_NAME.to_string()),
            physical_table: Some(GREPTIME_PHYSICAL_TABLE.to_string()),
            get_vm_proto_version: None,
        }
    }
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "remote_write")
)]
pub async fn remote_write(
    State(state): State<PromStoreState>,
    Query(params): Query<RemoteWriteQuery>,
    (Extension(query_ctx), extensions): (Extension<QueryContext>, Extensions),
    content_type: Option<TypedHeader<headers::ContentType>>,
    pipeline_info: PipelineInfo,
    content_encoding: TypedHeader<headers::ContentEncoding>,
    body: Bytes,
) -> Result<axum::response::Response> {
    let is_zstd = content_encoding.contains(VM_ENCODING);

    match remote_write_proto(content_type) {
        RemoteWriteProto::V1 => {
            remote_write_v1(state, params, query_ctx, pipeline_info, is_zstd, body).await
        }
        RemoteWriteProto::V2 => {
            if let Some(response) = unsupported_remote_write_v2_encoding_response(&content_encoding)
            {
                return Ok(response);
            }
            // Read after Bytes has collected every frame, including chunked-body reservations.
            let body_reserved_bytes = extensions
                .get::<BodyMemoryAccounting>()
                .map_or(0, BodyMemoryAccounting::reserved_bytes);
            remote_write_v2(
                state,
                params,
                query_ctx,
                pipeline_info,
                is_zstd,
                body,
                body_reserved_bytes,
            )
            .await
        }
        RemoteWriteProto::Unsupported(content_type) => Ok((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported prometheus remote write content type: {content_type}"),
        )
            .into_response()),
    }
}

async fn remote_write_v1(
    state: PromStoreState,
    params: RemoteWriteQuery,
    query_ctx: QueryContext,
    pipeline_info: PipelineInfo,
    is_zstd: bool,
    body: Bytes,
) -> Result<axum::response::Response> {
    let PromStoreState {
        prom_store_handler,
        pipeline_handler,
        prom_store_with_metric_engine,
        prom_validation_mode,
        pending_rows_batcher,
        memory_limiter,
    } = state;

    if let Some(response) = vm_proto_version_response(&params) {
        return Ok(response);
    }

    let (db, query_ctx, _timer) =
        prepare_remote_write_context(&params, query_ctx, REMOTE_WRITE_V1_VERSION);
    let query_ctx = Arc::new(query_ctx);

    let mut processor = PromSeriesProcessor::default_processor();

    if let Some(pipeline_name) = pipeline_info.pipeline_name {
        let pipeline_def = PipelineDefinition::from_name(
            &pipeline_name,
            to_pipeline_version(pipeline_info.pipeline_version.as_deref())
                .context(PipelineSnafu)?,
            None,
        )
        .context(PipelineSnafu)?;
        let pipeline_handler = pipeline_handler.context(InternalSnafu {
            err_msg: "pipeline handler is not set".to_string(),
        })?;

        processor.set_pipeline(pipeline_handler, query_ctx.clone(), pipeline_def);
    }

    // Keep the decode guards alive until the batches are built: the
    // `TablesBuilder` retains the decompressed buffer as its raw data.
    let (mut decoded, decode_guards) = decode_remote_write_request(
        is_zstd,
        body,
        prom_validation_mode,
        &mut processor,
        &memory_limiter,
    )
    .await?;

    // Parsing borrows the decode buffer, but row building copies out of it: tag
    // values through `decode_string`, column names through `to_owned`, and the
    // borrowing `col_indexes` dies inside `as_insert_requests`. Nothing below
    // references the buffer, so it need not span the write — the same goes for
    // the decode guards charged against the aggregate memory quota.
    let req = if processor.use_pipeline {
        drop(decoded);
        processor.exec_pipeline().await?
    } else {
        let req = decoded.as_insert_requests();
        drop(decoded);
        req
    };
    drop(decode_guards);
    let batches = into_prom_write_batches(req, query_ctx);

    let outcome = match write_prometheus_rows_with_progress(
        prom_store_handler,
        pending_rows_batcher,
        prom_store_with_metric_engine,
        batches,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            record_remote_write_samples(&db, REMOTE_WRITE_V1_VERSION, error.rows_written);
            return Err(error.error);
        }
    };
    record_remote_write_samples(&db, REMOTE_WRITE_V1_VERSION, outcome.rows_written);

    Ok((
        StatusCode::NO_CONTENT,
        write_cost_header_map(outcome.write_cost),
    )
        .into_response())
}

async fn remote_write_v2(
    state: PromStoreState,
    params: RemoteWriteQuery,
    query_ctx: QueryContext,
    pipeline_info: PipelineInfo,
    is_zstd: bool,
    body: Bytes,
    body_reserved_bytes: u64,
) -> Result<axum::response::Response> {
    let PromStoreState {
        prom_store_handler,
        pipeline_handler: _,
        prom_store_with_metric_engine,
        prom_validation_mode: _,
        pending_rows_batcher,
        memory_limiter,
    } = state;

    if let Some(response) = vm_proto_version_response(&params) {
        return Ok(response);
    }

    // Pipeline processing is not supported for remote write v2 yet. Ignore the
    // optional pipeline parameter and ingest samples directly.
    let _ = pipeline_info;

    let (db, mut query_ctx, _timer) =
        prepare_remote_write_context(&params, query_ctx, REMOTE_WRITE_V2_VERSION);

    let req =
        match decode_remote_write_v2(is_zstd, body, &memory_limiter, body_reserved_bytes).await {
            Ok(req) => req,
            Err(error) => return Ok(remote_write_v2_error_response(error, 0, 0, 0)),
        };
    // The v2 per-series metadata upgrades the written tables' semantic options
    // (metric type/unit, declared quality) at auto-create time.
    if let Some(index) = req.semantic_index.encode(&query_ctx.current_schema()) {
        query_ctx.set_extension(SEMANTIC_PER_TABLE_INDEX_KEY, index);
    }
    let query_ctx = Arc::new(query_ctx);
    let (sample_count, histogram_count) = (req.sample_count, req.histogram_count);
    let mut sample_batches = into_prom_write_batches(req.samples, query_ctx.clone());
    let mut histogram_batches = into_prom_write_batches(req.histograms, query_ctx);
    for (batches, histogram) in [(&mut sample_batches, false), (&mut histogram_batches, true)] {
        let counter = if histogram {
            crate::metrics::PROM_STORE_REMOTE_WRITE_HISTOGRAMS
                .with_label_values(&[&db, REMOTE_WRITE_V2_VERSION])
        } else {
            crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES
                .with_label_values(&[&db, REMOTE_WRITE_V2_VERSION])
        };
        for (ctx, _) in batches {
            let counter = counter.clone();
            let progress =
                session::write_progress::WriteProgress::with_reservation(req.memory_guard.clone())
                    .with_observer(move |rows| counter.inc_by(rows));
            let mut fork = ctx.fork();
            fork.set_write_progress(Arc::new(progress));
            *ctx = Arc::new(fork);
        }
    }
    let outcome = match write_prometheus_v2_rows_with_progress(
        prom_store_handler,
        pending_rows_batcher,
        prom_store_with_metric_engine,
        sample_batches,
        histogram_batches,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return Ok(remote_write_v2_error_response(
                error.error,
                error.samples_written,
                error.histograms_written,
                0,
            ));
        }
    };
    debug_assert!(outcome.samples_written <= sample_count);
    debug_assert!(outcome.histograms_written <= histogram_count);
    let mut headers = write_cost_header_map(outcome.write_cost);
    append_remote_write_v2_written_headers(
        &mut headers,
        outcome.samples_written,
        outcome.histograms_written,
        0,
    );

    let status = if outcome.accepted {
        StatusCode::ACCEPTED
    } else {
        StatusCode::NO_CONTENT
    };
    Ok((status, headers).into_response())
}

fn vm_proto_version_response(params: &RemoteWriteQuery) -> Option<axum::response::Response> {
    params
        .get_vm_proto_version
        .as_ref()
        .map(|_| VM_PROTO_VERSION.into_response())
}

/// Returns the context still un-shared so the caller can attach
/// request-derived extensions (the v2 per-table metadata index) before
/// wrapping it in an `Arc`.
fn prepare_remote_write_context(
    params: &RemoteWriteQuery,
    mut query_ctx: QueryContext,
    remote_write_version: &str,
) -> (String, QueryContext, HistogramTimer) {
    let db = params.db.clone().unwrap_or_default();
    query_ctx.set_channel(Channel::Prometheus);
    let physical_table = params
        .physical_table
        .clone()
        .unwrap_or_else(|| GREPTIME_PHYSICAL_TABLE.to_string());
    query_ctx.set_extension(PHYSICAL_TABLE_PARAM, physical_table);
    // Stamp the Prometheus metric identity here, before `as_req_iter` splits into the
    // batched and direct write paths, so both inherit it (the batched path bypasses
    // `PromStoreProtocolHandler::write`). Prometheus remote-write metadata is weak
    // here, so the type is inferred from naming; v2 upgrades tables whose series
    // carry inline metadata via the per-table index.
    query_ctx.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC);
    query_ctx.set_extension(SEMANTIC_SOURCE, SOURCE_PROMETHEUS);
    query_ctx.set_extension(SEMANTIC_SOURCE_VERSION, remote_write_version);
    query_ctx.set_extension(SEMANTIC_METRIC_METADATA_QUALITY, METADATA_QUALITY_INFERRED);
    let timer = crate::metrics::METRIC_HTTP_PROM_STORE_WRITE_ELAPSED
        .with_label_values(&[db.as_str(), remote_write_version])
        .start_timer();

    (db, query_ctx, timer)
}

struct PromWriteOutcome {
    write_cost: usize,
    rows_written: u64,
}

struct PromWriteError {
    error: error::Error,
    rows_written: u64,
}

struct PromWriteV2Outcome {
    accepted: bool,
    write_cost: usize,
    samples_written: u64,
    histograms_written: u64,
}

struct PromWriteV2Error {
    error: error::Error,
    samples_written: u64,
    histograms_written: u64,
}

type PromWriteBatch = (QueryContextRef, RowInsertRequests);

#[async_trait]
trait PromWriteBatcher: Send + Sync {
    async fn submit(&self, requests: RowInsertRequests, ctx: QueryContextRef) -> Result<u64>;
}

#[async_trait]
impl PromWriteBatcher for LogicalTablePendingRowsBatcher {
    async fn submit(&self, requests: RowInsertRequests, ctx: QueryContextRef) -> Result<u64> {
        LogicalTablePendingRowsBatcher::submit(self, requests, ctx).await
    }
}

fn into_prom_write_batches(req: ContextReq, query_ctx: QueryContextRef) -> Vec<PromWriteBatch> {
    req.as_req_iter(query_ctx).collect()
}

async fn preflight_prometheus_rows(
    prom_store_handler: &PromStoreProtocolHandlerRef,
    batches: &mut [PromWriteBatch],
) -> Result<()> {
    for (ctx, reqs) in batches.iter_mut() {
        prom_store_handler.pre_write(reqs, ctx.clone()).await?;
        // Detach from context clones retained by pre-write hooks so the checked
        // schema cannot change before this prepared batch is written.
        *ctx = Arc::new(ctx.fork());
    }
    operator::insert::admit_row_insert_batches(batches)
        .await
        .map_err(common_error::ext::BoxedError::new)
        .context(error::ExecuteGrpcQuerySnafu)?;
    Ok(())
}

/// Writes preflighted PRW batches and keeps the number of persisted rows on error.
///
/// The v2 handler uses that partial progress to return Prometheus' written
/// sample/histogram headers even when a later table write fails.
async fn write_prometheus_rows_with_progress(
    prom_store_handler: PromStoreProtocolHandlerRef,
    pending_rows_batcher: Option<Arc<LogicalTablePendingRowsBatcher>>,
    prom_store_with_metric_engine: bool,
    mut batches: Vec<PromWriteBatch>,
) -> std::result::Result<PromWriteOutcome, PromWriteError> {
    if prom_store_with_metric_engine && let Some(batcher) = pending_rows_batcher {
        // Preflight before the bulk eligibility decision: pre_write hooks
        // may redirect the contexts (e.g. to a per-tenant schema), so
        // eligibility must be evaluated against the prepared batches —
        // and the fallback below reuses them without re-running hooks or
        // admission.
        preflight_prometheus_rows(&prom_store_handler, &mut batches)
            .await
            .map_err(|error| PromWriteError {
                error,
                rows_written: 0,
            })?;
        // Destinations bound to another physical table must stay on the
        // ordinary insert path, which routes per destination; time index
        // units need no check — the bulk encode converts each request to
        // its destination's unit.
        if batcher.accepts_bulk_destinations(batches.iter()).await {
            let mut rows_written = 0;
            for (temp_ctx, reqs) in batches {
                let rows =
                    batcher
                        .submit(reqs, temp_ctx)
                        .await
                        .map_err(|error| PromWriteError {
                            error,
                            rows_written,
                        })?;
                rows_written += rows;
            }
            return Ok(PromWriteOutcome {
                write_cost: 0,
                rows_written,
            });
        }
        return write_prepared_prometheus_rows_with_progress(
            prom_store_handler,
            batches,
            prom_store_with_metric_engine,
        )
        .await;
    }

    let row_counts = batches
        .iter()
        .map(|(_, request)| prom_write_row_count(request))
        .collect::<Vec<_>>();
    let batch_count = batches.len();
    let outputs = prom_store_handler
        .write_all(batches, prom_store_with_metric_engine)
        .await
        .map_err(|error| PromWriteError {
            error,
            rows_written: 0,
        })?;
    let output_count = outputs.len();
    let mut write_cost = 0;
    let mut rows_written = 0;
    for (output, rows) in outputs.into_iter().zip(row_counts) {
        let output = output.map_err(|error| PromWriteError {
            error,
            rows_written,
        })?;
        write_cost += output.meta.cost;
        rows_written += rows;
    }
    if output_count != batch_count {
        return Err(PromWriteError {
            error: incomplete_prom_write_error(),
            rows_written,
        });
    }

    Ok(PromWriteOutcome {
        write_cost,
        rows_written,
    })
}

/// Writes already-preflighted batches through the ordinary (prepared) write
/// path; the pre_write hooks and admission have already run.
async fn write_prepared_prometheus_rows_with_progress(
    prom_store_handler: PromStoreProtocolHandlerRef,
    batches: Vec<PromWriteBatch>,
    prom_store_with_metric_engine: bool,
) -> std::result::Result<PromWriteOutcome, PromWriteError> {
    let mut write_cost = 0;
    let mut rows_written = 0;
    for (ctx, request) in batches {
        let rows = prom_write_row_count(&request);
        let output = prom_store_handler
            .write_prepared(request, ctx, prom_store_with_metric_engine)
            .await
            .map_err(|error| PromWriteError {
                error,
                rows_written,
            })?;
        write_cost += output.meta.cost;
        rows_written += rows;
    }
    Ok(PromWriteOutcome {
        write_cost,
        rows_written,
    })
}

async fn write_prometheus_v2_rows_with_progress(
    prom_store_handler: PromStoreProtocolHandlerRef,
    pending_rows_batcher: Option<Arc<LogicalTablePendingRowsBatcher>>,
    prom_store_with_metric_engine: bool,
    sample_batches: Vec<PromWriteBatch>,
    histogram_batches: Vec<PromWriteBatch>,
) -> std::result::Result<PromWriteV2Outcome, PromWriteV2Error> {
    let sample_batch_count = sample_batches.len();
    let mut batches = sample_batches;
    batches.extend(histogram_batches);
    preflight_prometheus_rows(&prom_store_handler, &mut batches)
        .await
        .map_err(|error| PromWriteV2Error {
            error,
            samples_written: 0,
            histograms_written: 0,
        })?;
    let batcher = if prom_store_with_metric_engine
        && let Some(batcher) = pending_rows_batcher.as_ref()
        && batcher.accepts_bulk_destinations(batches.iter()).await
    {
        Some(batcher.as_ref() as &dyn PromWriteBatcher)
    } else {
        None
    };
    write_batched_prometheus_v2_rows_with_progress(
        prom_store_handler,
        batcher,
        prom_store_with_metric_engine,
        sample_batch_count,
        batches,
    )
    .await
}

async fn write_batched_prometheus_v2_rows_with_progress(
    prom_store_handler: PromStoreProtocolHandlerRef,
    batcher: Option<&dyn PromWriteBatcher>,
    prom_store_with_metric_engine: bool,
    sample_batch_count: usize,
    mut batches: Vec<PromWriteBatch>,
) -> std::result::Result<PromWriteV2Outcome, PromWriteV2Error> {
    let progress = batches
        .iter_mut()
        .map(|(ctx, _)| {
            if ctx.write_progress().is_none() {
                let mut fork = ctx.fork();
                fork.set_write_progress(
                    Arc::new(session::write_progress::WriteProgress::default()),
                );
                *ctx = Arc::new(fork);
            }
            ctx.write_progress().cloned().unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let counts = || {
        let samples = progress[..sample_batch_count]
            .iter()
            .map(|p| p.written())
            .sum();
        let histograms = progress[sample_batch_count..]
            .iter()
            .map(|p| p.written())
            .sum();
        (samples, histograms)
    };
    let mut write_cost = 0;
    let mut samples_accepted = 0;
    let mut histograms_accepted = 0;
    for (index, (ctx, requests)) in batches.into_iter().enumerate() {
        let result = if index < sample_batch_count
            && let Some(batcher) = batcher
        {
            batcher
                .submit(requests, ctx)
                .await
                .map(|rows| (rows, 0, false))
        } else {
            prom_store_handler
                .write_prepared(requests, ctx, prom_store_with_metric_engine)
                .await
                .map(|output| {
                    let pending = !output.meta.write_completions.is_empty();
                    let (rows, cost) = output.extract_rows_and_cost();
                    (rows as u64, cost, pending)
                })
        };
        match result {
            Ok((rows, cost, pending)) => {
                write_cost += cost;
                if index < sample_batch_count {
                    samples_accepted += rows;
                } else {
                    histograms_accepted += rows;
                }
                if pending {
                    progress[index].mark_accepted();
                }
                if !progress[index].accepted() {
                    progress[index].confirm(rows);
                }
            }
            Err(error) => {
                for progress in &progress {
                    progress.wait().await;
                }
                let (samples_written, histograms_written) = counts();
                return Err(PromWriteV2Error {
                    error,
                    samples_written,
                    histograms_written,
                });
            }
        }
    }
    Ok(PromWriteV2Outcome {
        write_cost,
        // PRW counts accepted rows on success; errors above report confirmed storage writes.
        samples_written: samples_accepted,
        histograms_written: histograms_accepted,
        accepted: progress.iter().any(|p| p.accepted()),
    })
}

fn prom_write_row_count(request: &RowInsertRequests) -> u64 {
    request
        .inserts
        .iter()
        .filter_map(|insert| insert.rows.as_ref().map(|rows| rows.rows.len() as u64))
        .sum()
}

fn incomplete_prom_write_error() -> error::Error {
    InternalSnafu {
        err_msg: "prometheus write handler returned before processing every batch".to_string(),
    }
    .build()
}

fn record_remote_write_samples(db: &str, version: &str, rows: u64) {
    if rows == 0 {
        return;
    }
    crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES
        .with_label_values(&[db, version])
        .inc_by(rows);
}

fn remote_write_v2_error_response(
    error: error::Error,
    samples: u64,
    histograms: u64,
    exemplars: u64,
) -> axum::response::Response {
    let mut response = error.into_response();
    append_remote_write_v2_written_headers(response.headers_mut(), samples, histograms, exemplars);
    response
}

fn append_remote_write_v2_written_headers(
    headers: &mut HeaderMap,
    samples: u64,
    histograms: u64,
    exemplars: u64,
) {
    headers.insert(
        REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER,
        HeaderValue::from_str(&samples.to_string()).expect("u64 header value is valid"),
    );
    headers.insert(
        REMOTE_WRITE_V2_HISTOGRAMS_WRITTEN_HEADER,
        HeaderValue::from_str(&histograms.to_string()).expect("u64 header value is valid"),
    );
    headers.insert(
        REMOTE_WRITE_V2_EXEMPLARS_WRITTEN_HEADER,
        HeaderValue::from_str(&exemplars.to_string()).expect("u64 header value is valid"),
    );
}

enum RemoteWriteProto {
    V1,
    V2,
    Unsupported(mime::Mime),
}

// ref: https://github.com/prometheus/client_golang/blob/74560058a7af7a695db8196c8e84a0754032c6af/exp/api/remote/remote_api.go#L544
fn remote_write_proto(content_type: Option<TypedHeader<headers::ContentType>>) -> RemoteWriteProto {
    let Some(TypedHeader(content_type)) = content_type else {
        return RemoteWriteProto::V1;
    };

    let mime_type: mime::Mime = content_type.into();
    if !mime_type
        .essence_str()
        .eq_ignore_ascii_case(CONTENT_TYPE_PROTOBUF_STR)
    {
        return RemoteWriteProto::Unsupported(mime_type);
    }

    for (name, value) in mime_type.params() {
        if !name.as_str().eq_ignore_ascii_case(CONTENT_TYPE_PROTO_PARAM) {
            continue;
        }

        return match value.as_str() {
            REMOTE_WRITE_V1_PROTO => RemoteWriteProto::V1,
            REMOTE_WRITE_V2_PROTO => RemoteWriteProto::V2,
            _ => RemoteWriteProto::Unsupported(mime_type.clone()),
        };
    }

    RemoteWriteProto::V1
}

fn unsupported_remote_write_v2_encoding_response(
    content_encoding: &headers::ContentEncoding,
) -> Option<axum::response::Response> {
    if content_encoding.contains(DEFAULT_ENCODING) || content_encoding.contains(VM_ENCODING) {
        return None;
    }

    Some((
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!(
            "unsupported prometheus remote write content encoding: only {DEFAULT_ENCODING} and {VM_ENCODING} are supported"
        ),
    )
        .into_response())
}

impl IntoResponse for PromStoreResponse {
    fn into_response(self) -> axum::response::Response {
        let mut header_map = HeaderMap::new();
        header_map.insert(&header::CONTENT_TYPE, self.content_type);
        header_map.insert(&header::CONTENT_ENCODING, self.content_encoding);

        let metrics = if self.resp_metrics.is_empty() {
            None
        } else {
            serde_json::to_string(&self.resp_metrics).ok()
        };
        if let Some(m) = metrics.and_then(|m| HeaderValue::from_str(&m).ok()) {
            header_map.insert(&GREPTIME_DB_HEADER_METRICS, m);
        }

        (header_map, self.body).into_response()
    }
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "remote_read")
)]
pub async fn remote_read(
    State(state): State<PromStoreState>,
    Query(params): Query<RemoteWriteQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    body: Bytes,
) -> Result<PromStoreResponse> {
    let db = params.db.clone().unwrap_or_default();
    query_ctx.set_channel(Channel::Prometheus);

    let request = decode_remote_read_request(body, &state.memory_limiter).await?;

    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_READ_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    state.prom_store_handler.read(request, query_ctx).await
}

async fn decode_remote_read_request(
    body: Bytes,
    limiter: &ServerMemoryLimiter,
) -> Result<ReadRequest> {
    // Holds the memory permits for the decompressed bytes until the protobuf
    // decoding is finished.
    let buf =
        snappy_decompress_limited(&body[..], MAX_DECOMPRESSED_REQUEST_SIZE, limiter, 0).await?;

    ReadRequest::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use api::prom_store::remote::ReadRequest;
    use api::v1::{Row, RowInsertRequest, Rows};
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use axum::{Router, middleware, routing};
    use common_memory_manager::OnExhaustedPolicy;
    use common_query::Output;
    use pipeline::GreptimePipelineParams;
    use session::context::{QueryContext, QueryContextRef};
    use tower::ServiceExt;

    use super::*;
    use crate::http::memory_limit::memory_limit_middleware;
    use crate::prom_remote_write::validation::PromValidationMode;
    use crate::query_handler::PromStoreProtocolHandler;

    #[test]
    fn test_remote_write_proto() {
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf;proto=io.prometheus.write.v2.Request"
            )),
            RemoteWriteProto::V2
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf; proto=\"io.prometheus.write.v2.Request\""
            )),
            RemoteWriteProto::V2
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "APPLICATION/X-PROTOBUF;proto=io.prometheus.write.v2.Request"
            )),
            RemoteWriteProto::V2
        ));
        assert!(matches!(
            remote_write_proto(content_type("application/x-protobuf")),
            RemoteWriteProto::V1
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf;proto=prometheus.WriteRequest"
            )),
            RemoteWriteProto::V1
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/x-protobuf;proto=unknown.WriteRequest"
            )),
            RemoteWriteProto::Unsupported(_)
        ));
        assert!(matches!(
            remote_write_proto(content_type(
                "application/json;proto=io.prometheus.write.v2.Request"
            )),
            RemoteWriteProto::Unsupported(_)
        ));
        assert!(matches!(remote_write_proto(None), RemoteWriteProto::V1));
    }

    fn content_type(value: &str) -> Option<TypedHeader<headers::ContentType>> {
        Some(TypedHeader(std::str::FromStr::from_str(value).unwrap()))
    }

    #[test]
    fn test_prepare_remote_write_context_stamps_semantics() {
        let (_, query_ctx, _timer) = prepare_remote_write_context(
            &RemoteWriteQuery::default(),
            QueryContext::with("greptime", "public"),
            REMOTE_WRITE_V2_VERSION,
        );

        assert_eq!(
            query_ctx.extension(SEMANTIC_SIGNAL_TYPE),
            Some(SIGNAL_TYPE_METRIC)
        );
        assert_eq!(
            query_ctx.extension(SEMANTIC_SOURCE),
            Some(SOURCE_PROMETHEUS)
        );
        assert_eq!(
            query_ctx.extension(SEMANTIC_SOURCE_VERSION),
            Some(REMOTE_WRITE_V2_VERSION)
        );
        assert_eq!(
            query_ctx.extension(SEMANTIC_METRIC_METADATA_QUALITY),
            Some(METADATA_QUALITY_INFERRED)
        );
    }

    #[tokio::test]
    async fn test_mixed_v2_preflights_all_then_batches_only_samples() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let handler: PromStoreProtocolHandlerRef = Arc::new(RecordingPromStoreHandler {
            events: events.clone(),
        });
        let batcher = RecordingPromWriteBatcher {
            events: events.clone(),
        };

        let mut batches = vec![
            test_prom_write_batch("sample"),
            test_prom_write_batch("histogram"),
        ];
        // The caller preflights before choosing the bulk path; the batched
        // writer consumes the prepared batches.
        preflight_prometheus_rows(&handler, &mut batches)
            .await
            .unwrap();
        let Ok(outcome) = write_batched_prometheus_v2_rows_with_progress(
            handler,
            Some(&batcher),
            true,
            1,
            batches,
        )
        .await
        else {
            panic!("mixed remote write should succeed")
        };

        assert_eq!(1, outcome.samples_written);
        assert_eq!(1, outcome.histograms_written);
        assert_eq!(
            vec![
                "pre:sample".to_string(),
                "pre:histogram".to_string(),
                "batch:sample".to_string(),
                "direct:histogram".to_string(),
            ],
            *events.lock().unwrap()
        );
    }

    fn test_prom_write_batch(table_name: &str) -> PromWriteBatch {
        (
            Arc::new(QueryContext::with("greptime", "public")),
            RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: table_name.to_string(),
                    rows: Some(Rows {
                        schema: Vec::new(),
                        rows: vec![Row { values: Vec::new() }],
                    }),
                }],
            },
        )
    }

    #[tokio::test]
    async fn test_async_v2_counts_acceptance_without_confirming_storage() {
        let handler: PromStoreProtocolHandlerRef = Arc::new(RecordingPromStoreHandler {
            events: Default::default(),
        });
        let mut batches = vec![
            test_prom_write_batch("queued"),
            test_prom_write_batch("queued"),
        ];
        let mut progress = Vec::new();
        for (ctx, _) in &mut batches {
            let tracker = Arc::new(session::write_progress::WriteProgress::default());
            Arc::get_mut(ctx)
                .unwrap()
                .set_write_progress(tracker.clone());
            progress.push(tracker);
        }
        preflight_prometheus_rows(&handler, &mut batches)
            .await
            .unwrap();
        let Ok(outcome) =
            write_batched_prometheus_v2_rows_with_progress(handler, None, true, 1, batches).await
        else {
            panic!("queued writes should be accepted")
        };
        assert!(outcome.accepted);
        assert_eq!(outcome.samples_written, 1);
        assert_eq!(outcome.histograms_written, 1);
        assert!(progress.iter().all(|tracker| tracker.written() == 0));
    }

    fn record_write_event(events: &Mutex<Vec<String>>, phase: &str, request: &RowInsertRequests) {
        events.lock().unwrap().push(format!(
            "{phase}:{}",
            request.inserts.first().unwrap().table_name
        ));
    }

    #[tokio::test]
    async fn test_failed_request_waits_for_earlier_queued_writes() {
        struct DelayedBatcher(
            Mutex<
                Option<(
                    Arc<session::write_progress::WriteProgress>,
                    session::write_progress::PendingWrite,
                )>,
            >,
        );
        #[async_trait]
        impl PromWriteBatcher for DelayedBatcher {
            async fn submit(
                &self,
                request: RowInsertRequests,
                ctx: QueryContextRef,
            ) -> Result<u64> {
                let progress = ctx.write_progress().unwrap().clone();
                let pending = progress.start();
                progress.mark_accepted();
                *self.0.lock().unwrap() = Some((progress, pending));
                Ok(prom_write_row_count(&request))
            }
        }
        let handler: PromStoreProtocolHandlerRef = Arc::new(RecordingPromStoreHandler {
            events: Default::default(),
        });
        let batcher = DelayedBatcher(Mutex::new(None));
        let mut batches = vec![
            test_prom_write_batch("queued"),
            test_prom_write_batch("partial"),
        ];
        preflight_prometheus_rows(&handler, &mut batches)
            .await
            .unwrap();
        let write = write_batched_prometheus_v2_rows_with_progress(
            handler,
            Some(&batcher),
            true,
            1,
            batches,
        );
        tokio::pin!(write);
        assert!(futures::poll!(write.as_mut()).is_pending());
        let (progress, pending) = batcher.0.lock().unwrap().take().unwrap();
        progress.add_written(1);
        drop(pending);
        let Err(error) = write.await else {
            panic!("the second batch must fail")
        };
        assert_eq!(error.samples_written, 1);
        assert_eq!(error.histograms_written, 1);
    }

    struct RecordingPromWriteBatcher {
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl PromWriteBatcher for RecordingPromWriteBatcher {
        async fn submit(&self, requests: RowInsertRequests, ctx: QueryContextRef) -> Result<u64> {
            assert_eq!(ctx.write_rows_to_admit("greptime", "public", 1), 0);
            record_write_event(&self.events, "batch", &requests);
            Ok(prom_write_row_count(&requests))
        }
    }

    struct RecordingPromStoreHandler {
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl PromStoreProtocolHandler for RecordingPromStoreHandler {
        async fn pre_write(
            &self,
            request: &RowInsertRequests,
            _ctx: QueryContextRef,
        ) -> Result<()> {
            record_write_event(&self.events, "pre", request);
            Ok(())
        }

        async fn write_prepared(
            &self,
            request: RowInsertRequests,
            ctx: QueryContextRef,
            _with_metric_engine: bool,
        ) -> Result<Output> {
            assert_eq!(ctx.write_rows_to_admit("greptime", "public", 1), 0);
            record_write_event(&self.events, "direct", &request);
            match request.inserts[0].table_name.as_str() {
                "partial" => {
                    ctx.write_progress().unwrap().add_written(1);
                    return Err(InternalSnafu {
                        err_msg: "one region failed",
                    }
                    .build());
                }
                "queued" => ctx.write_progress().unwrap().mark_accepted(),
                _ => {}
            }
            Ok(Output::new_with_affected_rows(
                prom_write_row_count(&request) as usize,
            ))
        }

        async fn write(
            &self,
            _request: RowInsertRequests,
            _ctx: QueryContextRef,
            _with_metric_engine: bool,
        ) -> Result<Output> {
            unreachable!("mixed v2 writes use preflighted execution")
        }

        async fn write_all(
            &self,
            _requests: Vec<(QueryContextRef, RowInsertRequests)>,
            _with_metric_engine: bool,
        ) -> Result<Vec<Result<Output>>> {
            unreachable!("mixed v2 writes preserve sample and histogram routing")
        }

        async fn read(
            &self,
            _request: ReadRequest,
            _ctx: QueryContextRef,
        ) -> Result<PromStoreResponse> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_remote_write_v2_reports_confirmed_partial_and_queued_writes() {
        for (name, expected_status, expected_count) in [
            ("confirmed", StatusCode::NO_CONTENT, "2"),
            ("partial", StatusCode::INTERNAL_SERVER_ERROR, "1"),
            ("queued", StatusCode::ACCEPTED, "2"),
        ] {
            let request = crate::prom_remote_write::v2::test_util::request_with_labels_and_samples(
                vec![("__name__", name)],
                vec![api::greptime_proto::io::prometheus::write::v2::Sample::default(); 2],
            );
            let body =
                Bytes::from(crate::prom_store::snappy_compress(&request.encode_to_vec()).unwrap());
            let mut state = test_state();
            state.prom_store_handler = Arc::new(RecordingPromStoreHandler {
                events: Default::default(),
            });
            let response = remote_write_v2(
                state,
                RemoteWriteQuery::default(),
                QueryContext::with("greptime", "public"),
                pipeline_info(None),
                false,
                body,
                0,
            )
            .await
            .unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(
                response.headers()[REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER],
                expected_count
            );
            assert_eq!(
                response.headers()[REMOTE_WRITE_V2_HISTOGRAMS_WRITTEN_HEADER],
                "0"
            );
            assert_eq!(
                response.headers()[REMOTE_WRITE_V2_EXEMPLARS_WRITTEN_HEADER],
                "0"
            );
        }
    }

    fn memory_limited_prom_write_app(limiter: ServerMemoryLimiter) -> Router {
        let mut state = test_state();
        state.memory_limiter = limiter.clone();
        Router::new()
            .route("/write", routing::post(remote_write))
            .with_state(state)
            .layer(Extension(QueryContext::with("greptime", "public")))
            .layer(middleware::from_fn_with_state(
                limiter,
                memory_limit_middleware,
            ))
    }

    #[tokio::test(start_paused = true)]
    async fn test_remote_write_v2_accounts_for_body_when_rejecting_or_waiting() {
        let wire = api::greptime_proto::io::prometheus::write::v2::Request {
            symbols: vec![
                String::new(),
                format!("{:?}", (0..2048).collect::<Vec<_>>()),
            ],
            timeseries: Vec::new(),
        }
        .encode_to_vec();
        let decoded_bytes = wire.len().div_ceil(1024) * 1024;

        for encoding in [DEFAULT_ENCODING, VM_ENCODING] {
            let body = Bytes::from(if encoding == DEFAULT_ENCODING {
                crate::prom_store::snappy_compress(&wire).unwrap()
            } else {
                zstd::stream::encode_all(wire.as_slice(), 3).unwrap()
            });
            for chunked in [false, true] {
                // Small frames make their rounded permits exceed the rounded total body length.
                let chunk_size = if chunked { 513 } else { body.len() };
                let frames = body
                    .chunks(chunk_size)
                    .map(Bytes::copy_from_slice)
                    .collect::<Vec<_>>();
                let body_reserved: usize =
                    frames.iter().map(|f| f.len().div_ceil(1024) * 1024).sum();
                if chunked {
                    assert!(body_reserved > body.len().div_ceil(1024) * 1024);
                }
                // Reject during decompression or scratch allocation; otherwise wait for contention.
                for decoder_quota in [decoded_bytes - 1024, decoded_bytes, decoded_bytes + 1024] {
                    let limit = (body_reserved + decoder_quota) as u64;
                    let limiter = ServerMemoryLimiter::new(limit, OnExhaustedPolicy::default());
                    let fits = decoder_quota > decoded_bytes;
                    let occupied = fits.then(|| limiter.try_acquire(1024).unwrap());
                    let app = memory_limited_prom_write_app(limiter.clone());
                    let mut request = Request::builder()
                        .uri("/write")
                        .method("POST")
                        .header(
                            header::CONTENT_TYPE,
                            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
                        )
                        .header(header::CONTENT_ENCODING, encoding);
                    let request_body = if chunked {
                        Body::from_stream(futures::stream::iter(
                            frames.clone().into_iter().map(Ok::<_, std::io::Error>),
                        ))
                    } else {
                        request = request.header(header::CONTENT_LENGTH, body.len().to_string());
                        Body::from(body.clone())
                    };
                    let started = tokio::time::Instant::now();
                    let response = app.oneshot(request.body(request_body).unwrap());
                    tokio::pin!(response);
                    if let Some(occupied) = occupied {
                        assert!(futures::poll!(response.as_mut()).is_pending());
                        // A failed attempt releases decoder permits, retaining only the HTTP body.
                        assert_eq!(limiter.used_bytes(), body_reserved as u64 + 1024);
                        drop(occupied);
                    }
                    let response = response.await.unwrap();
                    assert_eq!(
                        response.status(),
                        if fits {
                            StatusCode::NO_CONTENT
                        } else {
                            StatusCode::TOO_MANY_REQUESTS
                        },
                        "encoding={encoding}, chunked={chunked}, decoder_quota={decoder_quota}"
                    );
                    assert_eq!(started.elapsed(), Duration::ZERO);
                    assert_eq!(
                        response.headers()[REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER],
                        "0"
                    );
                    assert_eq!(limiter.used_bytes(), 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_remote_write_v2_ignores_pipeline() {
        let request = api::greptime_proto::io::prometheus::write::v2::Request {
            symbols: vec![String::new()],
            timeseries: Vec::new(),
        };
        let body =
            Bytes::from(crate::prom_store::snappy_compress(&request.encode_to_vec()).unwrap());

        let response = remote_write_v2(
            test_state(),
            RemoteWriteQuery::default(),
            QueryContext::with("greptime", "public"),
            pipeline_info(Some("pipeline")),
            false,
            body,
            0,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            Some("0"),
            response
                .headers()
                .get(REMOTE_WRITE_V2_SAMPLES_WRITTEN_HEADER)
                .map(|x| x.to_str().unwrap())
        );
    }

    fn test_state() -> PromStoreState {
        PromStoreState {
            prom_store_handler: Arc::new(NoopPromStoreHandler),
            pipeline_handler: None,
            prom_store_with_metric_engine: false,
            prom_validation_mode: PromValidationMode::Strict,
            pending_rows_batcher: None,
            memory_limiter: ServerMemoryLimiter::default(),
        }
    }

    fn pipeline_info(pipeline_name: Option<&str>) -> PipelineInfo {
        PipelineInfo {
            pipeline_name: pipeline_name.map(ToString::to_string),
            pipeline_version: None,
            pipeline_params: GreptimePipelineParams::default(),
        }
    }

    struct NoopPromStoreHandler;

    #[async_trait]
    impl PromStoreProtocolHandler for NoopPromStoreHandler {
        async fn pre_write(
            &self,
            _request: &RowInsertRequests,
            _ctx: QueryContextRef,
        ) -> Result<()> {
            Ok(())
        }

        async fn write_prepared(
            &self,
            _request: RowInsertRequests,
            _ctx: QueryContextRef,
            _with_metric_engine: bool,
        ) -> Result<Output> {
            unreachable!("empty remote write v2 request should not write")
        }

        async fn write(
            &self,
            _request: RowInsertRequests,
            _ctx: QueryContextRef,
            _with_metric_engine: bool,
        ) -> Result<Output> {
            unreachable!("empty remote write v2 request should not write")
        }

        async fn write_all(
            &self,
            requests: Vec<(QueryContextRef, RowInsertRequests)>,
            _with_metric_engine: bool,
        ) -> Result<Vec<Result<Output>>> {
            assert!(requests.is_empty());
            Ok(Vec::new())
        }

        async fn read(
            &self,
            _request: ReadRequest,
            _ctx: QueryContextRef,
        ) -> Result<PromStoreResponse> {
            unimplemented!()
        }
    }
}
