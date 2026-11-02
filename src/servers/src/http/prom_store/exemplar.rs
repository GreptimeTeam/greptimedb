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
use std::sync::Arc;

use api::v1::RowInsertRequests;
use api::v1::value::ValueData;
use common_error::ext::BoxedError;
use common_query::prelude::EXEMPLAR_METRIC_COLUMN;
use futures::future::ready;
use operator::insert::PreparedMetricInserts;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::batcher::logical_table::LogicalTablePendingRowsBatcher;
use crate::error::{self, Result};
use crate::exemplar::{ExemplarRequests, into_rows};
use crate::http::prom_store::PromWriteBatch;
use crate::query_handler::PromStoreProtocolHandlerRef;

#[derive(Default)]
pub(super) struct Outcome {
    pub cost: usize,
    pub accepted: bool,
    /// Accepted rows on success; confirmed storage writes on error.
    pub written: [u64; 3],
    pub error: Option<error::Error>,
}

enum PreparedWrite {
    Metric(PreparedMetricInserts),
    Rows(RowInsertRequests),
}

/// Preflights all destinations together, retaining per-kind write progress.
pub(super) async fn write(
    handler: PromStoreProtocolHandlerRef,
    batcher: Option<Arc<LogicalTablePendingRowsBatcher>>,
    metric_engine: bool,
    samples: Vec<PromWriteBatch>,
    histograms: Vec<PromWriteBatch>,
    exemplars: ExemplarRequests,
    ctx: QueryContextRef,
) -> Outcome {
    match prepare(&handler, samples, histograms, exemplars, ctx).await {
        Ok(writes) => {
            let mut targets = HashSet::new();
            for (_, ctx, request) in writes.iter().filter(|(kind, _, _)| *kind == 2) {
                for rows in request
                    .inserts
                    .iter()
                    .filter_map(|request| request.rows.as_ref())
                {
                    if let Some(index) = rows
                        .schema
                        .iter()
                        .position(|c| c.column_name == EXEMPLAR_METRIC_COLUMN)
                    {
                        for row in &rows.rows {
                            if let Some(ValueData::StringValue(name)) =
                                &row.values[index].value_data
                            {
                                targets.insert((
                                    ctx.current_catalog().to_string(),
                                    ctx.current_schema(),
                                    name.clone(),
                                ));
                            }
                        }
                    }
                }
            }
            let mut sources = Vec::new();
            let mut prepared = Vec::new();
            let mut source_preparation_failed = false;
            for (kind, ctx, request) in writes {
                if kind != 2 && metric_engine {
                    let (pinned, other): (Vec<_>, Vec<_>) =
                        request.inserts.into_iter().partition(|request| {
                            targets.contains(&(
                                ctx.current_catalog().to_string(),
                                ctx.current_schema(),
                                request.table_name.clone(),
                            ))
                        });
                    if !other.is_empty() {
                        prepared.push((
                            kind,
                            ctx.clone(),
                            Ok(PreparedWrite::Rows(RowInsertRequests { inserts: other })),
                        ));
                    }
                    if !pinned.is_empty() {
                        let result = handler
                            .prepare_metric_write(
                                RowInsertRequests { inserts: pinned },
                                ctx.clone(),
                            )
                            .await;
                        if let Ok(request) = &result {
                            sources.extend(request.source_tables().cloned());
                        } else {
                            source_preparation_failed = true;
                        }
                        prepared.push((kind, ctx, result.map(PreparedWrite::Metric)));
                    }
                } else {
                    prepared.push((kind, ctx, Ok(PreparedWrite::Rows(request))));
                }
            }
            // A failed source preparation must not fall back to a newer table with the same name.
            if source_preparation_failed {
                prepared.retain(|(kind, _, _)| *kind != 2);
            }
            let sample_batches = prepared.iter().filter_map(|(kind, ctx, request)| {
                if *kind == 0
                    && let Ok(PreparedWrite::Rows(request)) = request
                {
                    Some((ctx, request))
                } else {
                    None
                }
            });
            let batcher = if metric_engine
                && let Some(batcher) = batcher.as_ref()
                && batcher.accepts_bulk_destinations(sample_batches).await
            {
                Some(batcher)
            } else {
                None
            };
            let mut outputs = Vec::with_capacity(prepared.len());
            // Serialize schema preparation across batches that resolve to the same table.
            for (kind, ctx, request) in prepared {
                let mut deferred = false;
                let output = match request {
                    Err(error) => Err(error),
                    Ok(PreparedWrite::Metric(request)) => {
                        handler.write_prepared_metric(request, ctx).await
                    }
                    Ok(PreparedWrite::Rows(request)) => {
                        if kind == 2 && !metric_engine {
                            error::NotSupportedSnafu {
                                feat: "exemplars require metric-engine ingestion",
                            }
                            .fail()
                        } else if kind == 2 {
                            handler
                                .write_exemplars_with_sources(request, ctx, &sources)
                                .await
                        } else if kind == 0
                            && let Some(batcher) = batcher
                        {
                            deferred = !batcher.is_synchronous();
                            batcher
                                .submit_output_with(request, ctx, |_| ready(Ok(())))
                                .await
                                .map(|(output, ())| output)
                        } else {
                            handler.write_prepared(request, ctx, metric_engine).await
                        }
                    }
                };
                outputs.push((kind, output, deferred));
            }
            let mut outcome = Outcome::default();
            let mut pending_samples = Vec::new();
            for (kind, output, deferred) in outputs {
                match output {
                    Ok(output) => {
                        outcome.cost += output.meta.cost;
                        if deferred && !output.meta.write_completions.is_empty() {
                            pending_samples.push(output);
                            continue;
                        }
                        let progress = output.write_progress().await;
                        outcome.written[kind] += progress.affected_rows as u64;
                        if let Err(error) =
                            progress.into_result().context(error::ExecuteGrpcQuerySnafu)
                        {
                            outcome.error.get_or_insert(error);
                        }
                    }
                    Err(error) => {
                        outcome.error.get_or_insert(error);
                    }
                }
            }
            if outcome.error.is_none() {
                outcome.accepted = !pending_samples.is_empty();
                outcome.written[0] += pending_samples
                    .iter()
                    .map(|output| output.extract_rows_and_cost().0 as u64)
                    .sum::<u64>();
            } else {
                // Split submissions may share a counter; each completion owns its row count.
                for output in pending_samples {
                    outcome.written[0] += output.write_progress().await.affected_rows as u64;
                }
            }
            outcome
        }
        Err(error) => Outcome {
            error: Some(error),
            ..Default::default()
        },
    }
}

async fn prepare(
    handler: &PromStoreProtocolHandlerRef,
    samples: Vec<PromWriteBatch>,
    histograms: Vec<PromWriteBatch>,
    exemplars: ExemplarRequests,
    ctx: QueryContextRef,
) -> Result<Vec<(usize, QueryContextRef, RowInsertRequests)>> {
    let mut kinds = Vec::new();
    let mut prepared = Vec::new();
    for (kind, batches) in [samples, histograms].into_iter().enumerate() {
        for (ctx, request) in batches {
            handler.pre_write(&request, ctx.clone()).await?;
            kinds.push(kind);
            // Keep per-batch routing options when admitting database-wide totals.
            prepared.push((Arc::new(ctx.with_confirmed_writes()), request));
        }
    }
    for (schema, exemplars) in exemplars {
        let target = ctx.fork();
        if let Some(schema) = schema {
            target.set_current_schema(&schema);
        }
        let (request, sources) = into_rows(exemplars)?;
        let target = Arc::new(target);
        handler.pre_write(&request, target.clone()).await?;
        let target = Arc::new(target.with_confirmed_writes());
        if !sources.is_empty() {
            handler
                .check_exemplar_sources(&sources, target.clone())
                .await?;
        }
        kinds.push(2);
        prepared.push((target, request));
    }
    operator::insert::admit_row_insert_batches(&mut prepared)
        .await
        .map_err(BoxedError::new)
        .context(error::ExecuteGrpcQuerySnafu)?;
    Ok(kinds
        .into_iter()
        .zip(prepared)
        .map(|(kind, (ctx, request))| (kind, ctx, request))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use api::prom_store::remote::ReadRequest;
    use api::v1::{Row, RowInsertRequest, Rows};
    use async_trait::async_trait;
    use common_query::{Output, OutputData, OutputMeta, WriteCompletion, WriteProgress};
    use session::context::QueryContext;
    use tokio::sync::Notify;

    use super::*;
    use crate::exemplar::Exemplar;
    use crate::query_handler::{PromStoreProtocolHandler, PromStoreResponse};

    #[derive(Default)]
    struct Handler {
        events: Mutex<Vec<String>>,
        exemplar_done: Arc<Notify>,
        deny_schema: bool,
        fail_preparation: bool,
        successful_writes: bool,
        check_serial: bool,
        writing: AtomicBool,
    }

    #[async_trait]
    impl PromStoreProtocolHandler for Handler {
        async fn prepare_metric_write(
            &self,
            _: RowInsertRequests,
            _: QueryContextRef,
        ) -> Result<PreparedMetricInserts> {
            if self.fail_preparation {
                return error::InvalidPromRemoteRequestSnafu {
                    msg: "source preparation failed",
                }
                .fail();
            }
            Ok(PreparedMetricInserts::default())
        }

        async fn write_prepared_metric(
            &self,
            _: PreparedMetricInserts,
            ctx: QueryContextRef,
        ) -> Result<Output> {
            self.write_prepared(RowInsertRequests::default(), ctx, true)
                .await
        }

        async fn pre_write(&self, request: &RowInsertRequests, ctx: QueryContextRef) -> Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(format!("pre:{}", ctx.current_schema()));
            assert!(!request.inserts.is_empty());
            if self.deny_schema && ctx.current_schema() == "other" {
                return error::InvalidPromRemoteRequestSnafu {
                    msg: "denied destination",
                }
                .fail();
            }
            if let Some(schema) = ctx.extension("resolved_schema") {
                ctx.set_current_schema(schema);
            }
            Ok(())
        }

        async fn check_exemplar_sources(
            &self,
            sources: &BTreeSet<String>,
            ctx: QueryContextRef,
        ) -> Result<()> {
            assert_eq!(sources, &BTreeSet::from(["metric".into()]));
            self.events
                .lock()
                .unwrap()
                .push(format!("source:{}", ctx.current_schema()));
            Ok(())
        }

        async fn write_prepared(
            &self,
            _request: RowInsertRequests,
            ctx: QueryContextRef,
            _: bool,
        ) -> Result<Output> {
            if self.check_serial {
                assert!(!self.writing.swap(true, Ordering::SeqCst));
                tokio::task::yield_now().await;
                self.writing.store(false, Ordering::SeqCst);
            }
            assert!(ctx.confirmed_writes());
            assert_eq!(
                ctx.write_rows_to_admit(ctx.current_catalog(), &ctx.current_schema(), 3),
                0
            );
            self.events.lock().unwrap().push("metric".into());
            if self.successful_writes {
                if let Some(counter) = ctx.write_progress() {
                    counter.add_written(2);
                }
                return Ok(Output::new_with_affected_rows(2));
            }
            let done = self.exemplar_done.clone();
            let counter = ctx.write_progress().cloned();
            let completion = WriteCompletion::with_progress(async move {
                done.notified().await;
                if let Some(counter) = counter {
                    counter.add_written(1);
                }
                WriteProgress {
                    affected_rows: 1,
                    error: Some(Arc::new(BoxedError::new(
                        error::InternalSnafu {
                            err_msg: "late metric failure",
                        }
                        .build(),
                    ))),
                }
            });
            Ok(Output::new(
                OutputData::AffectedRows(2),
                OutputMeta {
                    cost: 2,
                    write_completions: vec![completion],
                    ..Default::default()
                },
            ))
        }

        async fn write_exemplars_prepared(
            &self,
            _: RowInsertRequests,
            ctx: QueryContextRef,
        ) -> Result<Output> {
            assert!(ctx.confirmed_writes());
            assert_eq!(
                ctx.write_rows_to_admit(ctx.current_catalog(), &ctx.current_schema(), 1),
                0
            );
            self.events.lock().unwrap().push("exemplar".into());
            if let Some(counter) = ctx.write_progress() {
                counter.add_written(1);
            }
            self.exemplar_done.notify_one();
            Ok(Output::new(
                OutputData::AffectedRows(1),
                OutputMeta::new_with_cost(1),
            ))
        }

        async fn write(&self, _: RowInsertRequests, _: QueryContextRef, _: bool) -> Result<Output> {
            unreachable!()
        }
        async fn write_all(
            &self,
            _: Vec<(QueryContextRef, RowInsertRequests)>,
            _: bool,
        ) -> Result<Vec<Result<Output>>> {
            unreachable!()
        }
        async fn read(&self, _: ReadRequest, _: QueryContextRef) -> Result<PromStoreResponse> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn exemplar_requires_successful_metric_engine_source_preparation() {
        for metric_engine in [false, true] {
            let handler = Arc::new(Handler {
                fail_preparation: true,
                ..Default::default()
            });
            let ctx = QueryContext::arc();
            let samples = if metric_engine {
                vec![(
                    ctx.clone(),
                    RowInsertRequests {
                        inserts: vec![RowInsertRequest {
                            table_name: "metric".into(),
                            rows: Some(Rows {
                                schema: vec![],
                                rows: vec![Row { values: vec![] }],
                            }),
                        }],
                    },
                )]
            } else {
                vec![]
            };
            let exemplars = ExemplarRequests::from([(
                None,
                vec![Exemplar {
                    metric: "metric".into(),
                    series_labels: BTreeMap::new(),
                    labels: BTreeMap::new(),
                    timestamp: 1,
                    value: 1.0,
                }],
            )]);
            let outcome = write(
                handler.clone(),
                None,
                metric_engine,
                samples,
                vec![],
                exemplars,
                ctx,
            )
            .await;
            assert!(outcome.error.is_some());
            assert_eq!(outcome.written, [0, 0, 0]);
            assert!(
                !handler
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|e| e == "metric" || e == "exemplar")
            );
        }
    }

    #[tokio::test]
    async fn exemplar_preserves_batch_routing_after_preflight() {
        use crate::http::prom_store::PHYSICAL_TABLE_PARAM;

        let handler: PromStoreProtocolHandlerRef = Arc::new(Handler::default());
        let mut ctx = QueryContext::with("greptime", "public");
        ctx.set_extension("resolved_schema", "tenant");
        let ctx = Arc::new(ctx);
        let batch = |physical: Option<&str>| {
            let mut target = ctx.fork();
            if let Some(physical) = physical {
                target.set_extension(PHYSICAL_TABLE_PARAM, physical);
            }
            (
                Arc::new(target),
                RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: "metric".into(),
                        rows: Some(Rows {
                            schema: vec![],
                            rows: vec![Row { values: vec![] }],
                        }),
                    }],
                },
            )
        };
        let exemplars = ExemplarRequests::from([(
            None,
            vec![Exemplar {
                metric: "metric".into(),
                series_labels: BTreeMap::new(),
                labels: BTreeMap::new(),
                timestamp: 1,
                value: 1.0,
            }],
        )]);
        let writes = prepare(
            &handler,
            vec![
                batch(Some("physical_a")),
                batch(Some("physical_b")),
                batch(None),
            ],
            vec![batch(Some("histogram_physical"))],
            exemplars,
            ctx,
        )
        .await
        .unwrap();
        let expected = [
            (0, Some("physical_a")),
            (0, Some("physical_b")),
            (0, None),
            (1, Some("histogram_physical")),
            (2, None),
        ];
        assert_eq!(writes.len(), expected.len());
        for ((kind, ctx, request), (expected_kind, physical)) in writes.into_iter().zip(expected) {
            assert_eq!(kind, expected_kind);
            assert_eq!(ctx.extension(PHYSICAL_TABLE_PARAM), physical);
            assert_eq!(ctx.current_schema(), "tenant");
            assert!(ctx.confirmed_writes());
            assert_eq!(ctx.write_rows_to_admit("greptime", "tenant", 1), 0);
            assert_eq!(request.inserts.len(), 1);
        }
    }

    #[tokio::test]
    async fn exemplar_submits_metric_batches_sequentially() {
        let handler = Arc::new(Handler {
            successful_writes: true,
            check_serial: true,
            ..Default::default()
        });
        let ctx = QueryContext::arc();
        let samples = ["metric", "other", "other"]
            .into_iter()
            .map(|name| {
                (
                    Arc::new(ctx.fork()),
                    RowInsertRequests {
                        inserts: vec![RowInsertRequest {
                            table_name: name.into(),
                            rows: Some(Rows {
                                schema: vec![],
                                rows: vec![Row { values: vec![] }; 2],
                            }),
                        }],
                    },
                )
            })
            .collect();
        let exemplars = ExemplarRequests::from([(
            None,
            vec![Exemplar {
                metric: "metric".into(),
                series_labels: BTreeMap::new(),
                labels: BTreeMap::new(),
                timestamp: 1,
                value: 1.0,
            }],
        )]);
        let outcome = write(handler, None, true, samples, vec![], exemplars, ctx).await;
        assert!(outcome.error.is_none());
        assert_eq!(outcome.written, [6, 0, 1]);
    }

    #[tokio::test]
    async fn exemplar_v2_batching_acknowledgement_and_partial_counts() {
        use api::greptime_proto::io::prometheus::write::v2;
        use common_memory_manager::OnExhaustedPolicy;
        use prost::Message;

        use crate::batcher::logical_table::test_util::{WorkerCommand, submission_batcher};
        use crate::http::extractor::PipelineInfo;
        use crate::http::prom_store::{PromStoreState, RemoteWriteQuery, remote_write_v2};
        use crate::prom_remote_write::REMOTE_WRITE_V2_VERSION;
        use crate::prom_remote_write::validation::PromValidationMode;
        use crate::request_memory_limiter::ServerMemoryLimiter;

        for (sync, successful_writes) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let (batcher, mut receiver) = submission_batcher(sync).await;
            let limiter = ServerMemoryLimiter::new(1024 * 1024, OnExhaustedPolicy::Fail);
            let db = "exemplar_confirmed_counts";
            let counters = [
                &*crate::metrics::PROM_STORE_REMOTE_WRITE_SAMPLES,
                &*crate::metrics::PROM_STORE_REMOTE_WRITE_EXEMPLARS,
            ]
            .map(|counter| counter.with_label_values(&[db, REMOTE_WRITE_V2_VERSION]));
            let before = counters.each_ref().map(|counter| counter.get());
            let request = v2::Request {
                symbols: vec![
                    String::new(),
                    "__name__".into(),
                    "metric".into(),
                    "cpu".into(),
                ],
                timeseries: vec![
                    v2::TimeSeries {
                        labels_refs: vec![1, 2],
                        samples: vec![
                            v2::Sample {
                                timestamp: 1,
                                value: 1.0,
                                ..Default::default()
                            };
                            2
                        ],
                        exemplars: vec![v2::Exemplar {
                            labels_refs: vec![],
                            value: 0.25,
                            timestamp: 1,
                        }],
                        ..Default::default()
                    },
                    v2::TimeSeries {
                        labels_refs: vec![1, 3],
                        samples: vec![
                            v2::Sample {
                                timestamp: 1,
                                value: 1.0,
                                ..Default::default()
                            };
                            2
                        ],
                        ..Default::default()
                    },
                ],
            };
            let response = remote_write_v2(
                PromStoreState {
                    prom_store_handler: Arc::new(Handler {
                        successful_writes,
                        ..Default::default()
                    }),
                    pipeline_handler: None,
                    prom_store_with_metric_engine: true,
                    prom_validation_mode: PromValidationMode::Strict,
                    pending_rows_batcher: Some(batcher),
                    memory_limiter: limiter.clone(),
                },
                RemoteWriteQuery {
                    db: Some(db.into()),
                    ..Default::default()
                },
                QueryContext::with("greptime", "public"),
                PipelineInfo {
                    pipeline_name: None,
                    pipeline_version: None,
                    pipeline_params: Default::default(),
                },
                false,
                crate::prom_store::snappy_compress(&request.encode_to_vec())
                    .unwrap()
                    .into(),
                0,
            );
            tokio::pin!(response);
            let mut early_response = None;
            let command = tokio::select! {
                biased;
                result = &mut response => {
                    early_response = Some(result.unwrap());
                    receiver.try_recv().unwrap()
                }
                command = receiver.recv() => command.unwrap(),
            };
            assert_eq!(early_response.is_some(), !sync && successful_writes);
            assert!(limiter.used_bytes() > 0);
            let WorkerCommand::Submit {
                ctx,
                response_tx,
                _completion,
                ..
            } = command
            else {
                panic!("expected sample submission")
            };
            let affected_rows = if successful_writes { 2 } else { 1 };
            ctx.write_progress().unwrap().add_written(affected_rows);
            let progress = WriteProgress {
                affected_rows,
                error: (!successful_writes).then(|| {
                    Arc::new(BoxedError::new(
                        error::InternalSnafu {
                            err_msg: "partial queued write",
                        }
                        .build(),
                    ))
                }),
            };
            assert_eq!(
                response_tx.send(progress).is_ok(),
                sync || !successful_writes
            );
            drop(_completion);
            let response = match early_response {
                Some(response) => response,
                None => response.await.unwrap(),
            };
            if successful_writes {
                assert_eq!(response.status(), if sync { 204 } else { 202 });
            } else {
                assert!(response.status().is_server_error());
            }
            let samples = if successful_writes { "4" } else { "2" };
            for (kind, count) in [
                ("samples", samples),
                ("histograms", "0"),
                ("exemplars", "1"),
            ] {
                assert_eq!(
                    response.headers()[format!("x-prometheus-remote-write-{kind}-written")],
                    count
                );
            }
            for ((counter, before), count) in counters
                .into_iter()
                .zip(before)
                .zip([if successful_writes { 4 } else { 2 }, 1])
            {
                assert_eq!(counter.get() - before, count);
            }
            drop(ctx);
            assert_eq!(limiter.used_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn exemplar_preflights_all_databases_and_retains_delayed_partial_progress() {
        for denied in [false, true] {
            let handler = Arc::new(Handler {
                deny_schema: denied,
                ..Default::default()
            });
            let mut exemplars = ExemplarRequests::new();
            for schema in if denied {
                vec![None, Some("other".into())]
            } else {
                vec![None]
            } {
                exemplars.insert(
                    schema,
                    vec![Exemplar {
                        metric: "metric".into(),
                        series_labels: BTreeMap::new(),
                        labels: BTreeMap::new(),
                        timestamp: 1,
                        value: 1.0,
                    }],
                );
            }
            let ctx = QueryContext::arc();
            let samples = vec![(
                ctx.clone(),
                RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: "metric".into(),
                        rows: Some(Rows {
                            schema: vec![],
                            rows: vec![Row { values: vec![] }; 2],
                        }),
                    }],
                },
            )];
            let outcome = write(handler.clone(), None, true, samples, vec![], exemplars, ctx).await;
            assert!(outcome.error.is_some());
            let events = handler.events.lock().unwrap();
            if denied {
                assert_eq!(outcome.written, [0, 0, 0]);
                assert!(!events.iter().any(|e| e == "metric" || e == "exemplar"));
            } else {
                assert_eq!(outcome.written, [1, 0, 1]);
                assert_eq!(outcome.cost, 3);
                assert_eq!(
                    *events,
                    [
                        "pre:public",
                        "pre:public",
                        "source:public",
                        "metric",
                        "exemplar"
                    ]
                );
            }
        }
    }
}
