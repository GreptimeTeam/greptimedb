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

use api::v1::RowInsertRequests;
use common_error::ext::BoxedError;
use futures::future::{join_all, ready};
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
    pub written: [u64; 3],
    pub error: Option<error::Error>,
}

/// Preflights all destinations together, then preserves confirmed counts by input kind.
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
            let outputs = join_all(writes.into_iter().map(|(kind, ctx, request)| {
                let handler = &handler;
                let batcher = &batcher;
                async move {
                    let batch = (ctx, request);
                    let output = if kind == 2 {
                        handler.write_exemplars_prepared(batch.1, batch.0).await
                    } else if metric_engine
                        && let Some(batcher) = batcher
                        && batcher
                            .accepts_bulk_destinations(std::iter::once(&batch))
                            .await
                    {
                        batcher
                            .submit_output_with(batch.1, batch.0, |_| ready(Ok(())))
                            .await
                            .map(|(output, ())| output)
                    } else {
                        handler
                            .write_prepared(batch.1, batch.0, metric_engine)
                            .await
                    };
                    (kind, output)
                }
            }))
            .await;
            let mut outcome = Outcome::default();
            for (kind, output) in outputs {
                match output {
                    Ok(output) => {
                        outcome.cost += output.meta.cost;
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
    }

    #[async_trait]
    impl PromStoreProtocolHandler for Handler {
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
            assert!(ctx.confirmed_writes());
            assert_eq!(
                ctx.write_rows_to_admit(ctx.current_catalog(), &ctx.current_schema(), 3),
                0
            );
            self.events.lock().unwrap().push("metric".into());
            let done = self.exemplar_done.clone();
            let completion = WriteCompletion::with_progress(async move {
                done.notified().await;
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
    async fn exemplar_v2_error_headers_retain_confirmed_counts() {
        use api::greptime_proto::io::prometheus::write::v2;
        use prost::Message;

        use crate::http::extractor::PipelineInfo;
        use crate::http::prom_store::{PromStoreState, RemoteWriteQuery, remote_write_v2};
        use crate::prom_remote_write::validation::PromValidationMode;

        let request = v2::Request {
            symbols: vec![String::new(), "__name__".into(), "metric".into()],
            timeseries: vec![v2::TimeSeries {
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
            }],
        };
        let response = remote_write_v2(
            PromStoreState {
                prom_store_handler: Arc::new(Handler::default()),
                pipeline_handler: None,
                prom_store_with_metric_engine: false,
                prom_validation_mode: PromValidationMode::Strict,
                pending_rows_batcher: None,
                memory_limiter: Default::default(),
            },
            RemoteWriteQuery::default(),
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
        )
        .await
        .unwrap();
        assert!(response.status().is_server_error());
        for (kind, count) in [("samples", "1"), ("histograms", "0"), ("exemplars", "1")] {
            assert_eq!(
                response.headers()[format!("x-prometheus-remote-write-{kind}-written")],
                count
            );
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
            let outcome = write(
                handler.clone(),
                None,
                false,
                samples,
                vec![],
                exemplars,
                ctx,
            )
            .await;
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
