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

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use TimeUnit as ArrowTimeUnit;
    use api::region::RegionResponse;
    use api::v1::region::{RegionRequest, region_request};
    use client::{DEFAULT_CATALOG_NAME, OutputData};
    use common_meta::cache::LayeredCacheRegistryBuilder;
    use common_meta::node_manager::{
        Datanode, DatanodeManager, DatanodeRef, FlownodeManager, FlownodeRef, NodeManagerRef,
    };
    use common_meta::peer::Peer;
    use common_query::request::QueryRequest;
    use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
    use datatypes::arrow::array::AsArray;
    use datatypes::arrow::datatypes::{
        DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType,
    };
    use frontend::instance::Instance;
    use frontend::instance::builder::FrontendBuilder;
    use frontend::service_config::BatcherOptions;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_rust::proto::opentelemetry::common::v1::any_value::Value as Val;
    use otel_arrow_rust::proto::opentelemetry::common::v1::{
        AnyValue, InstrumentationScope, KeyValue,
    };
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::number_data_point::Value;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, DataPointFlags, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric,
    };
    use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;
    use pipeline::{GreptimePipelineParams, PipelineWay};
    use serde_json::json;
    use servers::batcher::BatchingProtocol;
    use servers::otlp::trace::{TraceAuxData, span};
    use servers::query_handler::OpenTelemetryProtocolHandler;
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::QueryContext;
    use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};
    use store_api::region_engine::RegionEngine;
    use store_api::storage::RegionId;
    use tokio::sync::oneshot;

    use crate::standalone::{GreptimeDbStandalone, GreptimeDbStandaloneBuilder};
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_trace_aux_cache() {
        check_trace_aux_cache(false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_trace_aux_cache_with_batching_v0_v1() {
        check_trace_aux_cache(true).await;
    }

    #[test]
    fn test_trace_aux_async_completion() {
        temp_env::with_var("PENDING_ROWS_BATCH_SYNC", Some("false"), || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(check_trace_aux_async_completion());
        });
    }

    #[test]
    fn test_trace_aux_admission() {
        temp_env::with_var("PENDING_ROWS_BATCH_SYNC", Some("false"), || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(60), check_trace_aux_admission())
                        .await
                        .expect("auxiliary admission must make progress");
                });
        });
    }

    struct TraceAuxWriteGate {
        region_id: RegionId,
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<bool>,
    }

    struct GatedTraceNodeManager {
        inner: NodeManagerRef,
        gate: Arc<Mutex<Option<TraceAuxWriteGate>>>,
    }

    #[async_trait::async_trait]
    impl DatanodeManager for GatedTraceNodeManager {
        async fn datanode(&self, peer: &Peer) -> DatanodeRef {
            Arc::new(GatedTraceDatanode {
                inner: self.inner.datanode(peer).await,
                gate: self.gate.clone(),
            })
        }
    }

    #[async_trait::async_trait]
    impl FlownodeManager for GatedTraceNodeManager {
        async fn flownode(&self, peer: &Peer) -> FlownodeRef {
            self.inner.flownode(peer).await
        }
    }

    struct GatedTraceDatanode {
        inner: DatanodeRef,
        gate: Arc<Mutex<Option<TraceAuxWriteGate>>>,
    }

    #[async_trait::async_trait]
    impl Datanode for GatedTraceDatanode {
        async fn handle_request(
            &self,
            request: Arc<RegionRequest>,
        ) -> common_meta::error::Result<RegionResponse> {
            let gate = {
                let mut gate = self.gate.lock().unwrap();
                if gate.as_ref().is_some_and(|gate| match &request.body {
                    Some(region_request::Body::Inserts(inserts)) => inserts
                        .requests
                        .iter()
                        .any(|insert| insert.region_id == gate.region_id.as_u64()),
                    Some(region_request::Body::BulkInsert(insert)) => {
                        insert.region_id == gate.region_id.as_u64()
                    }
                    _ => false,
                }) {
                    gate.take()
                } else {
                    None
                }
            };
            if let Some(gate) = gate {
                gate.started.send(()).unwrap();
                if !gate.release.await.unwrap() {
                    return common_meta::error::UnexpectedSnafu {
                        err_msg: "injected auxiliary write failure",
                    }
                    .fail();
                }
            }
            self.inner.handle(request).await
        }

        async fn handle_query(
            &self,
            request: QueryRequest,
        ) -> common_meta::error::Result<SendableRecordBatchStream> {
            self.inner.handle_query(request).await
        }
    }

    async fn check_trace_aux_admission() {
        let standalone = GreptimeDbStandaloneBuilder::new("trace_aux_admission")
            .with_table_batcher(BatcherOptions {
                protocols: vec![BatchingProtocol::Otlp],
                pending_rows_flush_interval: Duration::from_secs(3600),
                max_batch_rows: 1,
                max_inflight_requests: 1,
                ..Default::default()
            })
            .build()
            .await;
        let source = standalone.fe_instance();
        let gate = Arc::new(Mutex::new(None));
        let cache_registry = Arc::new(
            cache::with_default_composite_cache_registry(
                LayeredCacheRegistryBuilder::default().add_cache_registry(
                    cache::build_fundamental_cache_registry(standalone.kv_backend.clone()),
                ),
            )
            .unwrap()
            .build(),
        );
        let mut options = standalone.opts.frontend_options();
        options.otlp.trace_ingest_chunk_size = 1;
        let instance = Arc::new(
            FrontendBuilder::new(
                options,
                standalone.kv_backend.clone(),
                cache_registry,
                source.catalog_manager().clone(),
                Arc::new(GatedTraceNodeManager {
                    inner: source.node_manager().clone(),
                    gate: gate.clone(),
                }),
                source.procedure_executor().clone(),
                Arc::new(catalog::process_manager::ProcessManager::new(
                    "trace_aux_admission".into(),
                    None,
                )),
            )
            .try_build()
            .await
            .unwrap(),
        );

        for version in 0..=1 {
            let table_name = format!("trace_aux_admission_v{version}");
            let mut context = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
            context.set_extension(
                common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
                &table_name,
            );
            context.set_extension(table::requests::TRACE_TABLE_PARTITIONS_HINT_KEY, "1");
            context.set_batching_enabled(true);
            let ctx = Arc::new(context);
            let write = async |request| {
                instance
                    .traces(
                        instance.clone(),
                        request,
                        if version == 0 {
                            PipelineWay::OtlpTraceDirectV0
                        } else {
                            PipelineWay::OtlpTraceDirectV1
                        },
                        GreptimePipelineParams::default(),
                        table_name.clone(),
                        ctx.clone(),
                    )
                    .await
                    .unwrap()
            };
            let cached = |request: ExportTraceServiceRequest| {
                let mut data = TraceAuxData::default();
                for span in span::parse(request).iter().flat_map(|group| &group.spans) {
                    data.observe_span(span);
                }
                instance.trace_aux_data_cached_for_test(&table_name, data, &ctx)
            };
            let warm = trace_aux_request();
            assert_eq!(1, write(warm.clone()).await.accepted_spans);
            while !cached(warm.clone()) {
                tokio::task::yield_now().await;
            }
            let table = instance
                .catalog_manager()
                .table(
                    DEFAULT_CATALOG_NAME,
                    "public",
                    &format!("{table_name}_operations"),
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            let route = instance
                .partition_manager()
                .find_physical_table_route(table.table_info().table_id())
                .await
                .unwrap();
            let region_id = route.region_routes[0].region.id;

            for success in [true, false] {
                let mut cold = warm.clone();
                let spans = &mut cold.resource_spans[0].scope_spans[0].spans;
                spans[0].name = format!("cold_{success}");
                spans.push(spans[0].clone());
                let (started_tx, started_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                *gate.lock().unwrap() = Some(TraceAuxWriteGate {
                    region_id,
                    started: started_tx,
                    release: release_rx,
                });

                // Two chunks must progress with just one main and one auxiliary slot.
                assert_eq!(2, write(cold.clone()).await.accepted_spans);
                started_rx.await.unwrap();
                assert!(
                    !cached(cold.clone()),
                    "pending writes must not warm the cache"
                );
                let before = trace_aux_sequences(&standalone, &table_name).await;
                let mut waiting = Box::pin(write(cold.clone()));
                assert!(futures::poll!(waiting.as_mut()).is_pending());

                // Main admission is free while auxiliary storage is blocked. Warm
                // requests must bypass auxiliary admission and keep making progress.
                assert_eq!(1, write(warm.clone()).await.accepted_spans);
                assert!(futures::poll!(waiting.as_mut()).is_pending());
                release_tx.send(success).unwrap();
                assert_eq!(2, waiting.await.accepted_spans);
                if !success {
                    // The failed write cannot satisfy the waiting request's miss.
                    while !cached(cold.clone()) {
                        tokio::task::yield_now().await;
                    }
                }
                let after = trace_aux_sequences(&standalone, &table_name).await;
                assert_eq!(before[1], after[1], "cached service must not be rewritten");
                assert_eq!(
                    before[2] + 1,
                    after[2],
                    "exactly one successful operation write"
                );
            }
        }
    }

    fn trace_aux_request() -> ExportTraceServiceRequest {
        serde_json::from_value(json!({
            "resourceSpans": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "svc"}}
                ]},
                "scopeSpans": [{"spans": [{
                    "traceId": "c05d7a4ec8e1f231f02ed6e8da8655b4",
                    "spanId": "9630f2916e2f7909",
                    "name": "operation",
                    "kind": 2,
                    "startTimeUnixNano": "1736480942444376000",
                    "endTimeUnixNano": "1736480942444499000"
                }]}]
            }]
        }))
        .unwrap()
    }

    async fn trace_aux_sequences(standalone: &GreptimeDbStandalone, table_name: &str) -> Vec<u64> {
        let instance = standalone.fe_instance();
        let mut sequences = Vec::new();
        for suffix in ["", "_services", "_operations"] {
            let table = instance
                .catalog_manager()
                .table(
                    DEFAULT_CATALOG_NAME,
                    "public",
                    &format!("{table_name}{suffix}"),
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            let route = instance
                .partition_manager()
                .find_physical_table_route(table.table_info().table_id())
                .await
                .unwrap();
            assert_eq!(route.region_routes.len(), 1);
            sequences.push(
                standalone
                    .mito_engine
                    .get_committed_sequence(route.region_routes[0].region.id)
                    .await
                    .unwrap(),
            );
        }
        sequences
    }

    async fn check_trace_aux_async_completion() {
        let standalone = GreptimeDbStandaloneBuilder::new("trace_aux_async_completion")
            .with_table_batcher(BatcherOptions {
                protocols: vec![BatchingProtocol::Otlp],
                pending_rows_flush_interval: Duration::from_secs(3600),
                max_batch_rows: 2,
                ..Default::default()
            })
            .build()
            .await;
        let instance = standalone.fe_instance();
        // V2 bulk ingestion has a separate Variant/Struct conversion failure.
        for version in 0..=1 {
            let table_name = format!("trace_aux_async_v{version}");
            let mut context = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
            context.set_extension(
                common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
                &table_name,
            );
            context.set_extension(table::requests::TRACE_TABLE_PARTITIONS_HINT_KEY, "1");
            context.set_batching_enabled(true);
            let ctx = Arc::new(context);
            let request = trace_aux_request();
            let write = async |request, ctx| {
                let outcome = tokio::time::timeout(
                    Duration::from_secs(30),
                    instance.traces(
                        instance.clone(),
                        request,
                        if version == 0 {
                            PipelineWay::OtlpTraceDirectV0
                        } else {
                            PipelineWay::OtlpTraceDirectV1
                        },
                        GreptimePipelineParams::default(),
                        table_name.clone(),
                        ctx,
                    ),
                )
                .await
                .expect("async trace ingestion must return before the batch flush")
                .unwrap();
                assert_eq!((outcome.accepted_spans, outcome.rejected_spans), (1, 0));
                assert!(outcome.error_message.is_none());
            };
            write(request.clone(), ctx.clone()).await;
            assert_trace_v2_query(
                instance,
                &format!("SELECT COUNT(*) = 0 FROM {table_name}"),
                ctx.clone(),
            )
            .await
            .unwrap();
            for suffix in ["_services", "_operations"] {
                assert!(
                    instance
                        .catalog_manager()
                        .table(
                            DEFAULT_CATALOG_NAME,
                            "public",
                            &format!("{table_name}{suffix}"),
                            None
                        )
                        .await
                        .unwrap()
                        .is_none()
                );
            }

            // The second submission deterministically triggers the main flush.
            // Omitting the service avoids a second concurrent auxiliary write.
            let mut flush_request = request.clone();
            flush_request.resource_spans[0]
                .resource
                .as_mut()
                .unwrap()
                .attributes
                .clear();
            write(flush_request, ctx.clone()).await;
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let mut aux_data = TraceAuxData::default();
                    for span in span::parse(request.clone())
                        .into_iter()
                        .flat_map(|group| group.spans)
                    {
                        aux_data.observe_span(&span);
                    }
                    if instance.trace_aux_data_cached_for_test(&table_name, aux_data, &ctx) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("deferred auxiliary writes must populate the cache");
            for suffix in ["_services", "_operations"] {
                assert_trace_v2_query(
                    instance,
                    &format!("SELECT COUNT(*) = 1 FROM {table_name}{suffix}"),
                    ctx.clone(),
                )
                .await
                .unwrap();
            }
            assert_trace_v2_query(
                instance,
                &format!("SELECT COUNT(*) > 0 FROM {table_name}"),
                ctx.clone(),
            )
            .await
            .unwrap();

            let before = trace_aux_sequences(&standalone, &table_name).await;
            // A direct warm request finishes before we compare storage sequences.
            let mut warm_ctx = ctx.fork();
            warm_ctx.set_batching_enabled(false);
            write(request, Arc::new(warm_ctx)).await;
            let after = trace_aux_sequences(&standalone, &table_name).await;
            assert!(after[0] > before[0]);
            assert_eq!(after[1..], before[1..], "v{version}");
        }
    }

    async fn check_trace_aux_cache(batching: bool) {
        let standalone = GreptimeDbStandaloneBuilder::new("trace_aux_cache")
            .with_table_batcher(if batching {
                BatcherOptions {
                    protocols: vec![BatchingProtocol::Otlp],
                    // Two main spans trigger a flush, while each auxiliary table
                    // has only one row and would wait an hour if batched.
                    pending_rows_flush_interval: Duration::from_secs(3600),
                    max_batch_rows: 2,
                    ..Default::default()
                }
            } else {
                BatcherOptions::default()
            })
            .build()
            .await;
        let instance = standalone.fe_instance();
        let cloned_instance = Arc::new(instance.as_ref().clone());

        // V2 bulk writes currently fail on Variant/Struct schema conversion
        // before reaching auxiliary ingestion; cover V2 with direct main writes.
        let max_version = if batching { 1 } else { 2 };
        for version in 0..=max_version {
            let table_name = format!("trace_aux_v{version}");
            let mut context = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
            context.set_extension(
                common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
                &table_name,
            );
            context.set_extension(table::requests::TRACE_TABLE_PARTITIONS_HINT_KEY, "1");
            context.set_batching_enabled(batching);
            let ctx = Arc::new(context);
            let mut request = trace_aux_request();
            if batching {
                let spans = &mut request.resource_spans[0].scope_spans[0].spans;
                spans.push(spans[0].clone());
            }
            let accepted_spans = if batching { 2 } else { 1 };
            let write = async |frontend: &Arc<Instance>, request| {
                tokio::time::timeout(
                    Duration::from_secs(30),
                    frontend.traces(
                        frontend.clone(),
                        request,
                        match version {
                            0 => PipelineWay::OtlpTraceDirectV0,
                            1 => PipelineWay::OtlpTraceDirectV1,
                            _ => PipelineWay::OtlpTraceDirectV2,
                        },
                        GreptimePipelineParams::default(),
                        table_name.clone(),
                        ctx.clone(),
                    ),
                )
                .await
                .expect("auxiliary writes must not wait for the batch flush interval")
                .unwrap()
            };

            // An incompatible auxiliary column fails after the main span is
            // accepted. No candidate key may be cached from the failed batch.
            instance
                .do_query(
                    &format!(
                        "CREATE TABLE {table_name}_operations (\
                         \"timestamp\" TIMESTAMP(9) TIME INDEX, service_name STRING, \
                         span_name STRING, span_kind BIGINT)"
                    ),
                    ctx.clone(),
                )
                .await
                .remove(0)
                .unwrap();
            let failed_aux = write(instance, request.clone()).await;
            assert_eq!(
                (failed_aux.accepted_spans, failed_aux.rejected_spans),
                (accepted_spans, 0)
            );
            assert!(
                failed_aux
                    .error_message
                    .unwrap()
                    .contains("Auxiliary trace tables")
            );

            instance
                .do_query(&format!("DROP TABLE {table_name}_operations"), ctx.clone())
                .await
                .remove(0)
                .unwrap();
            let retry = write(instance, request.clone()).await;
            assert_eq!(
                (retry.accepted_spans, retry.rejected_spans),
                (accepted_spans, 0)
            );
            assert!(retry.error_message.is_none(), "{retry:?}");
            let before = trace_aux_sequences(&standalone, &table_name).await;
            assert!(before.iter().all(|sequence| *sequence > 0));

            // Storage sequences, unlike row counts in an upsert table, prove
            // the writes were skipped. Cloned Instances must share the cache.
            let warm = write(&cloned_instance, request.clone()).await;
            assert_eq!(
                (warm.accepted_spans, warm.rejected_spans),
                (accepted_spans, 0)
            );
            assert!(warm.error_message.is_none(), "{warm:?}");
            let after = trace_aux_sequences(&standalone, &table_name).await;
            assert!(after[0] > before[0]);
            assert_eq!(after[1..], before[1..], "v{version}");

            request.resource_spans[0].scope_spans[0].spans[0].name = "new_operation".into();
            let new_operation = write(instance, request).await;
            assert_eq!(
                (new_operation.accepted_spans, new_operation.rejected_spans),
                (accepted_spans, 0)
            );
            assert!(new_operation.error_message.is_none(), "{new_operation:?}");
            let new_sequences = trace_aux_sequences(&standalone, &table_name).await;
            assert!(new_sequences[0] > after[0]);
            assert_eq!(new_sequences[1], after[1]);
            assert!(new_sequences[2] > after[2]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_trace_v2_on_standalone() -> Result<(), Box<dyn std::error::Error>> {
        let standalone = GreptimeDbStandaloneBuilder::new("trace_v2_standalone")
            .build()
            .await;
        test_trace_v2(standalone.fe_instance()).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_trace_v2_on_distributed() -> Result<(), Box<dyn std::error::Error>> {
        let distributed = tests::create_distributed_instance("trace_v2_distributed").await;
        test_trace_v2(&distributed.frontend()).await?;
        Ok(())
    }

    async fn test_trace_v2(instance: &Arc<Instance>) -> Result<(), Box<dyn std::error::Error>> {
        let mut context = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        context.set_extension(
            common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
            "trace_v2",
        );
        let ctx = Arc::new(context);
        let request: ExportTraceServiceRequest = serde_json::from_value(json!({
            "resourceSpans": [{
                "resource": {"attributes": [
                    {"key": "deployment.environment", "value": {"stringValue": "prod"}},
                    {"key": "cloud.region", "value": {"stringValue": "us-west"}},
                    {"key": "capacity", "value": {"intValue": "8"}}
                ]},
                "scopeSpans": [{
                    "scope": {"attributes": [
                        {"key": "sample.rate", "value": {"doubleValue": 0.5}},
                        {"key": "enabled", "value": {"boolValue": true}},
                        {"key": "config", "value": {"kvlistValue": {"values": [
                            {"key": "batch_size", "value": {"intValue": "4"}}
                        ]}}}
                    ]},
                    "spans": [{
                        "traceId": "c05d7a4ec8e1f231f02ed6e8da8655b4",
                        "spanId": "9630f2916e2f7909", "name": "op", "kind": 2,
                        "startTimeUnixNano": "1736480942444376000",
                        "endTimeUnixNano": "1736480942444499000",
                        "events": [{
                            "timeUnixNano": "1736480942444400000",
                            "name": "cache.hit",
                            "attributes": [{"key": "event.code", "value": {"intValue": "7"}}]
                        }],
                        "links": [{
                            "traceId": "cc9e0991a2e63d274984bd44ee669203",
                            "spanId": "8f847259b0f6e1ab",
                            "traceState": "vendor=value",
                            "attributes": [{"key": "link.type", "value": {"stringValue": "follows_from"}}]
                        }],
                        "attributes": [
                            {"key": "http.status_code", "value": {"intValue": "200"}},
                            {"key": "latency", "value": {"doubleValue": 12.5}},
                            {"key": "empty"},
                            {"key": "bytes", "value": {"bytesValue": "AQID"}},
                            {"key": "nested", "value": {"kvlistValue": {"values": [
                                {"key": "a.b", "value": {"arrayValue": {"values": [{"boolValue": true}, {"stringValue": "ok"}]}}}
                            ]}}}
                        ]
                    }]
                }]
            }]
        }))?;
        let result = instance
            .traces(
                instance.clone(),
                request.clone(),
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await?;
        assert_eq!((result.accepted_spans, result.rejected_spans), (1, 0));

        // Check the complete schema created by the first export, before later writes.
        let output = instance
            .do_query("SHOW CREATE TABLE trace_v2", ctx.clone())
            .await;
        let batches = match output
            .into_iter()
            .next()
            .ok_or("Expected non-empty SQL output")??
            .data
        {
            OutputData::Stream(stream) => RecordBatches::try_collect(stream).await?,
            OutputData::RecordBatches(batches) => batches,
            OutputData::AffectedRows(_) => return Err("Expected SHOW CREATE TABLE rows".into()),
        };
        let expected = r#"CREATE TABLE IF NOT EXISTS "trace_v2" (
  "timestamp" TIMESTAMP(9) NOT NULL,
  "timestamp_end" TIMESTAMP(9) NULL COMPRESSION WITH(encoding = 'delta_binary_packed', type = 'zstd'),
  "duration_nano" BIGINT NULL,
  "parent_span_id" STRING NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '10240', type = 'BLOOM') COMPRESSION WITH(encoding = 'plain', page_size = '64KiB', type = 'zstd'),
  "trace_id" STRING NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '10240', type = 'BLOOM'),
  "span_id" STRING NULL COMPRESSION WITH(encoding = 'plain', page_size = '64KiB', type = 'zstd'),
  "span_kind" STRING NULL,
  "span_name" STRING NULL,
  "span_status_code" STRING NULL,
  "span_status_message" STRING NULL,
  "trace_state" STRING NULL,
  "scope_name" STRING NULL,
  "scope_version" STRING NULL,
  "service_name" STRING NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '10240', type = 'BLOOM'),
  "span_attributes" JSON2(
    max_auto_expanded_paths = 100
  ) NULL,
  "scope_attributes" JSON2(
    max_auto_expanded_paths = 100
  ) NULL,
  "resource_attributes" JSON2(
    max_auto_expanded_paths = 100
  ) NULL,
  "span_events" JSON NULL,
  "span_links" JSON NULL,
  TIME INDEX ("timestamp"),
  PRIMARY KEY ("service_name")
)
PARTITION ON COLUMNS ("trace_id") (
  trace_id < '1',
  trace_id >= '1' AND trace_id < '2',
  trace_id >= '2' AND trace_id < '3',
  trace_id >= '3' AND trace_id < '4',
  trace_id >= '4' AND trace_id < '5',
  trace_id >= '5' AND trace_id < '6',
  trace_id >= '6' AND trace_id < '7',
  trace_id >= '7' AND trace_id < '8',
  trace_id >= '8' AND trace_id < '9',
  trace_id >= '9' AND trace_id < 'a',
  trace_id >= 'a' AND trace_id < 'b',
  trace_id >= 'b' AND trace_id < 'c',
  trace_id >= 'c' AND trace_id < 'd',
  trace_id >= 'd' AND trace_id < 'e',
  trace_id >= 'e' AND trace_id < 'f',
  trace_id >= 'f'
)
ENGINE=mito
WITH(
  'comment' = 'Created on insertion',
  append_mode = 'true',
  'greptime.semantic.entity.service.id' = 'service_name',
  'greptime.semantic.pipeline' = 'greptime_trace_v2',
  'greptime.semantic.signal_type' = 'trace',
  'greptime.semantic.source' = 'opentelemetry',
  'greptime.semantic.trace.conventions' = 'unknown',
  table_data_model = 'greptime_trace_v2'
)"#;
        let batches = batches.take();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(
            batches[0].iter_column_as_string(1).collect::<Vec<_>>(),
            vec![Some(expected.to_string())],
        );

        // First write has no service.name: it must still create the tag, but no auxiliary rows.
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 1
             FROM trace_v2
             WHERE service_name IS NULL",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 19
             FROM information_schema.columns
             WHERE table_name = 'trace_v2'",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 0
             FROM information_schema.tables
             WHERE table_name IN ('trace_v2_services', 'trace_v2_operations')",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            r#"SELECT COUNT(*) = 1
             FROM trace_v2
             WHERE span_attributes."http.status_code"::BIGINT = 200
               AND span_attributes.empty::STRING IS NULL
               AND span_attributes.bytes[1]::BIGINT = 2
               AND span_attributes.nested."a.b"[0]::BOOLEAN = true"#,
            ctx.clone(),
        )
        .await?;

        // Exercise each JSON2 column and expressions combining columns, before and after flush.
        let event_time =
            common_time::Timestamp::new_nanosecond(1736480942444400000).to_iso8601_string();
        let event_query = format!(
            r#"SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(CASE
                 WHEN json_get_string(span_events, '$[0].name') = 'cache.hit'
                   AND json_get_string(span_events, '$[0].time') = '{event_time}'
                   AND json_get_int(span_events, '$[0].attributes."event.code"') = 7
                   AND json_get_string(span_links, '$[0].trace_id') = 'cc9e0991a2e63d274984bd44ee669203'
                   AND json_get_string(span_links, '$[0].span_id') = '8f847259b0f6e1ab'
                   AND json_get_string(span_links, '$[0].trace_state') = 'vendor=value'
                   AND json_get_string(span_links, '$[0].attributes."link.type"') = 'follows_from'
                 THEN 1 END)
             FROM trace_v2"#
        );
        let json_queries = [
            event_query.as_str(),
            r#"SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(CASE
                 WHEN span_attributes.latency::DOUBLE * 2 = 25
                   AND upper(span_attributes.nested."a.b"[1]::STRING) = 'OK'
                   AND span_attributes.missing::STRING IS NULL
                 THEN 1 END)
             FROM trace_v2"#,
            r#"SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(CASE
                 WHEN scope_attributes."sample.rate"::DOUBLE + 0.25 = 0.75
                   AND scope_attributes.enabled::BOOLEAN
                   AND scope_attributes.config.batch_size::BIGINT * 2 = 8
                 THEN 1 END)
             FROM trace_v2"#,
            r#"SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(CASE
                 WHEN upper(resource_attributes."cloud.region"::STRING) = 'US-WEST'
                   AND resource_attributes.capacity::BIGINT - 2 = 6
                 THEN 1 END)
             FROM trace_v2"#,
            r#"SELECT AVG(span_attributes.latency::DOUBLE
                        * scope_attributes."sample.rate"::DOUBLE) = 6.25
                   AND SUM(resource_attributes.capacity::BIGINT)
                       = SUM(scope_attributes.config.batch_size::BIGINT) * 2
             FROM trace_v2"#,
        ];
        for sql in json_queries {
            assert_trace_v2_query(instance, sql, ctx.clone()).await?;
        }

        let mut mixed = request.clone();
        mixed.resource_spans[0]
            .resource
            .as_mut()
            .ok_or("Expected non-empty resource in trace fixture")?
            .attributes[0]
            .key = "service.name".to_string();
        let spans = &mut mixed.resource_spans[0].scope_spans[0].spans;
        // Reject one span in the first chunk, merge its healthy rows, then write the next chunk.
        spans.resize(514, spans[0].clone());
        for (index, span) in spans.iter_mut().enumerate() {
            span.span_id = (index as u64).to_be_bytes().to_vec();
        }
        spans[0].attributes[0]
            .value
            .as_mut()
            .ok_or("Expected non-empty attribute value in trace fixture")?
            .value = Some(
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("ok".to_string()),
        );
        spans[511].end_time_unix_nano = u64::MAX;
        let result = instance
            .traces(
                instance.clone(),
                mixed,
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await?;
        assert_eq!((result.accepted_spans, result.rejected_spans), (513, 1));
        assert!(
            result
                .error_message
                .as_deref()
                .is_some_and(|s| s.contains("Timestamp overflow"))
        );
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 514
             FROM trace_v2",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            r#"SELECT COUNT(*) = 1
             FROM trace_v2
             WHERE span_attributes."http.status_code"::STRING = 'ok'"#,
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 1
             FROM trace_v2_services
             WHERE service_name = 'prod'",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 1
             FROM trace_v2_operations
             WHERE service_name = 'prod'",
            ctx.clone(),
        )
        .await?;

        let mut invalid = request.clone();
        invalid.resource_spans[0].scope_spans[0].spans[0].start_time_unix_nano = u64::MAX;
        let result = instance
            .traces(
                instance.clone(),
                invalid,
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await?;
        assert_eq!((result.accepted_spans, result.rejected_spans), (0, 1));

        // A v1 request must fail before auto-ALTER can flatten attributes into this table.
        let error = instance
            .traces(
                instance.clone(),
                request.clone(),
                PipelineWay::OtlpTraceDirectV1,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await
            .err()
            .ok_or("Expected mixed trace models to be rejected")?;
        let error = format!("{error:?}");
        assert!(
            error.contains("Trace table `trace_v2` uses greptime_trace_v2, but the request uses greptime_trace_v1"),
            "{error}",
        );
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 19
             FROM information_schema.columns
             WHERE table_name = 'trace_v2'",
            ctx.clone(),
        )
        .await?;

        let mut context = (*ctx).clone();
        context.set_extension(
            common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
            "trace_v1",
        );
        let v1_ctx = Arc::new(context);
        // Create the v1 table with an explicit service identity for the isolation check.
        let mut v1_request = request.clone();
        v1_request.resource_spans[0]
            .resource
            .as_mut()
            .ok_or("Expected non-empty resource in trace fixture")?
            .attributes[0]
            .key = "service.name".to_string();
        let result = instance
            .traces(
                instance.clone(),
                v1_request,
                PipelineWay::OtlpTraceDirectV1,
                GreptimePipelineParams::default(),
                "trace_v1".to_string(),
                v1_ctx.clone(),
            )
            .await?;
        assert_eq!(result.accepted_spans, 1);
        let error = instance
            .traces(
                instance.clone(),
                request.clone(),
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v1".to_string(),
                v1_ctx,
            )
            .await
            .err()
            .ok_or("Expected mixed trace models to be rejected")?;
        let error = format!("{error:?}");
        assert!(
            error.contains("Trace table `trace_v1` uses greptime_trace_v1, but the request uses greptime_trace_v2"),
            "{error}",
        );

        let output = instance
            .do_query("ADMIN FLUSH_TABLE('trace_v2')", ctx.clone())
            .await;
        assert!(output.into_iter().all(|r| r.is_ok()));
        assert_trace_v2_query(
            instance,
            r#"SELECT COUNT(*) = 513
             FROM trace_v2
             WHERE span_attributes."http.status_code"::BIGINT = 200"#,
            ctx.clone(),
        )
        .await?;
        for sql in json_queries {
            assert_trace_v2_query(instance, sql, ctx.clone()).await?;
        }
        // Empty events and links are stored as JSON arrays in the same 19-column table.
        let mut empty_request = request;
        let empty_span = &mut empty_request.resource_spans[0].scope_spans[0].spans[0];
        empty_span.events.clear();
        empty_span.links.clear();
        let result = instance
            .traces(
                instance.clone(),
                empty_request,
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await?;
        assert_eq!((result.accepted_spans, result.rejected_spans), (1, 0));
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 1 FROM trace_v2
             WHERE json_to_string(span_events) = '[]' AND json_to_string(span_links) = '[]'",
            ctx.clone(),
        )
        .await?;
        Ok(())
    }

    async fn assert_trace_v2_query(
        instance: &Arc<Instance>,
        sql: &str,
        ctx: Arc<QueryContext>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let output = instance.do_query(sql, ctx).await;
        let OutputData::Stream(stream) = output
            .into_iter()
            .next()
            .ok_or("Expected non-empty SQL output")??
            .data
        else {
            return Err(format!("Expected a query stream: {sql}").into());
        };
        let batches = RecordBatches::try_collect(stream).await?;
        let batches = batches.take();
        assert_eq!(batches.len(), 1, "{sql}");
        assert_eq!(batches[0].num_rows(), 1, "{sql}");
        assert_eq!(
            batches[0].column(0).as_boolean().iter().collect::<Vec<_>>(),
            vec![Some(true)],
            "{sql}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_logical_batcher_non_millisecond_physical_table() {
        // Microsecond and nanosecond physical tables must both use the
        // logical batcher: requests are converted to the physical table's
        // unit during batch alignment
        // (<https://github.com/GreptimeTeam/greptimedb/issues/9342>).
        run_otlp_logical_batcher_non_millisecond_physical_table(
            "us",
            "TIMESTAMP(6)",
            ArrowTimeUnit::Microsecond,
            [60_000_000, 120_000_000],
        )
        .await;
        run_otlp_logical_batcher_non_millisecond_physical_table(
            "ns",
            "TIMESTAMP(9)",
            ArrowTimeUnit::Nanosecond,
            [60_000_000_000, 120_000_000_000],
        )
        .await;
    }

    async fn run_otlp_logical_batcher_non_millisecond_physical_table(
        suite: &str,
        sql_ts_type: &str,
        unit: ArrowTimeUnit,
        expected: [i64; 2],
    ) {
        use std::time::Duration;

        use common_base::Plugins;
        use datatypes::arrow::array::AsArray;
        use datatypes::arrow::datatypes::{TimestampMicrosecondType, TimestampNanosecondType};
        use frontend::server::Services;
        use frontend::service_config::pending_rows_batcher::BatcherOptions;
        use prost::Message;
        use servers::batcher::BatchingProtocol;
        use servers::http::test_helpers::TestClient;
        use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};

        let standalone =
            GreptimeDbStandaloneBuilder::new(&format!("otlp_logical_{suite}_physical"))
                .with_logical_batcher(BatcherOptions {
                    protocols: vec![BatchingProtocol::Otlp],
                    pending_rows_flush_interval: Duration::from_millis(5),
                    ..Default::default()
                })
                .build()
                .await;
        let instance = standalone.fe_instance();
        let options = standalone.opts.clone();
        let services = Services::new(options.clone(), instance.clone(), Plugins::default());
        let server = services
            .http_server_builder(
                &options.frontend_options(),
                services.server_memory_limiter.clone(),
            )
            .build();
        let client = TestClient::new(server.build(server.make_app()).unwrap()).await;

        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        ctx.set_logical_batching_enabled(true);
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        }));
        let ctx = Arc::new(ctx);

        // Pre-create the physical metric table with a non-millisecond time
        // index BEFORE any ingestion, so the batcher's bulk path must
        // handle it.
        let mut output = instance
            .do_query(
                &format!(
                    "CREATE TABLE greptime_physical_table (\
                 greptime_timestamp {sql_ts_type} NOT NULL, \
                 greptime_value DOUBLE NULL, \
                 TIME INDEX (greptime_timestamp)) \
                 ENGINE = metric WITH ('physical_metric_table' = 'true')"
                ),
                ctx.clone(),
            )
            .await;
        assert!(output.remove(0).is_ok());

        // submit_build_and_align counts every batcher submission in both
        // acknowledgement modes, so the assertion detects a regression that
        // disables non-millisecond batching.
        let submissions = servers::metrics::PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
            .with_label_values(&["submit_build_and_align"]);
        let before = submissions.get_sample_count();
        for (ts, value) in [(60, 10), (120, 20)] {
            let request = build_sum_request(
                "non.ms.alignment",
                AggregationTemporality::Cumulative,
                &[(ts, value)],
            );
            let response = client
                .post("/v1/otlp/v1/metrics")
                .header("content-type", "application/x-protobuf")
                .body(request.encode_to_vec())
                .send()
                .await;
            assert_eq!(response.status().as_u16(), 200);
        }
        assert_eq!(
            submissions.get_sample_count() - before,
            2,
            "non-millisecond physical tables must use the logical batcher"
        );

        // The rows land on the non-millisecond physical table, converted
        // from the nanosecond encoding.
        let sql = "SELECT greptime_timestamp, greptime_value FROM non_ms_alignment_total ORDER BY greptime_timestamp";
        let batches = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let output = instance.do_query(sql, ctx.clone()).await.remove(0).unwrap();
                let OutputData::Stream(stream) = output.data else {
                    panic!("expected stream")
                };
                let batches = RecordBatches::try_collect(stream).await.unwrap();
                if batches.iter().map(|batch| batch.num_rows()).sum::<usize>() == 2 {
                    break batches;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let batch = &batches.take()[0];
        let stored = |row: usize| match unit {
            ArrowTimeUnit::Microsecond => batch
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(row),
            ArrowTimeUnit::Nanosecond => batch
                .column(0)
                .as_primitive::<TimestampNanosecondType>()
                .value(row),
            _ => unreachable!("covered units only"),
        };
        assert_eq!(stored(0), expected[0]);
        assert_eq!(stored(1), expected[1]);
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Float64Array>()
            .unwrap_or_else(|| panic!("expected f64 values"));
        assert_eq!(values.value(0), 10.0);
        assert_eq!(values.value(1), 20.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_logical_batcher_fallback_for_cross_physical_destination() {
        use std::time::Duration;

        use common_base::Plugins;
        use datatypes::arrow::array::AsArray;
        use datatypes::arrow::datatypes::TimestampMicrosecondType;
        use frontend::server::Services;
        use frontend::service_config::pending_rows_batcher::BatcherOptions;
        use prost::Message;
        use servers::batcher::BatchingProtocol;
        use servers::http::test_helpers::TestClient;
        use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};

        // With the logical batcher enabled, an OTLP request targeting an
        // existing logical table bound to ANOTHER physical table must fall
        // back to the ordinary insert path (the bulk eligibility check
        // rejects the destination binding), which converts the request to
        // the destination's unit. This covers the case previously guarded
        // by the removed OTLP pre-gate alignment.
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_logical_cross_physical")
            .with_logical_batcher(BatcherOptions {
                protocols: vec![BatchingProtocol::Otlp],
                pending_rows_flush_interval: Duration::from_millis(5),
                ..Default::default()
            })
            .build()
            .await;
        let instance = standalone.fe_instance();
        let options = standalone.opts.clone();
        let services = Services::new(options.clone(), instance.clone(), Plugins::default());
        let server = services
            .http_server_builder(
                &options.frontend_options(),
                services.server_memory_limiter.clone(),
            )
            .build();
        let client = TestClient::new(server.build(server.make_app()).unwrap()).await;

        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        ctx.set_logical_batching_enabled(true);
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        }));
        let ctx = Arc::new(ctx);

        // A physical table with a microsecond time index and a logical table
        // bound to it; OTLP requests always select the default
        // (millisecond) physical table, so the destination binding differs.
        for sql in [
            "CREATE TABLE phy_us (\
             greptime_timestamp TIMESTAMP(6) NOT NULL, greptime_value DOUBLE NULL, \
             TIME INDEX (greptime_timestamp)) \
             ENGINE = metric WITH ('physical_metric_table' = 'true')",
            "CREATE TABLE cross_alignment_total (\
             greptime_timestamp TIMESTAMP(6) NOT NULL, greptime_value DOUBLE NULL, \
             \"stream\" STRING NULL, TIME INDEX (greptime_timestamp), PRIMARY KEY (\"stream\")) \
             ENGINE = metric WITH ('on_physical_table' = 'phy_us')",
        ] {
            let mut output = instance.do_query(sql, ctx.clone()).await;
            let result = output.remove(0);
            assert!(result.is_ok(), "setup ddl failed: {result:?} — {sql}");
        }

        let submissions = servers::metrics::PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
            .with_label_values(&["submit_build_and_align"]);
        let before = submissions.get_sample_count();
        let request = build_sum_request(
            "cross.alignment",
            AggregationTemporality::Cumulative,
            &[(60, 10)],
        );
        let response = client
            .post("/v1/otlp/v1/metrics")
            .header("content-type", "application/x-protobuf")
            .body(request.encode_to_vec())
            .send()
            .await;
        assert_eq!(response.status().as_u16(), 200);
        // The request must NOT enter the logical batcher.
        assert_eq!(
            submissions.get_sample_count() - before,
            0,
            "cross-physical destinations must fall back to the ordinary insert path"
        );

        // The row lands on the microsecond logical table, converted from the
        // nanosecond encoding: 60s -> 60_000_000us.
        let sql = "SELECT greptime_timestamp, greptime_value FROM cross_alignment_total";
        let batches = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let output = instance.do_query(sql, ctx.clone()).await.remove(0).unwrap();
                let OutputData::Stream(stream) = output.data else {
                    panic!("expected stream")
                };
                let batches = RecordBatches::try_collect(stream).await.unwrap();
                if batches.iter().map(|batch| batch.num_rows()).sum::<usize>() == 1 {
                    break batches;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let batch = &batches.take()[0];
        let timestamps = batch.column(0).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(timestamps.value(0), 60_000_000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_logical_batcher_alignment() {
        use std::time::Duration;

        use common_base::Plugins;
        use frontend::server::Services;
        use frontend::service_config::pending_rows_batcher::BatcherOptions;
        use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint, exponential_histogram_data_point,
        };
        use prost::Message;
        use servers::batcher::{BatchingProtocol, pending_rows_batch_sync_enabled};
        use servers::http::test_helpers::TestClient;
        use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};

        // Fixed workload, only the batcher opt-in changes. Compare stored rows,
        // schema evolution and visibility under the configured acknowledgement policy.
        let mut results = Vec::new();
        for enabled in [false, true] {
            let standalone = GreptimeDbStandaloneBuilder::new(&format!("otlp_logical_{enabled}"))
                .with_logical_batcher(BatcherOptions {
                    protocols: if enabled {
                        vec![BatchingProtocol::Otlp]
                    } else {
                        vec![]
                    },
                    pending_rows_flush_interval: Duration::from_millis(5),
                    ..Default::default()
                })
                .build()
                .await;
            let instance = standalone.fe_instance();
            let options = standalone.opts.clone();
            let services = Services::new(options.clone(), instance.clone(), Plugins::default());
            let server = services
                .http_server_builder(
                    &options.frontend_options(),
                    services.server_memory_limiter.clone(),
                )
                .build();
            let client = TestClient::new(server.build(server.make_app()).unwrap()).await;
            let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
            ctx.set_logical_batching_enabled(true);
            ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
                with_metric_engine: true,
                ..Default::default()
            }));
            let ctx = Arc::new(ctx);
            let submissions = servers::metrics::PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["submit_wait_flush_result"]);
            let before = submissions.get_sample_count();
            for (ts, value) in [(60, 10), (120, 20)] {
                let mut request = build_sum_request(
                    "batch.alignment",
                    AggregationTemporality::Cumulative,
                    &[(ts, value)],
                );
                if ts == 120
                    && let Some(metric::Data::Sum(sum)) =
                        &mut request.resource_metrics[0].scope_metrics[0].metrics[0].data
                {
                    sum.data_points[0].attributes.push(keyvalue("extra", "new"));
                }
                let response = client
                    .post("/v1/otlp/v1/metrics")
                    .header("content-type", "application/x-protobuf")
                    .body(request.encode_to_vec())
                    .send()
                    .await;
                assert_eq!(response.status().as_u16(), 200);
            }
            if enabled {
                assert_eq!(
                    submissions.get_sample_count() - before,
                    if pending_rows_batch_sync_enabled() {
                        2
                    } else {
                        0
                    },
                    "logical submissions must follow the global acknowledgement policy"
                );
            }
            let sql = "SELECT greptime_timestamp, greptime_value, stream, extra FROM batch_alignment_total ORDER BY greptime_timestamp";
            let batches = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let output = instance.do_query(sql, ctx.clone()).await.remove(0).unwrap();
                    let OutputData::Stream(stream) = output.data else {
                        panic!("expected stream")
                    };
                    let batches = RecordBatches::try_collect(stream).await.unwrap();
                    if !enabled
                        || pending_rows_batch_sync_enabled()
                        || batches.iter().map(|batch| batch.num_rows()).sum::<usize>() == 2
                    {
                        break batches;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                2
            );
            results.push(batches.pretty_print().unwrap());

            let before_histograms = submissions.get_sample_count();
            // First create a histogram table, then reuse it in a mixed export.
            // Neither request may enter the scalar-only logical batcher.
            for mixed in [false, true] {
                let mut request = build_sum_request(
                    "batch.mixed",
                    AggregationTemporality::Cumulative,
                    &[(180, 30)],
                );
                let metrics = &mut request.resource_metrics[0].scope_metrics[0].metrics;
                let mut histogram = metrics[0].clone();
                histogram.name = "batch.histogram".to_string();
                histogram.data = Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    data_points: vec![ExponentialHistogramDataPoint {
                        start_time_unix_nano: 1_000_000_000,
                        time_unix_nano: if mixed { 4_000_000_000 } else { 3_000_000_000 },
                        count: 4,
                        sum: Some(8.0),
                        zero_count: 1,
                        positive: Some(exponential_histogram_data_point::Buckets {
                            offset: -1,
                            bucket_counts: vec![1, 2],
                        }),
                        ..Default::default()
                    }],
                }));
                if !mixed {
                    metrics.clear();
                }
                metrics.push(histogram);
                let response = client
                    .post("/v1/otlp/v1/metrics")
                    .header("content-type", "application/x-protobuf")
                    .body(request.encode_to_vec())
                    .send()
                    .await;
                assert_eq!(
                    response.status().as_u16(),
                    200,
                    "enabled={enabled}, mixed={mixed}"
                );
            }
            assert_eq!(submissions.get_sample_count(), before_histograms);
            for (sql, expected_rows) in [
                (
                    "SELECT greptime_timestamp, greptime_native_histogram FROM batch_histogram ORDER BY greptime_timestamp",
                    2,
                ),
                (
                    "SELECT greptime_timestamp, greptime_value FROM batch_mixed_total ORDER BY greptime_timestamp",
                    1,
                ),
            ] {
                let output = instance.do_query(sql, ctx.clone()).await.remove(0).unwrap();
                let OutputData::Stream(stream) = output.data else {
                    panic!("expected stream")
                };
                let batches = RecordBatches::try_collect(stream).await.unwrap();
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    expected_rows
                );
                results.push(batches.pretty_print().unwrap());
            }

            // An existing metric attached to another physical table must use
            // the original routing, not the request's default physical table.
            for sql in [
                "CREATE TABLE custom_physical (greptime_timestamp TIMESTAMP TIME INDEX, greptime_value DOUBLE) ENGINE=metric WITH ('physical_metric_table'='')",
                "CREATE TABLE custom_total (greptime_timestamp TIMESTAMP(3) TIME INDEX, greptime_value DOUBLE, \"stream\" STRING PRIMARY KEY) ENGINE=metric WITH ('on_physical_table'='custom_physical')",
            ] {
                instance.do_query(sql, ctx.clone()).await.remove(0).unwrap();
            }
            instance
                .metrics(
                    build_sum_request("custom", AggregationTemporality::Cumulative, &[(60, 7)]),
                    ctx.clone(),
                )
                .await
                .unwrap();
            let output = instance
                .do_query("SELECT greptime_value FROM custom_total", ctx.clone())
                .await
                .remove(0)
                .unwrap();
            let OutputData::Stream(stream) = output.data else {
                panic!("expected stream")
            };
            assert!(
                RecordBatches::try_collect(stream)
                    .await
                    .unwrap()
                    .pretty_print()
                    .unwrap()
                    .contains("7.0")
            );

            // Request-level schema policy cannot be bypassed by batching.
            let mut fixed = ctx.fork();
            fixed.set_extension("auto_create_table", "false");
            assert!(
                instance
                    .metrics(
                        build_sum_request(
                            "missing",
                            AggregationTemporality::Cumulative,
                            &[(60, 1)]
                        ),
                        Arc::new(fixed)
                    )
                    .await
                    .unwrap()
                    .rejected_data_points
                    == 1
            );
        }
        assert_eq!(results[..3], results[3..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_on_standalone() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_otlp")
            .build()
            .await;
        let instance = standalone.fe_instance();

        test_otlp(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_on_distributed() {
        let instance = tests::create_distributed_instance("test_standalone_otlp").await;

        test_otlp(&instance.frontend()).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_fixed_schema_rejects_missing_temporality_tag() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_otlp_fixed_schema")
            .with_auto_create_table(false)
            .build()
            .await;
        let instance = standalone.fe_instance();
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "public"));
        let mut output = instance
            .do_query(
                "CREATE TABLE fixed_delta_total (\
                 \"stream\" STRING, greptime_timestamp TIMESTAMP(3) NOT NULL, \
                 greptime_value DOUBLE, TIME INDEX (greptime_timestamp), \
                 PRIMARY KEY (\"stream\")) ENGINE=mito",
                ctx.clone(),
            )
            .await;
        let result = output.remove(0);
        assert!(result.is_ok(), "{result:?}");

        let outcome = instance
            .metrics(
                build_sum_request("fixed.delta", AggregationTemporality::Delta, &[(60, 10)]),
                ctx.clone(),
            )
            .await
            .unwrap();
        assert_eq!(outcome.accepted_data_points, 0);
        assert_eq!(outcome.rejected_data_points, 1);
        assert!(
            outcome
                .error_message
                .unwrap()
                .contains("otlp_aggregation_temporality")
        );

        let mut output = instance
            .do_query("SELECT COUNT(*) FROM fixed_delta_total", ctx)
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        assert!(
            RecordBatches::try_collect(stream)
                .await
                .unwrap()
                .pretty_print()
                .unwrap()
                .contains("| 0        |")
        );
    }

    async fn test_otlp(instance: &Arc<Instance>) {
        let req = build_request();
        let db = "otlp";
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, db));

        assert!(
            SqlQueryHandler::do_query(
                instance.as_ref(),
                &format!("CREATE DATABASE IF NOT EXISTS {db}"),
                ctx.clone(),
            )
            .await
            .first()
            .unwrap()
            .is_ok()
        );

        let resp = instance.metrics(req, ctx.clone()).await;
        assert!(resp.is_ok());

        let mut output = instance
            .do_query(
                "CREATE TABLE raw_delta_mito_total (\
                 \"stream\" STRING, greptime_timestamp TIMESTAMP(3) NOT NULL, \
                 greptime_value DOUBLE, TIME INDEX (greptime_timestamp), \
                 PRIMARY KEY (\"stream\")) ENGINE=mito",
                ctx.clone(),
            )
            .await;
        let result = output.remove(0);
        assert!(result.is_ok(), "{result:?}");

        for (metric, table) in [
            ("raw.delta", "raw_delta_total"),
            ("raw.delta.mito", "raw_delta_mito_total"),
        ] {
            for request in [
                build_sum_request(metric, AggregationTemporality::Cumulative, &[(60, 10)]),
                build_sum_request(
                    metric,
                    AggregationTemporality::Delta,
                    &[(60, 10), (120, 20), (180, 15)],
                ),
                build_sum_request(metric, AggregationTemporality::Cumulative, &[(180, 30)]),
            ] {
                let result = instance.metrics(request, ctx.clone()).await;
                assert!(result.is_ok(), "{metric}: {result:?}");
            }

            let mut output = instance
                .do_query(
                    &format!(
                        "SELECT COALESCE(otlp_aggregation_temporality, '') AS temporality, \
                         COUNT(*) AS samples, SUM(greptime_value) AS total \
                         FROM {table} GROUP BY otlp_aggregation_temporality ORDER BY temporality"
                    ),
                    ctx.clone(),
                )
                .await;
            let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                unreachable!()
            };
            assert_eq!(
                RecordBatches::try_collect(stream)
                    .await
                    .unwrap()
                    .pretty_print()
                    .unwrap(),
                "\
+-------------+---------+-------+
| temporality | samples | total |
+-------------+---------+-------+
|             | 2       | 40.0  |
| delta       | 3       | 45.0  |
+-------------+---------+-------+"
            );

            for (function, expected) in [("increase", "45.0"), ("rate", "0.25")] {
                let mut output = instance
                    .do_query(
                        &format!("TQL EVAL (180, 180, '1m') {function}({table}[3m])"),
                        ctx.clone(),
                    )
                    .await;
                let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                    unreachable!()
                };
                let rendered = RecordBatches::try_collect(stream)
                    .await
                    .unwrap()
                    .pretty_print()
                    .unwrap();
                assert!(rendered.contains("delta"), "{rendered}");
                assert!(rendered.contains(expected), "{rendered}");
            }

            let mut stale = build_sum_request(metric, AggregationTemporality::Delta, &[(240, 99)]);
            let Some(metric::Data::Sum(sum)) = stale.resource_metrics[0].scope_metrics[0].metrics
                [0]
            .data
            .as_mut() else {
                unreachable!()
            };
            sum.data_points[0].flags = DataPointFlags::NoRecordedValueMask as u32;
            assert!(instance.metrics(stale, ctx.clone()).await.is_ok());

            for (matcher, expected_rows) in [
                ("otlp_aggregation_temporality=\"delta\"", 0),
                ("otlp_aggregation_temporality!=\"delta\"", 1),
            ] {
                let mut output = instance
                    .do_query(
                        &format!("TQL EVAL (240, 240, '1m') {table}{{{matcher}}}"),
                        ctx.clone(),
                    )
                    .await;
                let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                    unreachable!()
                };
                let batches = RecordBatches::try_collect(stream).await.unwrap();
                assert_eq!(
                    expected_rows,
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
                );
                if expected_rows == 1 {
                    assert!(batches.pretty_print().unwrap().contains("30.0"));
                }
            }
        }

        let malformed = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "rejected.delta.histogram".to_string(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                count: 1,
                                bucket_counts: vec![1],
                                explicit_bounds: vec![1.0, 2.0],
                                ..Default::default()
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let outcome = instance.metrics(malformed, ctx.clone()).await.unwrap();
        assert_eq!(0, outcome.accepted_data_points);
        assert_eq!(1, outcome.rejected_data_points);
        assert!(
            outcome
                .error_message
                .as_deref()
                .unwrap()
                .contains("bucket_counts length")
        );
        let mut output = instance
            .do_query(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN \
                 ('rejected_delta_histogram_bucket', 'rejected_delta_histogram_sum', \
                  'rejected_delta_histogram_count')",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        assert!(
            RecordBatches::try_collect(stream)
                .await
                .unwrap()
                .pretty_print()
                .unwrap()
                .contains("| 0        |")
        );

        let point = |stream: &str, seconds: u64, bounds: Vec<f64>, sum| HistogramDataPoint {
            attributes: vec![keyvalue("stream", stream)],
            time_unix_nano: seconds * 1_000_000_000,
            count: u64::try_from(bounds.len() + 1).unwrap(),
            sum,
            bucket_counts: vec![1; bounds.len() + 1],
            explicit_bounds: bounds,
            ..Default::default()
        };
        let tombstone = |stream: &str, seconds: u64, bounds: Vec<f64>, sum| HistogramDataPoint {
            flags: DataPointFlags::NoRecordedValueMask as u32,
            ..point(stream, seconds, bounds, sum)
        };
        for points in [
            vec![
                point("same", 300, vec![1.0, 2.0], Some(4.0)),
                tombstone("same", 360, vec![1.0, 2.0], None),
            ],
            vec![
                point("changed", 420, vec![3.0, 5.0], Some(8.0)),
                tombstone("changed", 480, vec![1.0, 2.0], Some(99.0)),
            ],
            vec![
                point("boundless", 420, vec![3.0, 5.0], Some(8.0)),
                tombstone("boundless", 480, vec![], None),
            ],
            vec![tombstone("new", 480, vec![], None)],
        ] {
            let outcome = instance
                .metrics(
                    build_histogram_request("raw.delta.histogram", points),
                    ctx.clone(),
                )
                .await
                .unwrap();
            assert_eq!(0, outcome.rejected_data_points);
        }

        let mut output = instance
            .do_query(
                "TQL EVAL (300, 300, '1m') \
                 histogram_quantile(0.5, rate(raw_delta_histogram_bucket[2m]))",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let rendered = RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .pretty_print()
            .unwrap();
        assert!(rendered.contains("1.5"), "{rendered}");

        for (query, expected_rows) in [
            ("raw_delta_histogram_bucket{stream=\"same\",le=~\"1|2\"}", 0),
            ("raw_delta_histogram_sum{stream=\"same\"}", 1),
            (
                "raw_delta_histogram_bucket{stream=\"changed\",le=~\"3|5\"}",
                2,
            ),
            ("raw_delta_histogram_sum{stream=\"changed\"}", 0),
            (
                "raw_delta_histogram_bucket{stream=\"boundless\",le=~\"3|5\"}",
                2,
            ),
        ] {
            let mut output = instance
                .do_query(&format!("TQL EVAL (480, 480, '1m') {query}"), ctx.clone())
                .await;
            let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                unreachable!()
            };
            let batches = RecordBatches::try_collect(stream).await.unwrap();
            assert_eq!(
                expected_rows,
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                "{query}: {}",
                batches.pretty_print().unwrap()
            );
        }

        let mut output = instance
            .do_query(
                "SELECT \
                   (SELECT COUNT(*) FROM raw_delta_histogram_bucket WHERE \"stream\" = 'new') AS buckets, \
                   (SELECT COUNT(*) FROM raw_delta_histogram_count WHERE \"stream\" = 'new') AS counts, \
                   (SELECT COUNT(*) FROM raw_delta_histogram_sum WHERE \"stream\" = 'new') AS sums",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let rendered = RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .pretty_print()
            .unwrap();
        assert!(
            rendered.contains("| 1       | 1      | 0    |"),
            "{rendered}"
        );

        let mut output = instance
            .do_query(
                "SELECT * FROM my_test_metric_my_ignored_unit ORDER BY greptime_timestamp",
                ctx.clone(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+----------------+---------------------+----------------+
| container_name | greptime_timestamp  | greptime_value |
+----------------+---------------------+----------------+
| testserver     | 1970-01-01T00:00:00 | 105.0          |
| testsevrer     | 1970-01-01T00:00:00 | 100.0          |
+----------------+---------------------+----------------+",
        );

        let mut output = instance
            .do_query(
                "SELECT le, greptime_value FROM my_test_histo_my_ignored_unit_bucket order by le",
                ctx.clone(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+-----+----------------+
| le  | greptime_value |
+-----+----------------+
| 1   | 1.0            |
| 5   | 3.0            |
| inf | 4.0            |
+-----+----------------+",
        );

        let mut output = instance
            .do_query(
                "SELECT * FROM my_test_histo_my_ignored_unit_sum",
                ctx.clone(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+------------+---------------------+----------------+
| host       | greptime_timestamp  | greptime_value |
+------------+---------------------+----------------+
| testserver | 1970-01-01T00:00:00 | 51.0           |
+------------+---------------------+----------------+",
        );

        let mut output = instance
            .do_query(
                "SELECT * FROM my_test_histo_my_ignored_unit_count",
                ctx.clone(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+------------+---------------------+----------------+
| host       | greptime_timestamp  | greptime_value |
+------------+---------------------+----------------+
| testserver | 1970-01-01T00:00:00 | 4.0            |
+------------+---------------------+----------------+",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_metrics_into_microsecond_physical_table_on_standalone() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_otlp_us_physical")
            .build()
            .await;

        test_otlp_metrics_into_non_millisecond_physical_table(
            standalone.fe_instance(),
            "TIMESTAMP(6)",
            TimeUnit::Microsecond,
            1_704_067_200_123_456,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_metrics_into_microsecond_physical_table_on_distributed() {
        let instance = tests::create_distributed_instance("test_otlp_us_physical_dist").await;

        test_otlp_metrics_into_non_millisecond_physical_table(
            &instance.frontend(),
            "TIMESTAMP(6)",
            TimeUnit::Microsecond,
            1_704_067_200_123_456,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_metrics_into_seconds_physical_table_on_standalone() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_otlp_s_physical")
            .build()
            .await;

        // Narrowing truncates: 1704067200123456789ns -> 1704067200s.
        test_otlp_metrics_into_non_millisecond_physical_table(
            standalone.fe_instance(),
            "TIMESTAMP(0)",
            TimeUnit::Second,
            1_704_067_200,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_metrics_into_nanoseconds_physical_table_on_standalone() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_otlp_ns_physical")
            .build()
            .await;

        // Same unit: the full nanosecond precision is kept verbatim.
        test_otlp_metrics_into_non_millisecond_physical_table(
            standalone.fe_instance(),
            "TIMESTAMP(9)",
            TimeUnit::Nanosecond,
            1_704_067_200_123_456_789,
        )
        .await;
    }

    /// Regression test for <https://github.com/GreptimeTeam/greptimedb/issues/9231>:
    /// a physical metric table pre-created with a non-millisecond time index
    /// accepts OTLP ingestion; the nanosecond samples are converted to the
    /// physical table's unit (kept verbatim for a nanosecond table,
    /// truncating the sub-unit part for micro/second tables).
    async fn test_otlp_metrics_into_non_millisecond_physical_table(
        instance: &Arc<Instance>,
        sql_ts_type: &str,
        expected_unit: TimeUnit,
        expected_value: i64,
    ) {
        let db = "otlp_non_ms_physical";
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, db);
        // Route the request to the metric engine, like the OTLP HTTP handler
        // does with the default `prom_store.with_metric_engine = true`.
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        }));
        let ctx = Arc::new(ctx);
        assert!(
            SqlQueryHandler::do_query(
                instance.as_ref(),
                &format!("CREATE DATABASE IF NOT EXISTS {db}"),
                ctx.clone(),
            )
            .await
            .first()
            .unwrap()
            .is_ok()
        );

        let mut output = instance
            .do_query(
                &format!(
                    "CREATE TABLE greptime_physical_table (\
                 greptime_timestamp {sql_ts_type} NOT NULL, \
                 greptime_value DOUBLE NULL, \
                 TIME INDEX (greptime_timestamp)) \
                 ENGINE = metric WITH ('physical_metric_table' = 'true')"
                ),
                ctx.clone(),
            )
            .await;
        assert!(output.remove(0).is_ok());

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "my_gauge".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![keyvalue("host", "h1")],
                                time_unix_nano: 1_704_067_200_123_456_789,
                                value: Some(Value::AsDouble(1.0)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        instance.metrics(request, ctx.clone()).await.unwrap();

        let mut output = instance
            .do_query("SELECT greptime_timestamp FROM my_gauge", ctx.clone())
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let batches = RecordBatches::try_collect(stream).await.unwrap().take();
        assert_eq!(batches[0].num_rows(), 1);
        // The logical table's time index keeps the physical table's unit.
        let ts_column = batches[0].column(0);
        assert_eq!(
            ts_column.data_type(),
            &DataType::Timestamp(expected_unit, None),
            "unexpected time index type"
        );
        let stored = match expected_unit {
            TimeUnit::Second => ts_column.as_primitive::<TimestampSecondType>().value(0),
            TimeUnit::Millisecond => ts_column
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            TimeUnit::Microsecond => ts_column
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            TimeUnit::Nanosecond => ts_column.as_primitive::<TimestampNanosecondType>().value(0),
        };
        assert_eq!(stored, expected_value);
    }

    fn build_request() -> ExportMetricsServiceRequest {
        let data_points = vec![
            NumberDataPoint {
                attributes: vec![keyvalue("container.name", "testsevrer")],
                time_unix_nano: 100,
                value: Some(Value::AsInt(100)),
                ..Default::default()
            },
            NumberDataPoint {
                attributes: vec![keyvalue("container.name", "testserver")],
                time_unix_nano: 105,
                value: Some(Value::AsInt(105)),
                ..Default::default()
            },
        ];
        let gauge = Gauge { data_points };

        let histo_data_points = vec![HistogramDataPoint {
            attributes: vec![keyvalue("host", "testserver")],
            time_unix_nano: 100,
            count: 4,
            bucket_counts: vec![1, 2, 1],
            explicit_bounds: vec![1.0f64, 5.0f64],
            sum: Some(51f64),
            ..Default::default()
        }];

        let histo = Histogram {
            data_points: histo_data_points,
            aggregation_temporality: 0,
        };

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "my.test.metric".into(),
                            description: "my ignored desc".into(),
                            unit: "my ignored unit".into(),
                            metadata: vec![],
                            data: Some(metric::Data::Gauge(gauge)),
                        },
                        Metric {
                            name: "my.test.histo".into(),
                            description: "my ignored desc".into(),
                            unit: "my ignored unit".into(),
                            metadata: vec![],
                            data: Some(metric::Data::Histogram(histo)),
                        },
                    ],
                    scope: Some(InstrumentationScope {
                        attributes: vec![
                            keyvalue("scope", "otel"),
                            keyvalue("telemetry.sdk.name", "java"),
                        ],
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                resource: Some(Resource {
                    attributes: vec![keyvalue("resource", "greptimedb")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                ..Default::default()
            }],
        }
    }

    fn metric_request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn gauge_metric(name: &str, points: &[(u64, f64, Option<&str>)]) -> Metric {
        Metric {
            name: name.into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: points
                    .iter()
                    .map(|&(time, value, host)| NumberDataPoint {
                        time_unix_nano: time,
                        value: Some(Value::AsDouble(value)),
                        attributes: host
                            .map(|host| vec![keyvalue("host", host)])
                            .unwrap_or_default(),
                        ..Default::default()
                    })
                    .collect(),
            })),
            ..Default::default()
        }
    }

    fn native_metric(name: &str, scale: i32) -> Metric {
        use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint,
        };
        Metric {
            name: name.into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 60_000_000_000,
                    count: 10,
                    zero_count: 10,
                    scale,
                    sum: Some(0.0),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        }
    }

    #[test]
    fn test_otlp_primary_and_descriptor_write_failures() {
        for sync in [false, true] {
            temp_env::with_var("PENDING_ROWS_BATCH_SYNC", Some(sync.to_string()), || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        tokio::time::timeout(
                            Duration::from_secs(60),
                            check_metric_write_failure(sync),
                        )
                        .await
                        .expect("metric acknowledgement must make progress");
                    });
            });
        }
    }

    async fn check_metric_write_failure(sync: bool) {
        use prost::Message;

        let standalone = GreptimeDbStandaloneBuilder::new("otlp_write_failure")
            .with_logical_batcher(BatcherOptions {
                protocols: vec![BatchingProtocol::Otlp],
                max_batch_rows: 1,
                pending_rows_flush_interval: Duration::from_secs(3600),
                ..Default::default()
            })
            .build()
            .await;
        let source = standalone.fe_instance();
        let gate = Arc::new(Mutex::new(None));
        let cache_registry = Arc::new(
            cache::with_default_composite_cache_registry(
                LayeredCacheRegistryBuilder::default().add_cache_registry(
                    cache::build_fundamental_cache_registry(standalone.kv_backend.clone()),
                ),
            )
            .unwrap()
            .build(),
        );
        let instance = Arc::new(
            FrontendBuilder::new(
                standalone.opts.frontend_options(),
                standalone.kv_backend.clone(),
                cache_registry,
                source.catalog_manager().clone(),
                Arc::new(GatedTraceNodeManager {
                    inner: source.node_manager().clone(),
                    gate: gate.clone(),
                }),
                source.procedure_executor().clone(),
                Arc::new(catalog::process_manager::ProcessManager::new(
                    "otlp_write_failure".into(),
                    None,
                )),
            )
            .try_build()
            .await
            .unwrap(),
        );
        instance.init_logical_batcher(&standalone.opts.frontend_options());
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        }));
        let mut warm = metric_request(vec![gauge_metric(
            "ack_metric",
            &[(60_000_000_000, 1.0, None)],
        )]);
        warm.resource_metrics[0].resource = Some(Resource {
            attributes: vec![
                keyvalue("service.name", "ack-service"),
                keyvalue("host.id", "ack-host"),
            ],
            ..Default::default()
        });
        instance
            .metrics(warm.clone(), Arc::new(ctx.clone()))
            .await
            .unwrap();
        let region = async |name: &str| {
            let table = instance
                .catalog_manager()
                .table(DEFAULT_CATALOG_NAME, "public", name, None)
                .await
                .unwrap()
                .unwrap();
            instance
                .partition_manager()
                .find_physical_table_route(table.table_info().table_id())
                .await
                .unwrap()
                .region_routes[0]
                .region
                .id
        };
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *gate.lock().unwrap() = Some(TraceAuxWriteGate {
            region_id: region("ack_metric").await,
            started: started_tx,
            release: release_rx,
        });
        ctx.set_logical_batching_enabled(true);
        let request = metric_request(vec![gauge_metric(
            "ack_metric",
            &[(120_000_000_000, 2.0, None)],
        )]);
        let failures = servers::metrics::FLUSH_FAILURES.get();
        let dropped = servers::metrics::FLUSH_DROPPED_ROWS.get();
        let write = tokio::spawn({
            let instance = instance.clone();
            let ctx = Arc::new(ctx.clone());
            async move { instance.metrics(request, ctx).await }
        });
        tokio::time::timeout(Duration::from_secs(10), started_rx)
            .await
            .expect("primary batch must reach the write gate")
            .unwrap();
        if sync {
            assert!(!write.is_finished());
            release_tx.send(false).unwrap();
            assert!(write.await.unwrap().is_err());
        } else {
            let accepted = tokio::time::timeout(Duration::from_secs(10), write)
                .await
                .expect("asynchronous acknowledgement must precede physical completion")
                .unwrap()
                .unwrap();
            assert_eq!(
                (accepted.accepted_data_points, accepted.rejected_data_points),
                (1, 0)
            );
            release_tx.send(false).unwrap();
            while servers::metrics::FLUSH_FAILURES.get() == failures
                || servers::metrics::FLUSH_DROPPED_ROWS.get() == dropped
            {
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(servers::metrics::FLUSH_FAILURES.get(), failures + 1);
        assert_eq!(servers::metrics::FLUSH_DROPPED_ROWS.get(), dropped + 1);
        assert_trace_v2_query(
            &instance,
            "SELECT COUNT(*)=1 AND MAX(greptime_value)=1 FROM ack_metric",
            Arc::new(ctx),
        )
        .await
        .unwrap();

        // The derived write fails after the metric was accepted, including when
        // primary logical batching acknowledges before its flush completes.
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *gate.lock().unwrap() = Some(TraceAuxWriteGate {
            region_id: region("greptime_otel_resource_info").await,
            started: started_tx,
            release: release_rx,
        });
        let options = standalone.opts.clone();
        let services = frontend::server::Services::new(
            options.clone(),
            instance.clone(),
            common_base::Plugins::default(),
        );
        let server = services
            .http_server_builder(
                &options.frontend_options(),
                services.server_memory_limiter.clone(),
            )
            .build();
        let client =
            servers::http::test_helpers::TestClient::new(server.build(server.make_app()).unwrap())
                .await;
        let errors = frontend::OTLP_RESOURCE_INFO_WRITE_ERRORS.get();
        let accepted_rows = frontend::OTLP_METRICS_ROWS.get();
        let response = tokio::spawn(async move {
            client
                .post("/v1/otlp/v1/metrics")
                .header("content-type", "application/x-protobuf")
                .body(warm.encode_to_vec())
                .send()
                .await
        });
        started_rx.await.unwrap();
        release_tx.send(false).unwrap();
        let response = response.await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let bytes = response.bytes().await;
        let response = otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceResponse::decode(bytes).unwrap();
        let partial = response.partial_success.unwrap();
        assert_eq!(partial.rejected_data_points, 0);
        assert!(partial.error_message.contains("descriptor"));
        assert_eq!(frontend::OTLP_RESOURCE_INFO_WRITE_ERRORS.get(), errors + 1);
        assert_eq!(frontend::OTLP_METRICS_ROWS.get(), accepted_rows + 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_metric_permission_precedes_preflight() {
        use common_error::ext::ErrorExt;
        use common_error::status_code::StatusCode;

        #[derive(Default)]
        struct DenyRestricted {
            calls: Mutex<Vec<Vec<String>>>,
        }

        impl auth::PermissionChecker for DenyRestricted {
            fn check_permission(
                &self,
                _user_info: auth::UserInfoRef,
                _req: auth::PermissionReq,
            ) -> auth::error::Result<auth::PermissionResp> {
                Ok(auth::PermissionResp::Allow)
            }

            fn check_permission_with_table_targets(
                &self,
                _user_info: auth::UserInfoRef,
                req: auth::PermissionReq,
                targets: auth::PermissionTableTargets,
            ) -> auth::error::Result<auth::PermissionResp> {
                if matches!(req, auth::PermissionReq::Action(auth::OTLP_WRITE)) {
                    let auth::PermissionTableTargets::Resolved(targets) = targets else {
                        panic!("metric destinations must be resolved");
                    };
                    let denied = targets.iter().any(|t| t.table == "restricted_metric");
                    self.calls
                        .lock()
                        .unwrap()
                        .push(targets.into_iter().map(|t| t.table).collect());
                    if denied {
                        return Ok(auth::PermissionResp::Reject);
                    }
                }
                Ok(auth::PermissionResp::Allow)
            }
        }

        let checker = Arc::new(DenyRestricted::default());
        let plugins = common_base::Plugins::new();
        plugins.insert::<auth::PermissionCheckerRef>(checker.clone());
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_permission_preflight")
            .with_plugin(plugins)
            .build()
            .await;
        let instance = standalone.fe_instance();
        let ctx = QueryContext::arc();
        instance
            .do_query(
                "CREATE TABLE restricted_metric (greptime_timestamp TIMESTAMP(3) TIME INDEX, \
                 greptime_value DOUBLE, host STRING NOT NULL PRIMARY KEY) \
                 WITH ('otlp_metric_compat'='prom')",
                ctx.clone(),
            )
            .await
            .remove(0)
            .unwrap();

        for host in [Some("present"), None] {
            for with_allowed in [false, true] {
                let mut metrics = vec![gauge_metric(
                    "restricted_metric",
                    &[(60_000_000_000, 1.0, host), (120_000_000_000, 2.0, host)],
                )];
                let mut expected_targets = vec!["restricted_metric"];
                if with_allowed {
                    metrics.push(gauge_metric(
                        "allowed_metric",
                        &[(60_000_000_000, 7.0, None)],
                    ));
                    expected_targets.push("allowed_metric");
                }
                let err = instance
                    .metrics(metric_request(metrics), ctx.clone())
                    .await
                    .unwrap_err();
                assert_eq!(err.status_code(), StatusCode::PermissionDenied);
                assert!(!err.to_string().contains("missing required column"));
                assert_eq!(
                    std::mem::take(&mut *checker.calls.lock().unwrap()),
                    vec![expected_targets]
                );
            }
        }
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*)=0 FROM information_schema.tables WHERE table_name='allowed_metric'",
            ctx,
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_preflight_defaults_order_and_required_columns() {
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_preflight_defaults")
            .build()
            .await;
        let instance = standalone.fe_instance();
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "public"));
        instance.do_query("CREATE TABLE defaulted (greptime_timestamp TIMESTAMP(3) TIME INDEX, greptime_value DOUBLE, host STRING NOT NULL DEFAULT 'unknown' PRIMARY KEY)",ctx.clone()).await.remove(0).unwrap();
        let outcome = instance
            .metrics(
                metric_request(vec![gauge_metric(
                    "defaulted",
                    &[
                        (60_000_000_000, 10.0, None),
                        (60_000_000_000, 20.0, Some("unknown")),
                        (60_000_000_000, 30.0, None),
                    ],
                )]),
                ctx.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            (outcome.accepted_data_points, outcome.rejected_data_points),
            (3, 0)
        );
        for flushed in [false, true] {
            if flushed {
                instance
                    .do_query("ADMIN FLUSH_TABLE('defaulted')", ctx.clone())
                    .await
                    .remove(0)
                    .unwrap();
            }
            assert_trace_v2_query(instance,"SELECT COUNT(*)=1 AND MIN(greptime_value)=30 AND MIN(host)='unknown' FROM defaulted",ctx.clone()).await.unwrap();
        }
        instance.do_query("CREATE TABLE required (greptime_timestamp TIMESTAMP(3) TIME INDEX, greptime_value DOUBLE, host STRING NOT NULL PRIMARY KEY)",ctx.clone()).await.remove(0).unwrap();
        for (name, points, expected) in [
            (
                "defaulted",
                [
                    (120_000_000_000, 1.0, None),
                    (180_000_000_000, 2.0, Some("supplied")),
                ],
                (3, 0),
            ),
            (
                "required",
                [
                    (60_000_000_000, 10.0, None),
                    (120_000_000_000, 20.0, Some("present")),
                ],
                (2, 1),
            ),
        ] {
            for reverse in [false, true] {
                let mut points = points;
                if reverse {
                    points.reverse();
                }
                let outcome = instance
                    .metrics(
                        metric_request(vec![
                            gauge_metric(name, &points),
                            gauge_metric("independent", &[(60_000_000_000, 7.0, None)]),
                        ]),
                        ctx.clone(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    (outcome.accepted_data_points, outcome.rejected_data_points),
                    expected,
                    "{name}, reverse={reverse}"
                );
            }
        }
        for sql in [
            "SELECT COUNT(*)=1 FROM defaulted WHERE greptime_value=1 AND host='unknown'",
            "SELECT COUNT(*)=1 FROM defaulted WHERE greptime_value=2 AND host='supplied'",
            "SELECT COUNT(*)=1 AND MIN(host)='present' FROM required",
        ] {
            assert_trace_v2_query(instance, sql, ctx.clone())
                .await
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_preflight_custom_backing_and_timestamp_units() {
        for policy in 0..3 {
            let standalone = GreptimeDbStandaloneBuilder::new(&format!("otlp_backing_{policy}"))
                .with_auto_create_table(policy != 0)
                .build()
                .await;
            let instance = standalone.fe_instance();
            for precision in [0, 3, 6, 9] {
                let db = format!("backing_{precision}");
                let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, &db);
                ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
                    with_metric_engine: true,
                    ..Default::default()
                }));
                if policy < 2 {
                    ctx.set_extension(
                        "auto_create_table",
                        if policy == 0 { "true" } else { "false" },
                    );
                }
                let ctx = Arc::new(ctx);
                for sql in [
                    format!("CREATE DATABASE {db}"),
                    format!(
                        "CREATE TABLE custom_physical (greptime_timestamp TIMESTAMP({precision}) TIME INDEX, greptime_value DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true')"
                    ),
                    format!(
                        "CREATE TABLE custom_total (greptime_timestamp TIMESTAMP({precision}) TIME INDEX, greptime_value DOUBLE, \"stream\" STRING PRIMARY KEY) ENGINE=metric WITH ('on_physical_table'='custom_physical','otlp_metric_compat'='prom')"
                    ),
                ] {
                    instance
                        .do_query(&sql, ctx.clone())
                        .await
                        .remove(0)
                        .unwrap();
                }
                let mut request =
                    build_sum_request("custom", AggregationTemporality::Cumulative, &[(60, 7)]);
                if let Some(metric::Data::Sum(data)) = request.resource_metrics[0].scope_metrics[0]
                    .metrics[0]
                    .data
                    .as_mut()
                {
                    data.data_points[0].time_unix_nano += 123_456_789;
                }
                request.resource_metrics[0].scope_metrics[0]
                    .metrics
                    .push(native_metric("bad", -5));
                let outcome = instance.metrics(request, ctx.clone()).await.unwrap();
                assert_eq!(
                    (outcome.accepted_data_points, outcome.rejected_data_points),
                    (1, 1),
                    "policy={policy}, precision={precision}, {outcome:?}"
                );
                let expected = 60_123_456_789i64 / 10i64.pow(9 - precision);
                let output = instance
                    .do_query(
                        "SELECT greptime_timestamp,greptime_value FROM custom_total",
                        ctx.clone(),
                    )
                    .await
                    .remove(0)
                    .unwrap();
                let OutputData::Stream(stream) = output.data else {
                    unreachable!()
                };
                let batches = RecordBatches::try_collect(stream).await.unwrap().take();
                let column = batches[0].column(0);
                let actual = match precision {
                    0 => column.as_primitive::<TimestampSecondType>().value(0),
                    3 => column.as_primitive::<TimestampMillisecondType>().value(0),
                    6 => column.as_primitive::<TimestampMicrosecondType>().value(0),
                    _ => column.as_primitive::<TimestampNanosecondType>().value(0),
                };
                assert_eq!(actual, expected);
                assert_trace_v2_query(instance,&format!("SELECT COUNT(*)=0 FROM information_schema.tables WHERE table_schema='{db}' AND table_name IN ('greptime_physical_table','bad')"),ctx.clone()).await.unwrap();
                if policy == 2 && precision == 3 {
                    instance.do_query("CREATE TABLE not_physical (greptime_timestamp TIMESTAMP TIME INDEX, greptime_value DOUBLE)", ctx.clone()).await.remove(0).unwrap();
                    let mut selected = (*ctx).clone();
                    selected.set_extension(
                        servers::http::prom_store::PHYSICAL_TABLE_PARAM,
                        "not_physical",
                    );
                    let mut request = build_sum_request(
                        "custom",
                        AggregationTemporality::Cumulative,
                        &[(120, 9)],
                    );
                    request.resource_metrics[0].scope_metrics[0]
                        .metrics
                        .push(gauge_metric(
                            "invalid_backing",
                            &[(120_000_000_000, 1.0, None)],
                        ));
                    let outcome = instance.metrics(request, Arc::new(selected)).await.unwrap();
                    assert_eq!(
                        (outcome.accepted_data_points, outcome.rejected_data_points),
                        (1, 1)
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_delta_exponential_distributed_queries() {
        use otel_arrow_rust::proto::opentelemetry::metrics::v1::exponential_histogram_data_point::Buckets;

        let cluster = tests::create_distributed_instance("delta_exponential_dist").await;
        let instance = cluster.frontend();
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        }));
        let ctx = Arc::new(ctx);
        let mut metric = native_metric("delta_distribution", 0);
        let Some(metric::Data::ExponentialHistogram(data)) = metric.data.as_mut() else {
            unreachable!()
        };
        let point = data.data_points[0].clone();
        data.data_points = [(60, 10), (120, 20), (180, 15)]
            .into_iter()
            .map(|(end, count)| {
                let mut point = point.clone();
                point.time_unix_nano = end * 1_000_000_000;
                point.start_time_unix_nano = (end - 60) * 1_000_000_000;
                point.count = count;
                point.sum = Some(count as f64 * 1.5);
                point.zero_count = 0;
                point.zero_threshold = 0.001;
                point.positive = Some(Buckets {
                    offset: 0,
                    bucket_counts: vec![count],
                });
                point
            })
            .collect();
        let outcome = instance
            .metrics(metric_request(vec![metric]), ctx.clone())
            .await
            .unwrap();
        assert_eq!(
            (outcome.accepted_data_points, outcome.rejected_data_points),
            (3, 0)
        );
        for flushed in [false, true] {
            if flushed {
                instance
                    .do_query("ADMIN FLUSH_TABLE('delta_distribution')", ctx.clone())
                    .await
                    .remove(0)
                    .unwrap();
            }
            let series =
                native_histogram_remote_read(&instance, "delta_distribution", ctx.clone()).await;
            assert_eq!(series.len(), 1);
            assert!(
                series[0]
                    .labels
                    .iter()
                    .any(|l| l.name == "otlp_aggregation_temporality" && l.value == "delta")
            );
            assert_eq!(series[0].histograms.len(), 3);
            for (histogram, (timestamp, count)) in
                series[0]
                    .histograms
                    .iter()
                    .zip([(60_000, 10), (120_000, 20), (180_000, 15)])
            {
                assert_eq!(histogram.timestamp, timestamp);
                assert_eq!(
                    histogram.count,
                    Some(api::prom_store::remote::histogram::Count::CountInt(count))
                );
                assert_eq!(histogram.positive_deltas, vec![count as i64]);
                assert_eq!(histogram.positive_spans[0].offset, 1);
                assert_eq!(histogram.sum, count as f64 * 1.5);
                assert_eq!(histogram.reset_hint, 3);
            }
            for (expression, expected) in [
                (
                    "histogram_count(sum_over_time(delta_distribution[3m]))",
                    45.0,
                ),
                ("histogram_sum(sum_over_time(delta_distribution[3m]))", 67.5),
                (
                    "histogram_count(sum_over_time(delta_distribution[3m]) / 180)",
                    0.25,
                ),
                (
                    "histogram_fraction(1, 2, sum_over_time(delta_distribution[3m]))",
                    1.0,
                ),
            ] {
                let output = instance
                    .do_query(
                        &format!("TQL EVAL (180,180,'1m') {expression}"),
                        ctx.clone(),
                    )
                    .await
                    .remove(0)
                    .unwrap();
                let OutputData::Stream(stream) = output.data else {
                    unreachable!()
                };
                let batches = RecordBatches::try_collect(stream).await.unwrap().take();
                assert_eq!(batches[0].num_rows(), 1);
                assert_eq!(
                    batches[0]
                        .columns()
                        .iter()
                        .find(|column| column.data_type() == &DataType::Float64)
                        .unwrap()
                        .as_primitive::<datatypes::arrow::datatypes::Float64Type>()
                        .value(0),
                    expected,
                    "{expression}"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_preflight_logical_nullability_and_multiple_backing_units() {
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_logical_presence")
            .with_logical_batcher(BatcherOptions {
                protocols: vec![BatchingProtocol::Otlp],
                max_batch_rows: 1,
                ..Default::default()
            })
            .build()
            .await;
        let instance = standalone.fe_instance();
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        for (physical, logical, precision) in [
            ("greptime_physical_table", "millis", 3),
            ("custom_physical", "nanos", 9),
        ] {
            for sql in [
                format!(
                    "CREATE TABLE {physical} (greptime_timestamp TIMESTAMP({precision}) TIME INDEX, greptime_value DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true')"
                ),
                format!(
                    "CREATE TABLE {logical} (greptime_timestamp TIMESTAMP({precision}) TIME INDEX, greptime_value DOUBLE, host STRING NOT NULL PRIMARY KEY) ENGINE=metric WITH ('on_physical_table'='{physical}')"
                ),
            ] {
                instance
                    .do_query(&sql, Arc::new(ctx.clone()))
                    .await
                    .remove(0)
                    .unwrap();
            }
        }
        for with_metric_engine in [false, true] {
            ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
                with_metric_engine,
                ..Default::default()
            }));
            for batching in [false, true] {
                ctx.set_logical_batching_enabled(batching);
                for reverse in [false, true] {
                    let mut points = vec![
                        (60_000_000_123, 10.0, None),
                        (120_000_000_456, 20.0, Some("present")),
                    ];
                    if reverse {
                        points.reverse();
                    }
                    // The default-backed target remains eligible when sent alone;
                    // custom-backed targets retain the established fallback.
                    for names in [vec!["millis"], vec!["millis", "nanos"]] {
                        // Ordinary ingestion emits milliseconds; test it against the
                        // matching existing logical table without auto-creation.
                        if !with_metric_engine && names.contains(&"nanos") {
                            continue;
                        }
                        let outcome = instance
                            .metrics(
                                metric_request(
                                    names
                                        .iter()
                                        .map(|name| gauge_metric(name, &points))
                                        .collect(),
                                ),
                                Arc::new(ctx.clone()),
                            )
                            .await
                            .unwrap();
                        assert_eq!(
                            (outcome.accepted_data_points, outcome.rejected_data_points),
                            (names.len() as i64 * 2, 0)
                        );
                    }
                }
            }
        }
        for (name, unit, expected) in [
            ("greptime_physical_table", TimeUnit::Millisecond, 60_000),
            ("custom_physical", TimeUnit::Nanosecond, 60_000_000_123),
        ] {
            let output = instance.do_query(&format!("SELECT greptime_timestamp, host, greptime_value FROM {name} ORDER BY greptime_timestamp"), Arc::new(ctx.clone())).await.remove(0).unwrap();
            let OutputData::Stream(stream) = output.data else {
                unreachable!()
            };
            let batches = RecordBatches::try_collect(stream).await.unwrap().take();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                2
            );
            assert!(batches[0].column(1).is_null(0));
            assert_eq!(
                batches[0]
                    .column(2)
                    .as_primitive::<datatypes::arrow::datatypes::Float64Type>()
                    .value(0),
                10.0
            );
            let timestamp = batches[0].column(0);
            assert_eq!(timestamp.data_type(), &DataType::Timestamp(unit, None));
            let actual = if unit == TimeUnit::Millisecond {
                timestamp
                    .as_primitive::<TimestampMillisecondType>()
                    .value(0)
            } else {
                timestamp.as_primitive::<TimestampNanosecondType>().value(0)
            };
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_preflight_shared_physical_and_fanout_conflicts() {
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_physical_conflicts")
            .build()
            .await;
        let instance = standalone.fe_instance();
        for reverse in [false, true] {
            let db = format!("physical_{reverse}");
            let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, &db);
            ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
                with_metric_engine: true,
                ..Default::default()
            }));
            let ctx = Arc::new(ctx);
            instance
                .do_query(&format!("CREATE DATABASE {db}"), ctx.clone())
                .await
                .remove(0)
                .unwrap();
            let mut scalar = gauge_metric("scalar", &[(60_000_000_000, 1.0, None)]);
            if let Some(metric::Data::Gauge(data)) = scalar.data.as_mut() {
                data.data_points[0]
                    .attributes
                    .push(keyvalue("greptime_native_histogram", "tag"));
            }
            let mut metrics = vec![
                scalar,
                native_metric("native", 0),
                gauge_metric("independent", &[(60_000_000_000, 7.0, None)]),
            ];
            if reverse {
                metrics.reverse();
            }
            let outcome = instance
                .metrics(metric_request(metrics.clone()), ctx.clone())
                .await
                .unwrap();
            assert_eq!(
                (outcome.accepted_data_points, outcome.rejected_data_points),
                (1, 2)
            );
            assert_trace_v2_query(instance,&format!("SELECT COUNT(*)=0 FROM information_schema.tables WHERE table_schema='{db}' AND table_name IN ('scalar','native')"),ctx.clone()).await.unwrap();
            assert_trace_v2_query(instance,&format!("SELECT COUNT(*)=0 FROM information_schema.columns WHERE table_schema='{db}' AND column_name='greptime_native_histogram'"),ctx.clone()).await.unwrap();
            // The existing physical Float64 definition is authoritative, including
            // for existing-column writes that propose no ALTER.
            instance.do_query("CREATE TABLE bad_type (greptime_timestamp TIMESTAMP(3) TIME INDEX, greptime_value BIGINT) WITH ('otlp_metric_compat'='prom')",ctx.clone()).await.remove(0).unwrap();
            let outcome = instance
                .metrics(
                    metric_request(vec![
                        gauge_metric("bad_type", &[(60_000_000_000, 3.0, None)]),
                        gauge_metric("independent", &[(120_000_000_000, 9.0, None)]),
                    ]),
                    ctx.clone(),
                )
                .await
                .unwrap();
            assert_eq!(
                (outcome.accepted_data_points, outcome.rejected_data_points),
                (1, 1)
            );
            assert_trace_v2_query(instance, "SELECT COUNT(*)=0 FROM bad_type", ctx.clone())
                .await
                .unwrap();
            let compatible_name = if reverse { "scalar" } else { "native" };
            let compatible = metrics
                .iter()
                .find(|m| m.name == compatible_name)
                .unwrap()
                .clone();
            let outcome = instance
                .metrics(metric_request(vec![compatible]), ctx.clone())
                .await
                .unwrap();
            assert_eq!(
                (outcome.accepted_data_points, outcome.rejected_data_points),
                (1, 0)
            );
            if !reverse {
                // An existing scalar logical table cannot ALTER a shared Struct
                // field into a string tag either.
                metrics
                    .iter_mut()
                    .find(|m| m.name == "scalar")
                    .unwrap()
                    .name = "independent".into();
            }
            let outcome = instance
                .metrics(metric_request(metrics), ctx.clone())
                .await
                .unwrap();
            assert_eq!(
                (outcome.accepted_data_points, outcome.rejected_data_points),
                (2, 1)
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_metrics_ignore_compat_option() {
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_compat_metadata")
            .build()
            .await;
        let instance = standalone.fe_instance();
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "public"));
        let tables = [
            ("without_option", ""),
            ("with_prom_option", "WITH ('otlp_metric_compat'='prom')"),
            ("with_old_option", "WITH ('otlp_metric_compat'='legacy')"),
        ];
        for (name, options) in tables {
            instance
                .do_query(
                    &format!("CREATE TABLE {name} (greptime_timestamp TIMESTAMP(3) TIME INDEX, greptime_value DOUBLE) {options}"),
                    ctx.clone(),
                )
                .await
                .remove(0)
                .unwrap();
        }
        // Neither absent nor retained option values select a different conversion
        // or make tables incompatible within the same request.
        for timestamp in [60_000_000_123, 120_000_000_456] {
            let outcome = instance
                .metrics(
                    metric_request(
                        tables
                            .iter()
                            .map(|(name, _)| gauge_metric(name, &[(timestamp, 7.0, None)]))
                            .collect(),
                    ),
                    ctx.clone(),
                )
                .await
                .unwrap();
            assert_eq!(
                (outcome.accepted_data_points, outcome.rejected_data_points),
                (3, 0)
            );
        }
        for (name, _) in tables {
            assert_trace_v2_query(
                instance,
                &format!("SELECT COUNT(*)=2 AND MIN(greptime_value)=7 AND MIN(greptime_timestamp)='1970-01-01 00:01:00'::TIMESTAMP FROM {name}"),
                ctx.clone(),
            )
            .await
            .unwrap();
        }
    }

    async fn native_histogram_remote_read(
        instance: &Arc<Instance>,
        table: &str,
        ctx: Arc<QueryContext>,
    ) -> Vec<api::prom_store::remote::TimeSeries> {
        use api::prom_store::remote::{LabelMatcher, Query, ReadRequest, ReadResponse};
        use prost::Message;
        use servers::query_handler::PromStoreProtocolHandler;

        let response = instance
            .read(
                ReadRequest {
                    queries: vec![Query {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: 200_000,
                        matchers: vec![LabelMatcher {
                            name: "__name__".to_string(),
                            value: table.to_string(),
                            r#type: 0,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ctx,
            )
            .await
            .unwrap();
        let body = servers::prom_store::snappy_decompress(&response.body).unwrap();
        ReadResponse::decode(body.as_slice())
            .unwrap()
            .results
            .remove(0)
            .timeseries
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_delta_exponential_storage_precision_and_replacement() {
        use datatypes::arrow::datatypes::Int64Type;
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_histogram_precision")
            .build()
            .await;
        let instance = standalone.fe_instance();
        for precision in [0, 3, 6, 9] {
            let db = format!("native_{precision}");
            let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, &db);
            ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
                with_metric_engine: true,
                ..Default::default()
            }));
            let ctx = Arc::new(ctx);
            for sql in [
                format!("CREATE DATABASE {db}"),
                format!(
                    "CREATE TABLE greptime_physical_table (greptime_timestamp TIMESTAMP({precision}) TIME INDEX, greptime_value DOUBLE) ENGINE=metric WITH ('physical_metric_table'='true')"
                ),
            ] {
                instance
                    .do_query(&sql, ctx.clone())
                    .await
                    .remove(0)
                    .unwrap();
            }
            let large = (1u64 << 53) + 1;
            // Replace the sample after timestamp truncation, then replay it exactly.
            for (round, count) in [(0, 10), (1, large), (1, large)] {
                let mut metric = native_metric("interval", 0);
                if let Some(metric::Data::ExponentialHistogram(data)) = metric.data.as_mut() {
                    let point = &mut data.data_points[0];
                    point.count = count;
                    point.zero_count = count;
                    point.start_time_unix_nano = round as u64 * 1_000_000_000;
                    point.time_unix_nano =
                        60_000_000_000 + if precision == 9 { 1 } else { 1 + round as u64 };
                }
                let outcome = instance
                    .metrics(metric_request(vec![metric]), ctx.clone())
                    .await
                    .unwrap();
                assert_eq!(
                    (outcome.accepted_data_points, outcome.rejected_data_points),
                    (1, 0)
                );
                for flushed in [false, true] {
                    if flushed {
                        instance
                            .do_query("ADMIN FLUSH_TABLE('interval')", ctx.clone())
                            .await
                            .remove(0)
                            .unwrap();
                    }
                    let series =
                        native_histogram_remote_read(instance, "interval", ctx.clone()).await;
                    assert_eq!(series.len(), 1);
                    assert!(series[0].labels.iter().any(|l| l.name == "otlp_aggregation_temporality" && l.value == "delta"));
                    let histogram = &series[0].histograms[0];
                    assert_eq!(histogram.timestamp, 60_000);
                    assert_eq!(
                        histogram.count,
                        Some(api::prom_store::remote::histogram::Count::CountInt(count))
                    );
                    assert_eq!(
                        histogram.zero_count,
                        Some(api::prom_store::remote::histogram::ZeroCount::ZeroCountInt(
                            count
                        ))
                    );
                    assert_eq!(histogram.reset_hint, 3);
                    assert_eq!(histogram.sum, 0.0);
                    let output = instance
                        .do_query(
                            "SELECT greptime_timestamp,greptime_native_histogram FROM interval",
                            ctx.clone(),
                        )
                        .await
                        .remove(0)
                        .unwrap();
                    let OutputData::Stream(stream) = output.data else {
                        unreachable!()
                    };
                    let batches = RecordBatches::try_collect(stream).await.unwrap().take();
                    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
                    let counts = batches[0]
                        .column(1)
                        .as_struct()
                        .column_by_name("count_i64")
                        .unwrap()
                        .as_primitive::<Int64Type>();
                    assert_eq!(counts.value(0), count as i64);
                    assert_eq!(
                        batches[0].column(0).data_type(),
                        &DataType::Timestamp(
                            match precision {
                                0 => TimeUnit::Second,
                                3 => TimeUnit::Millisecond,
                                6 => TimeUnit::Microsecond,
                                _ => TimeUnit::Nanosecond,
                            },
                            None
                        )
                    );
                }
            }
            if precision >= 6 {
                let mut metric = native_metric("interval", 0);
                if let Some(metric::Data::ExponentialHistogram(data)) = metric.data.as_mut() {
                    data.data_points[0].time_unix_nano += if precision == 6 { 1_000 } else { 2 };
                }
                instance
                    .metrics(metric_request(vec![metric]), ctx.clone())
                    .await
                    .unwrap();
                assert_trace_v2_query(instance, "SELECT COUNT(*)=2 FROM interval", ctx.clone())
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_otlp_preflight_disabled_delta_and_schema_precedence() {
        let standalone = GreptimeDbStandaloneBuilder::new("otlp_disabled_delta")
            .build()
            .await;
        let instance = standalone.fe_instance();
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        }));
        let ctx = Arc::new(ctx);
        let mut cumulative = native_metric("histogram", 0);
        if let Some(metric::Data::ExponentialHistogram(data)) = cumulative.data.as_mut() {
            data.aggregation_temporality = AggregationTemporality::Cumulative as i32;
        }
        instance
            .metrics(
                metric_request(vec![
                    cumulative,
                    gauge_metric("independent", &[(60_000_000_000, 7.0, None)]),
                ]),
                ctx.clone(),
            )
            .await
            .unwrap();
        let mut fixed = ctx.fork();
        fixed.set_extension("auto_create_table", "false");
        let outcome = instance
            .metrics(
                metric_request(vec![
                    native_metric("histogram", 0),
                    gauge_metric("independent", &[(120_000_000_000, 9.0, None)]),
                ]),
                Arc::new(fixed),
            )
            .await
            .unwrap();
        assert_eq!(
            (outcome.accepted_data_points, outcome.rejected_data_points),
            (1, 1),
            "{outcome:?}"
        );
        assert_trace_v2_query(instance,"SELECT COUNT(*)=0 FROM information_schema.columns WHERE column_name='otlp_aggregation_temporality'",ctx.clone()).await.unwrap();
        for reverse in [false, true] {
            let prefix = if reverse { "second" } else { "first" };
            instance.do_query(&format!("CREATE TABLE {prefix}_sum (greptime_timestamp TIMESTAMP(3) TIME INDEX, greptime_value BIGINT) WITH ('otlp_metric_compat'='prom')"),ctx.clone()).await.remove(0).unwrap();
            let mut explicit = build_histogram_request(
                prefix,
                vec![HistogramDataPoint {
                    time_unix_nano: 60_000_000_000,
                    count: 10,
                    bucket_counts: vec![10],
                    sum: Some(10.0),
                    ..Default::default()
                }],
            )
            .resource_metrics
            .remove(0)
            .scope_metrics
            .remove(0)
            .metrics
            .remove(0);
            explicit.name = prefix.into();
            let mut metrics = vec![explicit, native_metric(&format!("{prefix}_bucket"), 0)];
            if reverse {
                metrics.reverse();
            }
            let outcome = instance
                .metrics(metric_request(metrics), ctx.clone())
                .await
                .unwrap();
            assert_eq!(
                (outcome.accepted_data_points, outcome.rejected_data_points),
                (1, 1),
                "{outcome:?}"
            );
            assert_trace_v2_query(
                instance,
                &format!("SELECT COUNT(*)=1 FROM {prefix}_bucket"),
                ctx.clone(),
            )
            .await
            .unwrap();
            assert_trace_v2_query(instance,&format!("SELECT COUNT(*)=0 FROM information_schema.tables WHERE table_name='{prefix}_count'"),ctx.clone()).await.unwrap();
        }
    }

    fn build_sum_request(
        name: &str,
        temporality: AggregationTemporality,
        points: &[(u64, i64)],
    ) -> ExportMetricsServiceRequest {
        let data_points = points
            .iter()
            .map(|(seconds, value)| NumberDataPoint {
                attributes: vec![keyvalue("stream", "same")],
                time_unix_nano: *seconds * 1_000_000_000,
                value: Some(Value::AsInt(*value)),
                ..Default::default()
            })
            .collect();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: name.to_string(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points,
                            aggregation_temporality: temporality as i32,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn build_histogram_request(
        name: &str,
        points: Vec<HistogramDataPoint>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: name.to_string(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: points,
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn keyvalue(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Val::StringValue(value.into())),
            }),
        }
    }
}
