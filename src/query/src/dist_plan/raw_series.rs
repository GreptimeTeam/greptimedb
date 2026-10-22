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

use crate::dist_plan::merge_scan::NativeRawSource;
use crate::error::Result;
use common_query::request::{QueryRequest, RegionReadKind};
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use greptime_proto::v1::region::RegionRequestHeader;
use promql::source::merge_ordered_raw_series;

/// Reads the frozen regions as raw frames and merges their ordered raw-series streams.
///
/// No schema is taken here: a raw series frame declares its own fields, label layout and
/// ordering, and [`unpack_raw_series`] decodes it from that declaration. A caller that needs to
/// check the fields are the ones it planned for does so where the expectation exists - the
/// consumer that binds a tree to an input schema - rather than here, where the only available
/// answer would come from the same plan that produced the frame.
#[allow(dead_code)] // The native PromQL caller is not wired in yet.
pub(crate) async fn merged_raw_series(
    source: NativeRawSource<'_>,
) -> Result<store_api::region_engine::SendableRawSeriesStream> {
    let datafusion_expr::LogicalPlan::TableScan(scan) = source.plan else {
        return Err(DataFusionError::Plan(
            "raw series request requires a bare TableScan plan".into(),
        )
        .into());
    };
    if scan.fetch.is_some() {
        return Err(DataFusionError::Plan(
            "raw series request cannot use a TableScan with fetch".into(),
        )
        .into());
    }
    if source.route.targets.is_empty() {
        return Err(DataFusionError::Plan("raw series request has zero targets".into()).into());
    }
    let query_ctx = source.query_ctx;
    let region_ctx = query_ctx.fork();
    let dbname = format!(
        "{}.{}",
        query_ctx.current_catalog(),
        query_ctx.current_schema()
    );
    let header = Some(RegionRequestHeader {
        tracing_context: Default::default(),
        dbname,
        query_context: Some((&region_ctx).into()),
    });
    let mut inputs = Vec::with_capacity(source.route.targets.len());
    for (region_id, target) in &source.route.targets {
        let stream = source
            .region_query_handler
            .do_get(
                target,
                QueryRequest {
                    header: header.clone(),
                    region_id: *region_id,
                    plan: source.plan.clone(),
                    read_kind: RegionReadKind::RawSeries,
                },
            )
            .await?;
        let adapted: SendableRecordBatchStream = Box::pin(DfRecordBatchStreamAdapter::new(stream));
        let raw = common_query::series_frame::unpack_raw_series(adapted, None)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        inputs.push(raw);
    }
    let merged =
        merge_ordered_raw_series(inputs).map_err(|e| DataFusionError::External(e.into()))?;
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dist_plan::planner::CaptureNativeRawRoute;
    use crate::region_query::{RegionQueryHandler, RegionQueryTarget};
    use arrow::buffer::{Buffer, ScalarBuffer};
    use async_trait::async_trait;
    use common_error::ext::{BoxedError, PlainError};
    use common_meta::peer::Peer;
    use common_recordbatch::adapter::RecordBatchMetrics;
    use common_recordbatch::{RecordBatch, RecordBatchStream};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::logical_expr::LogicalTableSource;
    use datafusion_expr::LogicalPlanBuilder;
    use datatypes::arrow::array::Float64Array;
    use datatypes::schema::Schema;
    use datatypes::series::frame::{
        LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding, LabelDecodeSpec,
        LabelDecodeTag, RawLabelMode, pack_raw_series_chunk,
        raw_series_frame_schema_with_label_decode,
    };
    use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
    use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
    use futures_util::{Stream, TryStreamExt};
    use session::ReadPreference;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use store_api::storage::RegionId;

    struct InputStream {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        fail_after_batches: bool,
        failed: bool,
    }
    impl Stream for InputStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if !self.batches.is_empty() {
                return Poll::Ready(Some(Ok(self.batches.remove(0))));
            }
            if self.fail_after_batches && !self.failed {
                self.failed = true;
                return Poll::Ready(Some(Err(common_recordbatch::error::Error::External {
                    source: BoxedError::new(PlainError::new(
                        "stream failed after batch".to_string(),
                        common_error::status_code::StatusCode::Unexpected,
                    )),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })));
            }
            Poll::Ready(None)
        }
    }
    impl RecordBatchStream for InputStream {
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
        fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
            None
        }
        fn metrics(&self) -> Option<RecordBatchMetrics> {
            None
        }
    }

    struct Handler {
        batches: Vec<RecordBatch>,
    }

    struct PerTargetHandler {
        batches: Vec<Vec<RecordBatch>>,
        targets: Vec<RegionQueryTarget>,
        fail_after_batches: bool,
    }
    #[async_trait]
    impl RegionQueryHandler for Handler {
        async fn select_target(
            &self,
            _: ReadPreference,
            _: RegionId,
        ) -> crate::error::Result<RegionQueryTarget> {
            Ok(target())
        }
        async fn do_get(
            &self,
            _: &RegionQueryTarget,
            request: QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            assert_eq!(request.read_kind, RegionReadKind::RawSeries);
            let schema = self.batches[0].schema.clone();
            Ok(Box::pin(InputStream {
                schema,
                batches: self.batches.clone(),
                fail_after_batches: false,
                failed: false,
            }))
        }
        async fn handle_remote_dyn_filter_update(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            Ok(())
        }
        async fn handle_remote_dyn_filter_unregister(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            Ok(())
        }
    }
    #[async_trait]
    impl RegionQueryHandler for PerTargetHandler {
        async fn select_target(
            &self,
            _: ReadPreference,
            _: RegionId,
        ) -> crate::error::Result<RegionQueryTarget> {
            panic!("raw-series reads must use the captured route target")
        }
        async fn do_get(
            &self,
            target: &RegionQueryTarget,
            request: QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            assert_eq!(request.read_kind, RegionReadKind::RawSeries);
            let index = request.region_id.region_number() as usize - 1;
            let batches = self.batches.get(index).expect("route target has batches");
            let schema = batches[0].schema.clone();
            assert_eq!(target, &self.targets[index]);
            Ok(Box::pin(InputStream {
                schema,
                batches: batches.clone(),
                fail_after_batches: self.fail_after_batches,
                failed: false,
            }))
        }
        async fn handle_remote_dyn_filter_update(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            Ok(())
        }
        async fn handle_remote_dyn_filter_unregister(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl RegionQueryHandler for FailingHandler {
        async fn select_target(
            &self,
            _: ReadPreference,
            _: RegionId,
        ) -> crate::error::Result<RegionQueryTarget> {
            Ok(target())
        }
        async fn do_get(
            &self,
            _: &RegionQueryTarget,
            _: QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            Err(crate::error::Error::QueryPlan {
                source: BoxedError::new(PlainError::new(
                    "region refused the read".to_string(),
                    common_error::status_code::StatusCode::Unexpected,
                )),
                location: snafu::Location::new(file!(), line!(), column!()),
            })
        }
        async fn handle_remote_dyn_filter_update(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            Ok(())
        }
        async fn handle_remote_dyn_filter_unregister(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            Ok(())
        }
    }

    struct FailingHandler;

    fn target() -> RegionQueryTarget {
        target_with_peer(1)
    }
    fn target_with_peer(id: u64) -> RegionQueryTarget {
        RegionQueryTarget::new(Peer {
            id,
            addr: format!("localhost:{id}"),
        })
    }
    fn plan() -> datafusion_expr::LogicalPlan {
        let src = Arc::new(LogicalTableSource::new(Arc::new(ArrowSchema::new(vec![
            Field::new("x", DataType::Int64, true),
        ]))));
        LogicalPlanBuilder::scan("t", src, None)
            .unwrap()
            .build()
            .unwrap()
    }
    fn source<'a>(
        handler: &'a crate::region_query::RegionQueryHandlerRef,
        p: &'a datafusion_expr::LogicalPlan,
        route: &'a CaptureNativeRawRoute,
        table: &'a table::table_name::TableName,
        ctx: &'a session::context::QueryContextRef,
    ) -> NativeRawSource<'a> {
        NativeRawSource {
            route,
            plan: p,
            table,
            region_query_handler: handler,
            query_ctx: ctx,
        }
    }
    fn frame(key: &[u8], ts: &[i64], last: bool) -> Vec<RecordBatch> {
        let schema = SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: None,
        };
        let chunk = RawSeriesChunk::try_new(
            &schema,
            Buffer::from([key, &[0, 0]].concat()),
            None,
            RawTimestamps::try_new(ScalarBuffer::from(ts.to_vec())).unwrap(),
            SeriesFields::one(Samples::from_float(Float64Array::from(
                ts.iter().map(|x| Some(*x as f64)).collect::<Vec<_>>(),
            ))),
            last,
        )
        .unwrap();
        let description = LabelDecodeSpec {
            version: LABEL_DECODE_VERSION,
            metric_name: "cpu".to_string(),
            encoding: LabelDecodeEncoding::Dense,
            primary_key: vec![LabelDecodeColumn {
                column_id: 1,
                name: "host".to_string(),
                physical_type: datatypes::prelude::ConcreteDataType::string_datatype(),
            }],
            logical_tags: vec![LabelDecodeTag {
                column_id: 1,
                name: "host".to_string(),
            }],
        };
        let frame_schema = raw_series_frame_schema_with_label_decode(
            &schema,
            RawLabelMode::Key,
            Some(&description),
        )
        .unwrap();
        let frames = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk).unwrap();
        let mut metadata = frame_schema.metadata().clone();
        metadata.insert(
            "greptime.series.ordering".to_string(),
            "read_key_then_time".to_string(),
        );
        let arrow = Arc::new(ArrowSchema::new_with_metadata(
            frame_schema.fields().clone(),
            metadata,
        ));
        let schema = Arc::new(Schema::try_from(arrow.clone()).unwrap());
        frames
            .into_iter()
            .map(|batch| {
                let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
                    arrow.clone(),
                    batch.columns().to_vec(),
                )
                .unwrap();
                RecordBatch::from_df_record_batch(schema.clone(), batch)
            })
            .collect()
    }
    fn route(n: u32) -> CaptureNativeRawRoute {
        CaptureNativeRawRoute {
            targets: vec![(RegionId::new(1, n), target())],
        }
    }

    #[tokio::test]
    async fn one_target_chunks_remain_contiguous_and_ordered() {
        let batches = [frame(b"a", &[1], false), frame(b"a", &[2], true)].concat();
        let handler: crate::region_query::RegionQueryHandlerRef = Arc::new(Handler { batches });
        let p = plan();
        let r = route(1);
        let table = table::table_name::TableName::new("catalog", "schema", "table");
        let ctx = session::context::QueryContext::arc();
        let chunks = merged_raw_series(source(&handler, &p, &r, &table, &ctx))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].time.timestamp(0).unwrap() < chunks[1].time.timestamp(0).unwrap());
    }

    #[tokio::test]
    async fn several_targets_merge_without_loss() {
        // Two regions, one series each: the merged stream must carry both, once each.
        let a = [frame(b"a", &[1], false), frame(b"a", &[2], true)].concat();
        let b = [frame(b"b", &[1], false), frame(b"b", &[2], true)].concat();
        let targets = vec![target_with_peer(2), target_with_peer(1)];
        let handler: crate::region_query::RegionQueryHandlerRef = Arc::new(PerTargetHandler {
            batches: vec![b, a],
            targets: targets.clone(),
            fail_after_batches: false,
        });
        let p = plan();
        let r = CaptureNativeRawRoute {
            targets: vec![
                (RegionId::new(1, 1), targets[0].clone()),
                (RegionId::new(1, 2), targets[1].clone()),
            ],
        };
        let table = table::table_name::TableName::new("catalog", "schema", "table");
        let ctx = session::context::QueryContext::arc();
        let chunks = merged_raw_series(source(&handler, &p, &r, &table, &ctx))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let keys: Vec<&[u8]> = chunks.iter().map(|c| c.primary_key.as_slice()).collect();
        assert_eq!(
            keys,
            vec![b"a\0\0".as_slice(), b"a\0\0", b"b\0\0", b"b\0\0"]
        );
        let timestamps: Vec<i64> = chunks
            .iter()
            .map(|chunk| chunk.time.timestamp(0).unwrap())
            .collect();
        assert_eq!(timestamps, vec![1, 2, 1, 2]);
        let values: Vec<f64> = chunks
            .iter()
            .map(|chunk| match chunk.fields.get(0).unwrap().sample(0) {
                datatypes::series::samples::SampleRef::Float(value) => value,
                _ => panic!("expected float sample"),
            })
            .collect();
        assert_eq!(values, vec![1.0, 2.0, 1.0, 2.0]);
        let is_last: Vec<bool> = chunks.iter().map(|chunk| chunk.is_last).collect();
        assert_eq!(is_last, vec![false, true, false, true]);
    }

    #[tokio::test]
    async fn a_stream_error_after_a_batch_propagates() {
        let target = target_with_peer(7);
        let handler: crate::region_query::RegionQueryHandlerRef = Arc::new(PerTargetHandler {
            batches: vec![frame(b"a", &[1], false)],
            targets: vec![target.clone()],
            fail_after_batches: true,
        });
        let p = plan();
        let r = CaptureNativeRawRoute {
            targets: vec![(RegionId::new(1, 1), target)],
        };
        let table = table::table_name::TableName::new("catalog", "schema", "table");
        let ctx = session::context::QueryContext::arc();
        let error = merged_raw_series(source(&handler, &p, &r, &table, &ctx))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .expect_err("stream failure must propagate after a batch");
        let mut cause = std::error::Error::source(&error);
        while let Some(error) = cause {
            if error.to_string().contains("stream failed after batch") {
                return;
            }
            cause = error.source();
        }
        panic!("original stream error was not preserved: {error}");
    }

    #[tokio::test]
    async fn a_plan_that_is_not_a_bare_scan_is_rejected() {
        let handler: crate::region_query::RegionQueryHandlerRef =
            Arc::new(Handler { batches: vec![] });
        let inner = plan();
        let filtered = datafusion_expr::LogicalPlan::Filter(
            datafusion_expr::Filter::try_new(datafusion_expr::lit(true), Arc::new(inner)).unwrap(),
        );
        let r = route(1);
        let table = table::table_name::TableName::new("catalog", "schema", "table");
        let ctx = session::context::QueryContext::arc();
        let error = merged_raw_series(source(&handler, &filtered, &r, &table, &ctx))
            .await
            .err()
            .expect("expected plan error");
        assert!(
            error
                .to_string()
                .contains("raw series request requires a bare TableScan plan"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_scan_with_fetch_is_rejected() {
        let handler: crate::region_query::RegionQueryHandlerRef =
            Arc::new(Handler { batches: vec![] });
        let mut p = plan();
        if let datafusion_expr::LogicalPlan::TableScan(scan) = &mut p {
            scan.fetch = Some(1);
        } else {
            panic!("the fixture is a table scan");
        }
        let r = route(1);
        let table = table::table_name::TableName::new("catalog", "schema", "table");
        let ctx = session::context::QueryContext::arc();
        let error = merged_raw_series(source(&handler, &p, &r, &table, &ctx))
            .await
            .err()
            .expect("expected raw series error");
        assert!(
            error
                .to_string()
                .contains("raw series request cannot use a TableScan with fetch"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_region_error_propagates() {
        let handler: crate::region_query::RegionQueryHandlerRef = Arc::new(FailingHandler);
        let p = plan();
        let r = route(1);
        let table = table::table_name::TableName::new("catalog", "schema", "table");
        let ctx = session::context::QueryContext::arc();
        let result = merged_raw_series(source(&handler, &p, &r, &table, &ctx)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn zero_targets_rejected() {
        let handler: crate::region_query::RegionQueryHandlerRef =
            Arc::new(Handler { batches: vec![] });
        let p = plan();
        let route = CaptureNativeRawRoute { targets: vec![] };
        let table = table::table_name::TableName::new("catalog", "schema", "table");
        let ctx = session::context::QueryContext::arc();
        let error = merged_raw_series(source(&handler, &p, &route, &table, &ctx))
            .await
            .err()
            .expect("expected zero-target error");
        assert!(
            error
                .to_string()
                .contains("raw series request has zero targets")
        );
    }
}
