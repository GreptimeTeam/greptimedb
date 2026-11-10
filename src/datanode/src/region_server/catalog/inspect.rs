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

//! Streaming providers for internal inspection tables.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use futures::StreamExt;
use mito2::engine::MitoEngine;
use store_api::compaction_task::{CompactionTaskBatchBuilder, CompactionTaskEntry};
use store_api::sst_entry::{ManifestSstBatchBuilder, ManifestSstEntry};

/// Supplies batches for an internal inspection table. Implementations may prune
/// conservatively; DataFusion retains complete residual predicates.
pub(crate) trait InspectSource: std::fmt::Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;

    /// Creates a fresh scan at execution time, including on repeated execution.
    /// Projection includes columns required by residual predicates.
    fn scan(&self, request: &InspectScan, ctx: Arc<TaskContext>) -> SendableRecordBatchStream;
}

/// Parameters shared by inspection sources. Limits are enforced by the physical
/// plan, after residual filters, rather than by individual sources.
#[derive(Debug, Clone)]
pub(crate) struct InspectScan {
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    batch_size: usize,
}

/// Adapts a lazy inspection source to DataFusion's streaming execution plan.
#[derive(Debug)]
pub(crate) struct InspectTable {
    source: Arc<dyn InspectSource>,
}

impl InspectTable {
    /// Wraps a source without opening a scan.
    pub(crate) fn new(source: Arc<dyn InspectSource>) -> Self {
        Self { source }
    }
}

#[async_trait::async_trait]
impl TableProvider for InspectTable {
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // An inexact predicate must be checked before selecting a limited prefix.
        let limit = limit.filter(|_| filters.is_empty());
        let configured_batch_size: usize = state.config_options().execution.batch_size.into();
        let schema = match projection {
            Some(projection) => Arc::new(self.schema().project(projection)?),
            None => self.schema(),
        };
        let partition = InspectPartition {
            source: self.source.clone(),
            request: InspectScan {
                schema: schema.clone(),
                projection: projection.cloned(),
                filters: filters.to_vec(),
                batch_size: limit
                    .unwrap_or(configured_batch_size)
                    .min(configured_batch_size)
                    .max(1),
            },
        };
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![Arc::new(partition)],
            None,
            None,
            false,
            limit,
        )?))
    }
}

/// Reads manifest snapshots without materializing the full SST inventory.
pub(crate) struct ManifestSstSource {
    engine: MitoEngine,
}

impl ManifestSstSource {
    /// Creates a source backed by the engine's open-region manifests.
    pub(crate) fn new(engine: MitoEngine) -> Self {
        Self { engine }
    }
}

impl std::fmt::Debug for ManifestSstSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestSstSource").finish_non_exhaustive()
    }
}

impl InspectSource for ManifestSstSource {
    fn schema(&self) -> SchemaRef {
        ManifestSstEntry::schema().arrow_schema().clone()
    }

    fn scan(&self, request: &InspectScan, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let builder = ManifestSstBatchBuilder::new(request.schema.clone());
        let stream = self
            .engine
            .scan_ssts_from_manifest_projected(
                &request.filters,
                request.batch_size,
                request.projection.as_deref(),
            )
            .map(move |entries| builder.build(&entries).map_err(DataFusionError::from));
        Box::pin(RecordBatchStreamAdapter::new(
            request.schema.clone(),
            stream,
        ))
    }
}

/// Reads live compaction state only when a scan is executed.
pub(crate) struct CompactionTasksSource {
    engine: MitoEngine,
}

impl CompactionTasksSource {
    /// Creates a source backed by the engine's live compaction cycles.
    pub(crate) fn new(engine: MitoEngine) -> Self {
        Self { engine }
    }
}

impl std::fmt::Debug for CompactionTasksSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionTasksSource")
            .finish_non_exhaustive()
    }
}

impl InspectSource for CompactionTasksSource {
    fn schema(&self) -> SchemaRef {
        CompactionTaskEntry::schema().arrow_schema().clone()
    }

    fn scan(&self, request: &InspectScan, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let engine = self.engine.clone();
        let request = request.clone();
        let schema = request.schema.clone();
        let builder = CompactionTaskBatchBuilder::new(schema.clone());
        let stream = async_stream::try_stream! {
            let entries = engine
                .inspect_compaction_tasks(&request.filters)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            for entries in entries.chunks(request.batch_size) {
                yield builder.build(entries)?;
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

#[derive(Debug)]
struct InspectPartition {
    source: Arc<dyn InspectSource>,
    request: InspectScan,
}

impl PartitionStream for InspectPartition {
    fn schema(&self) -> &SchemaRef {
        &self.request.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.source.scan(&self.request, ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[derive(Debug, Default)]
    struct CountingSource {
        scans: Arc<AtomicUsize>,
        rows: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    struct ScanGuard(Arc<AtomicUsize>);

    impl Drop for ScanGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl InspectSource for CountingSource {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Int32, false),
            ]))
        }

        fn scan(&self, request: &InspectScan, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            self.scans.fetch_add(1, Ordering::SeqCst);
            let guard = ScanGuard(self.drops.clone());
            let rows = self.rows.clone();
            let schema = request.schema.clone();
            let projection = request.projection.clone().unwrap_or_else(|| vec![0, 1]);
            // Deliberately ignore source predicates: the provider must preserve
            // residual filtering before limit, including hidden predicate columns.
            let stream = futures::stream::iter(0..100).map(move |id| {
                let _guard = &guard;
                rows.fetch_add(1, Ordering::SeqCst);
                let columns: Vec<ArrayRef> = projection
                    .iter()
                    .map(|column| {
                        Arc::new(Int32Array::from(vec![if *column == 0 {
                            id
                        } else {
                            id * 10
                        }])) as ArrayRef
                    })
                    .collect();
                RecordBatch::try_new(schema.clone(), columns).map_err(DataFusionError::from)
            });
            Box::pin(RecordBatchStreamAdapter::new(
                request.schema.clone(),
                stream,
            ))
        }
    }

    #[tokio::test]
    async fn test_lazy_repeatable_scan_and_limit_cancellation() {
        let source = Arc::new(CountingSource::default());
        let ctx = SessionContext::new();
        ctx.register_table("inspection", Arc::new(InspectTable::new(source.clone())))
            .unwrap();
        let frame = ctx
            .sql("SELECT value FROM inspection LIMIT 2")
            .await
            .unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        assert_eq!(source.scans.load(Ordering::SeqCst), 0);
        assert_eq!(source.rows.load(Ordering::SeqCst), 0);
        for run in 1..=2 {
            let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
                .await
                .unwrap();
            let values = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, vec![0, 10]);
            assert_eq!(source.scans.load(Ordering::SeqCst), run);
            assert_eq!(source.rows.load(Ordering::SeqCst), run * 2);
            assert_eq!(source.drops.load(Ordering::SeqCst), run);
        }
    }

    #[tokio::test]
    async fn test_hidden_residual_filter_before_limit() {
        let source = Arc::new(CountingSource::default());
        let ctx = SessionContext::new();
        ctx.register_table("inspection", Arc::new(InspectTable::new(source.clone())))
            .unwrap();
        let batches = ctx
            .sql("SELECT value FROM inspection WHERE id >= 8 LIMIT 2")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let values = batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.num_columns(), 1);
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        // No ORDER BY: repartitioning may choose any two qualifying rows.
        assert_eq!(values.len(), 2);
        assert!(
            values
                .iter()
                .all(|value| (80..1000).contains(value) && value % 10 == 0)
        );
        assert_eq!(source.scans.load(Ordering::SeqCst), 1);
        assert_eq!(source.drops.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn test_manifest_optimized_sql_pruning() {
        use std::sync::Mutex;

        use datafusion_expr::{col, lit};
        use mito2::config::MitoConfig;
        use mito2::manifest::action::RegionEdit;
        use mito2::sst::file::FileMeta;
        use mito2::test_util::{CreateRequestBuilder, TestEnv};
        use store_api::region_engine::RegionEngine;
        use store_api::region_request::RegionRequest;
        use store_api::storage::{FileId, RegionId};

        #[derive(Debug)]
        struct RecordingSource {
            inner: ManifestSstSource,
            filters: Mutex<Vec<Expr>>,
            candidates: Arc<AtomicUsize>,
        }

        impl InspectSource for RecordingSource {
            fn schema(&self) -> SchemaRef {
                self.inner.schema()
            }

            fn scan(
                &self,
                request: &InspectScan,
                ctx: Arc<TaskContext>,
            ) -> SendableRecordBatchStream {
                *self.filters.lock().unwrap() = request.filters.clone();
                let candidates = self.candidates.clone();
                let stream = self.inner.scan(request, ctx).map(move |batch| {
                    if let Ok(batch) = &batch {
                        candidates.fetch_add(batch.num_rows(), Ordering::SeqCst);
                    }
                    batch
                });
                Box::pin(RecordBatchStreamAdapter::new(
                    request.schema.clone(),
                    stream,
                ))
            }
        }

        let mut env = TestEnv::new().await.with_node_id(42);
        let engine = env
            .create_engine(MitoConfig {
                schedule_compaction_after_edit: false,
                ..Default::default()
            })
            .await;
        let region_id = RegionId::new(11, 1);
        engine
            .handle_request(
                region_id,
                RegionRequest::Create(CreateRequestBuilder::new().build()),
            )
            .await
            .unwrap();
        engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_add: vec![FileMeta {
                        region_id,
                        file_id: FileId::from_bytes(1u128.to_be_bytes()),
                        ..Default::default()
                    }],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                },
            )
            .await
            .unwrap();
        let source = Arc::new(RecordingSource {
            inner: ManifestSstSource::new(engine),
            filters: Mutex::new(Vec::new()),
            candidates: Arc::new(AtomicUsize::new(0)),
        });
        let ctx = SessionContext::new();
        ctx.register_table("manifest", Arc::new(InspectTable::new(source.clone())))
            .unwrap();
        for (predicate, expected_filter, expected_count) in [
            ("node_id = 99".to_string(), col("node_id").eq(lit(99u64)), 0),
            (
                "node_id != 42".to_string(),
                col("node_id").not_eq(lit(42u64)),
                0,
            ),
            ("node_id = 42".to_string(), col("node_id").eq(lit(42u64)), 1),
            (
                "region_id = 0".to_string(),
                col("region_id").eq(lit(0u64)),
                0,
            ),
            (
                format!("region_id = {}", region_id.as_u64()),
                col("region_id").eq(lit(region_id.as_u64())),
                1,
            ),
        ] {
            source.candidates.store(0, Ordering::SeqCst);
            source.filters.lock().unwrap().clear();
            // collect() runs the default optimizer and the actual inspection provider.
            let batches = ctx
                .sql(&format!("SELECT file_id FROM manifest WHERE {predicate}"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            // The scanner receives a UInt64 comparison, with no Decimal cast.
            assert_eq!(
                *source.filters.lock().unwrap(),
                vec![expected_filter],
                "{predicate}"
            );
            // Count the manifest source's candidates before DataFusion's residual filter.
            assert_eq!(
                source.candidates.load(Ordering::SeqCst),
                expected_count,
                "{predicate}"
            );
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                expected_count,
                "{predicate}"
            );
        }
    }
}
