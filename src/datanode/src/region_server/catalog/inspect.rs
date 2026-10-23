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
use store_api::sst_entry::ManifestSstEntry;

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
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) filters: Vec<Expr>,
    pub(crate) batch_size: usize,
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
        let projection = request.projection.clone();
        let stream = self
            .engine
            .scan_ssts_from_manifest_projected(
                request.filters.clone(),
                request.batch_size,
                projection.clone(),
            )
            .map(move |entries| {
                ManifestSstEntry::to_projected_record_batch(&entries, projection.as_deref())
                    .map_err(DataFusionError::from)
            });
        Box::pin(RecordBatchStreamAdapter::new(
            request.schema.clone(),
            stream,
        ))
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
}
