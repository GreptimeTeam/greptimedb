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

//! Test-only helpers that produce *real* DataFusion dynamic filters.
//!
//! A `HashTableLookupExpr` built by hand always has `build_lease = None`, so it cannot
//! prove that build-side membership recovery works. These helpers run an actual
//! `HashJoinExec`, which publishes lookups backed by real build leases, and return the
//! code of the dynamic filter it produced.
//!
//! The helpers live behind `test`/the `testing` feature so both `common-query` and the
//! distributed-plan tests can exercise the same fixture.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{JoinType, NullEquality};
use datafusion_datasource::memory::MemorySourceConfig;
use futures_util::TryStreamExt;

/// A real hash-join dynamic filter and the probe schema it is bound to.
pub struct RealDynFilter {
    /// The predicate DataFusion published for the join's probe side.
    pub expr: Arc<dyn PhysicalExpr>,
    /// Schema of the probe side the predicate applies to.
    pub probe_schema: Arc<Schema>,
}

/// Schema with a single nullable column.
pub fn one_column_schema(name: &str, data_type: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(name, data_type, true)]))
}

/// Turns one column vector per partition into one `RecordBatch` per partition.
pub fn partitions(schema: &Arc<Schema>, columns: Vec<Vec<ArrayRef>>) -> Vec<Vec<RecordBatch>> {
    columns
        .into_iter()
        .map(|columns| vec![RecordBatch::try_new(Arc::clone(schema), columns).unwrap()])
        .collect()
}

/// A column reference by name and index.
pub fn key(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

/// A `Utf8` array.
pub fn utf8(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// Runs a real `HashJoinExec` and returns the dynamic filter it published.
///
/// The join is forced onto the `Map` pushdown strategy (the only strategy that
/// produces a runtime lookup) by setting the `IN`-list pushdown limit to zero, and
/// both children are hash-partitioned on the complete join-key vector, which is what
/// DataFusion requires before it publishes a partitioned dynamic filter.
pub async fn run_real_hash_join(
    partition_count: usize,
    left_schema: Arc<Schema>,
    right_schema: Arc<Schema>,
    build_partitions: Vec<Vec<RecordBatch>>,
    probe_partitions: Vec<Vec<RecordBatch>>,
    on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
) -> RealDynFilter {
    assert!(partition_count >= 1);

    let left_scan: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&build_partitions, Arc::clone(&left_schema), None)
            .unwrap();
    let right_scan: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&probe_partitions, Arc::clone(&right_schema), None)
            .unwrap();

    let (left_scan, right_scan) = if partition_count > 1 {
        // Hash on the complete join-key vector on both sides, like the real plan does.
        let left_partitioning = Partitioning::Hash(
            on.iter().map(|(build, _)| Arc::clone(build)).collect(),
            partition_count,
        );
        let right_partitioning = Partitioning::Hash(
            on.iter().map(|(_, probe)| Arc::clone(probe)).collect(),
            partition_count,
        );
        (
            Arc::new(RepartitionExec::try_new(left_scan, left_partitioning).unwrap())
                as Arc<dyn ExecutionPlan>,
            Arc::new(RepartitionExec::try_new(right_scan, right_partitioning).unwrap())
                as Arc<dyn ExecutionPlan>,
        )
    } else {
        (left_scan, right_scan)
    };
    let left = left_scan;

    let probe_filter = Arc::new(DynamicFilterPhysicalExpr::new(
        on.iter().map(|(_, probe)| Arc::clone(probe)).collect(),
        lit(true),
    ));
    let dyn_filter: Arc<dyn PhysicalExpr> = Arc::clone(&probe_filter) as _;
    // The probe subtree must reference the produced filter, otherwise the join never
    // publishes it.
    let right: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(Arc::clone(&dyn_filter), right_scan).unwrap());

    let join = HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap()
    .with_dynamic_filter_expr(Arc::clone(&probe_filter))
    .unwrap();
    let join: Arc<dyn ExecutionPlan> = Arc::new(join);

    let mut options = ConfigOptions::default();
    options.optimizer.hash_join_inlist_pushdown_max_size = 0;
    let session_config = SessionConfig::from(options);
    let state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_default_features()
        .build();
    let task_ctx = Arc::new(TaskContext::from(&state));

    // Every build partition must report before the filter is finalized, so all
    // partition streams have to make progress concurrently.
    let streams = (0..partition_count)
        .map(|partition| {
            let join = Arc::clone(&join);
            let task_ctx = Arc::clone(&task_ctx);
            async move {
                let stream = join.execute(partition, task_ctx).unwrap();
                stream.try_collect::<Vec<_>>().await.unwrap();
            }
        })
        .collect::<Vec<_>>();
    futures_util::future::join_all(streams).await;

    RealDynFilter {
        expr: probe_filter.current().unwrap(),
        probe_schema: right_schema,
    }
}

/// Build and probe batches for a `Utf8` join over distinct keys.
///
/// Both sides start as one partition; the fixture hash-repartitions them so every hash
/// partition holds rows and DataFusion produces one filter branch per partition. Also
/// returns every distinct key.
pub fn utf8_join(
    partition_count: usize,
) -> (Vec<Vec<RecordBatch>>, Vec<Vec<RecordBatch>>, Vec<String>) {
    let schema = one_column_schema("trace_id", DataType::Utf8);
    let values = (0..partition_count * 8)
        .map(|i| format!("trace-{i:02}"))
        .collect::<Vec<_>>();
    let borrowed = values.iter().map(String::as_str).collect::<Vec<_>>();
    let build = partitions(&schema, vec![vec![utf8(&borrowed)]]);
    // Probe rows include every member plus keys that belong to no build side.
    let mut probe_values = borrowed.clone();
    let misses = (0..partition_count * 4)
        .map(|i| format!("miss-{i:02}"))
        .collect::<Vec<_>>();
    probe_values.extend(misses.iter().map(String::as_str));
    let probe = partitions(&schema, vec![vec![utf8(&probe_values)]]);
    (build, probe, values)
}
