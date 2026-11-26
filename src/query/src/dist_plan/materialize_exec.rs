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

//! The local half of a materialized broadcast join, as a physical plan node.
//!
//! [`MaterializeExec`] runs the build side once on the frontend. Its output is shared
//! through [`OnceAsync`], so the two consumers - the stage itself and the merge stage
//! that binds the batches into the remote join template - observe the same evaluation,
//! the same value and the same failure:
//!
//! * the first poll drives the materialization,
//! * every later poll awaits the same shared future,
//! * dropping the pollers pauses the read instead of leaving it running in the
//!   background.
//!
//! The materialization collects every batch of the build side and has no run-time row limit of its
//! own: whether a build side is small enough to broadcast is a planning decision, taken from the
//! table statistics (`MAX_BROADCAST_BUILD_BYTES` of the nested broadcast join planner). The
//! collected batches are charged to the query's memory pool as they arrive, so a pool that refuses
//! the growth fails the materialization. That failure is reported as
//! [`BroadcastMaterializeError`], which identifies the stage in an error chain: the query then
//! falls back to the plan it would have run without the broadcast
//! (`DatafusionQueryEngine::exec_query_plan`), because nothing of this stage has been shipped to
//! the client yet.

use std::fmt::{self, Debug, Formatter};
use std::future::Future;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::EquivalenceProperties;
use futures::future::{BoxFuture, FutureExt, Shared};
use futures_util::StreamExt;

/// The shared failure type of [`OnceAsync`], so a failed evaluation is handed to every
/// waiter without cloning the error.
type SharedResult<T> = std::result::Result<T, Arc<DataFusionError>>;

/// The failure of a broadcast materialization, as opposed to a failure of the rest of the query.
///
/// The resource failure of a time series query is reported as
/// [`DataFusionError::ResourcesExhausted`], which the query level fallback cannot tell apart from
/// any other resource failure. The materialization therefore reports its own pool rejection, so
/// that the consumer of the output stream can recognize the stage in the error chain
/// ([`BroadcastMaterializeError::is_in_chain`]) and retry the query with the plan it would have
/// run without the broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastMaterializeError {
    /// The memory pool of the query refused to hold another batch of the build side.
    ExceededMemoryPool { requested: usize },
}

impl BroadcastMaterializeError {
    /// Whether `err` is, or wraps, a failure of a broadcast materialization.
    ///
    /// A failed stage reaches the consumer of the output stream through the error chain of the
    /// stream item (`common_recordbatch::Error` over `DataFusionError` over this error), so the
    /// source chain is walked instead of matching a single wrapper.
    pub fn is_in_chain(err: &(dyn std::error::Error + 'static)) -> bool {
        let mut current = Some(err);
        while let Some(err) = current {
            if err.downcast_ref::<Self>().is_some() {
                return true;
            }
            current = err.source();
        }
        false
    }
}

impl fmt::Display for BroadcastMaterializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExceededMemoryPool { requested } => write!(
                f,
                "broadcast materialize exceeded the memory pool: cannot reserve {requested} bytes \
                 of the build side"
            ),
        }
    }
}

impl std::error::Error for BroadcastMaterializeError {}

/// Runs an async computation once and shares its value as well as its error.
pub(crate) struct OnceAsync<T> {
    state: Mutex<Option<Shared<BoxFuture<'static, SharedResult<T>>>>>,
    #[cfg(test)]
    inits: AtomicUsize,
}

impl<T: Clone + Send + 'static> Default for OnceAsync<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + 'static> OnceAsync<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
            #[cfg(test)]
            inits: AtomicUsize::new(0),
        }
    }

    /// Returns the shared outcome, running `init` only on the first call.
    ///
    /// `init` is invoked by the first poll of the returned future, not while the state
    /// is locked, so a slow read never blocks other callers from joining it.
    pub async fn get_or_init<F, Fut>(&self, init: F) -> Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let shared = {
            let mut state = self.state.lock().unwrap();
            match state.as_ref() {
                Some(shared) => shared.clone(),
                None => {
                    let shared = async move { init().await.map_err(Arc::new) }
                        .boxed()
                        .shared();
                    #[cfg(test)]
                    self.inits.fetch_add(1, Ordering::SeqCst);
                    *state = Some(shared.clone());
                    shared
                }
            }
        };
        shared.await.map_err(DataFusionError::Shared)
    }

    #[cfg(test)]
    pub fn init_count(&self) -> usize {
        self.inits.load(Ordering::SeqCst)
    }
}

impl<T> Debug for OnceAsync<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "OnceAsync")
    }
}

/// Materializes the build side of a broadcast join and shares the batches with the
/// merge stage that publishes them.
#[derive(Clone)]
pub struct MaterializeExec {
    inner: Arc<Inner>,
}

struct Inner {
    build: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    result: Arc<OnceAsync<Vec<RecordBatch>>>,
    properties: Arc<PlanProperties>,
}

impl Debug for MaterializeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializeExec")
            .field("build", &self.inner.build)
            .finish()
    }
}

fn properties_of(schema: &SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema.clone()),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

impl MaterializeExec {
    pub fn new(build: Arc<dyn ExecutionPlan>) -> Self {
        let schema = build.schema();
        Self {
            inner: Arc::new(Inner {
                properties: properties_of(&schema),
                schema,
                build,
                result: Arc::new(OnceAsync::new()),
            }),
        }
    }

    /// The shared evaluation of the build side.
    ///
    /// The merge stage that ships these batches and this stage's own output stream both
    /// read the same cell, so the build side is evaluated once.
    pub fn result(&self) -> Arc<OnceAsync<Vec<RecordBatch>>> {
        self.inner.result.clone()
    }

    /// Evaluates the build side once and returns the shared batches.
    ///
    /// Every caller - the first output partition and the merge stage - goes through this
    /// method, so the build plan is read exactly once and a failure reaches all of
    /// them.
    pub async fn materialize(&self, context: Arc<TaskContext>) -> Result<Vec<RecordBatch>> {
        let build = self.inner.build.clone();
        self.result()
            .get_or_init(move || Self::evaluate(build, context))
            .await
    }

    /// Rebuilds the stage over `build`, keeping the shared evaluation of this instance.
    pub(crate) fn with_build(&self, build: Arc<dyn ExecutionPlan>) -> Self {
        let schema = build.schema();
        Self {
            inner: Arc::new(Inner {
                properties: properties_of(&schema),
                schema,
                build,
                result: self.inner.result.clone(),
            }),
        }
    }

    /// Fully consumes the build side, whatever its size, as long as the memory pool of the query
    /// holds the batches it collects.
    async fn evaluate(
        build: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        // The build side is distributed by region: coalesce all of its partitions,
        // otherwise only the first region of the build side is broadcast.
        let coalesced = CoalescePartitionsExec::new(build);
        let mut stream = coalesced.execute(0, context.clone())?;

        // Charging the batches to the pool of the query keeps the read inside the query's memory
        // budget: a pool that refuses the growth fails the stage with the marker error, which the
        // query level fallback recognizes and answers by replanning without the broadcast. The
        // reservation covers the read itself; the consumers of the shared batches then read the
        // memory the pool admitted.
        let reservation = MemoryConsumer::new("MaterializeExec").register(context.memory_pool());

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let requested = batch.get_array_memory_size();
            reservation.try_grow(requested).map_err(|_| {
                DataFusionError::External(Box::new(BroadcastMaterializeError::ExceededMemoryPool {
                    requested,
                }))
            })?;
            batches.push(batch);
        }
        Ok(batches)
    }
}

impl DisplayAs for MaterializeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "MaterializeExec")
    }
}

impl ExecutionPlan for MaterializeExec {
    fn name(&self) -> &str {
        "MaterializeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.inner.properties
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema.clone()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // The stage has no expressions of its own: it only ships the build side's batches.
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner.build]
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "MaterializeExec expects exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(self.with_build(children.swap_remove(0))))
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    /// Drops the materialized batches: the build side is read again on the next poll.
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(self.inner.build.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        if partition != 0 {
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema)));
        }

        let this = self.clone();
        let stream = async_stream::try_stream! {
            let batches = this.materialize(context).await?;
            for batch in batches {
                yield batch;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::test::TestMemoryExec;

    use super::*;

    fn int64_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(int64_schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn materialize_exec(batches: Vec<RecordBatch>) -> MaterializeExec {
        let build = TestMemoryExec::try_new(&[batches], int64_schema(), None).unwrap();
        MaterializeExec::new(Arc::new(build))
    }

    fn context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    /// A task context whose query memory pool holds at most `limit` bytes.
    fn context_with_memory_limit(limit: usize) -> Arc<TaskContext> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(limit)))
            .build()
            .unwrap();
        Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
    }

    #[tokio::test]
    async fn materializes_the_build_side_once() {
        let exec = materialize_exec(vec![batch(vec![1, 2]), batch(vec![3])]);

        let batches = exec.materialize(context()).await.unwrap();
        assert_eq!(1, exec.result().init_count());
        assert_eq!(
            vec![batch(vec![1, 2]), batch(vec![3])],
            batches,
            "the build side is consumed batch by batch"
        );

        // The stage exposes a single output partition; the others are empty.
        assert!(matches!(
            exec.properties().partitioning,
            Partitioning::UnknownPartitioning(1)
        ));
        assert_eq!(1, exec.children().len());

        let stream = exec.execute(0, context()).unwrap();
        let batches: Vec<_> = stream.collect().await;
        assert_eq!(2, batches.len());
        assert_eq!(
            1,
            exec.result().init_count(),
            "the output stream must reuse the shared evaluation"
        );

        let empty = exec.execute(1, context()).unwrap();
        let batches: Vec<_> = empty.collect().await;
        assert!(batches.is_empty());
    }

    /// There is no run-time row limit on the build side: a build side far above the former row
    /// budget is collected completely and shared, instead of failing the query. Only the memory
    /// pool of the query bounds the stage.
    #[tokio::test]
    async fn materializes_a_build_side_beyond_any_row_budget() {
        let build = vec![batch(vec![1i64; 10_001]), batch(vec![2i64; 10_001])];
        let exec = materialize_exec(build.clone());

        assert_eq!(build, exec.materialize(context()).await.unwrap());
        assert_eq!(1, exec.result().init_count());

        let stream = exec.execute(0, context()).unwrap();
        let batches: Vec<_> = stream.collect().await;
        assert_eq!(2, batches.len());
        assert!(batches.iter().all(|batch| batch.is_ok()));
        assert_eq!(
            1,
            exec.result().init_count(),
            "the output stream must reuse the shared evaluation"
        );
    }

    #[tokio::test]
    async fn reset_state_clears_the_materialized_batches() {
        let exec = Arc::new(materialize_exec(vec![batch(vec![7])]));
        assert_eq!(
            vec![batch(vec![7])],
            exec.materialize(context()).await.unwrap()
        );
        assert_eq!(1, exec.result().init_count());

        let reset = exec.reset_state().unwrap();
        let reset = reset
            .downcast_ref::<MaterializeExec>()
            .expect("reset_state keeps the stage type");
        assert_eq!(0, reset.result().init_count());
        assert_eq!(
            vec![batch(vec![7])],
            reset.materialize(context()).await.unwrap()
        );
        assert_eq!(1, reset.result().init_count());
    }

    /// A build side that the memory pool of the query does not hold fails the stage with the
    /// marker error, not with a plain resource error: the query level fallback recognizes the
    /// stage by it.
    #[tokio::test]
    async fn fails_with_the_marker_error_when_the_pool_refuses_the_build_side() {
        let build = batch(vec![1i64; 10_001]);
        let requested = build.get_array_memory_size();
        let exec = materialize_exec(vec![build]);

        let err = exec
            .materialize(context_with_memory_limit(requested / 2))
            .await
            .expect_err("the pool holds less than one batch");
        assert!(
            BroadcastMaterializeError::is_in_chain(&err),
            "the failure must carry the marker error, got: {err:?}"
        );
        assert!(
            err.to_string()
                .contains("broadcast materialize exceeded the memory pool"),
            "unexpected message: {err}"
        );
    }

    /// A pool that holds the build side changes nothing: the batches are collected completely and
    /// the reservation is returned to the pool once the read is done.
    #[tokio::test]
    async fn collects_the_build_side_the_pool_holds() {
        let build = vec![batch(vec![1i64; 128]), batch(vec![2i64; 128])];
        let held: usize = build
            .iter()
            .map(|batch| batch.get_array_memory_size())
            .sum();
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(held)))
            .build()
            .unwrap();
        let pool = runtime.memory_pool.clone();
        let context = Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
        let exec = materialize_exec(build.clone());

        assert_eq!(0, pool.reserved());
        assert_eq!(build, exec.materialize(context).await.unwrap());
        assert_eq!(
            0,
            pool.reserved(),
            "the reservation of the stage is returned to the pool"
        );
    }

    /// The marker error survives the wrapping of a failed shared evaluation, which is how a
    /// failure of the build side reaches the consumer of the output stream.
    #[test]
    fn marker_error_is_found_through_the_shared_failure() {
        let marker =
            DataFusionError::External(Box::new(BroadcastMaterializeError::ExceededMemoryPool {
                requested: 8,
            }));
        let shared = DataFusionError::Shared(Arc::new(marker));
        let err = DataFusionError::Context("executing the stage".to_string(), Box::new(shared));

        assert!(BroadcastMaterializeError::is_in_chain(&err));
        assert!(!BroadcastMaterializeError::is_in_chain(
            &DataFusionError::ResourcesExhausted("the pool is full".to_string())
        ));
    }
}
