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

//! Routes complete aggregate-state rows by their group's canonical labels.

use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    SendableStateSeriesStream, pack_aggregate_state, unpack_aggregate_state,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{EvaluationType, SchedulingType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::series::aggregate_state::{AggregateStateSchema, StateSeries};
use datatypes::series::frame::{FrameContent, SeriesFrameDeclaration};
use datatypes::series::samples::SeriesSchema;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use twox_hash::XxHash64;

use crate::grid_series::label_set;

const ROUTING_SEED: u64 = 0x517c_c1b7_2722_0a95;

/// Routes aggregate-state frames by their group's canonical labels.
///
/// A state row is one group, complete as written by one producer: there is nothing to chain and
/// nothing to merge, so this node only decides which output owns the row and hands it on. Every
/// contribution of one group therefore reaches exactly one owner, and Final above it sees all of
/// them. A row is never merged with another; `is_last` is producer-local and is not interpreted.
/// There is no coalesce. Rows within an output retain arrival order, sufficient because Final folds
/// positions, not sequences. The read belongs to outputs that were claimed, not to the plan: leases
/// stop the tasks when no claimed output remains and every output has either been claimed or ended.
pub struct StateRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    outputs: usize,
    schema: Arc<SeriesSchema>,
    state: Arc<AggregateStateSchema>,
    shared: Mutex<Option<Arc<Shared>>>,
    properties: Arc<PlanProperties>,
}

impl StateRepartitionExec {
    /// Creates a state-frame router with a fixed positive output count.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, outputs: usize) -> DataFusionResult<Self> {
        if outputs == 0 {
            return Err(DataFusionError::Plan(
                "StateRepartition requires at least one output".into(),
            ));
        }
        if input.properties().boundedness.is_unbounded() {
            return Err(DataFusionError::Plan(
                "StateRepartition requires a bounded input".into(),
            ));
        }
        let declaration = SeriesFrameDeclaration::parse(input.schema().as_ref()).map_err(|e| {
            DataFusionError::Plan(format!("StateRepartition input is not a state frame: {e}"))
        })?;
        if declaration.content() != FrameContent::AggregateState {
            return Err(DataFusionError::Plan(
                "StateRepartition requires aggregate-state content".into(),
            ));
        }
        let state = Arc::new(
            declaration
                .aggregate_state()
                .ok_or_else(|| DataFusionError::Plan("state declaration is absent".into()))?
                .clone(),
        );
        let schema = Arc::new(declaration.series().clone());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(outputs),
            input.properties().emission_type,
            input.properties().boundedness,
        )
        .with_evaluation_type(EvaluationType::Eager)
        .with_scheduling_type(SchedulingType::Cooperative);
        Ok(Self {
            input,
            outputs,
            schema,
            state,
            shared: Mutex::new(None),
            properties: Arc::new(properties),
        })
    }
}

impl fmt::Debug for StateRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateRepartitionExec")
            .field("outputs", &self.outputs)
            .finish()
    }
}

impl DisplayAs for StateRepartitionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "StateRepartitionExec: outputs={}", self.outputs)
    }
}

impl ExecutionPlan for StateRepartitionExec {
    fn name(&self) -> &'static str {
        "StateRepartitionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "StateRepartition requires one child".into(),
            ));
        }
        Ok(Arc::new(Self::try_new(children.remove(0), self.outputs)?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition >= self.outputs {
            return Err(DataFusionError::Execution(format!(
                "output partition {partition} is out of range"
            )));
        }
        let shared = {
            let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            if shared.is_none() {
                *shared = Some(Shared::start(
                    &self.input,
                    self.outputs,
                    Arc::clone(&self.state),
                    Arc::clone(&self.schema),
                    &context,
                )?);
            }
            Arc::clone(shared.as_ref().expect("shared state was initialized"))
        };
        let receiver = shared.take_receiver(partition)?;
        let stream = StateOutputStream {
            schema: self.input.schema(),
            packed: None,
            receiver: Some(receiver),
            _lease: Some(OutputLease {
                shared: Arc::clone(&shared),
            }),
            shared: Arc::clone(&shared),
            state: Arc::clone(&self.state),
            series: Arc::clone(&self.schema),
            input_count: self.input.properties().partitioning.partition_count(),
        };
        Ok(Box::pin(stream))
    }
}

struct Shared {
    receivers: Mutex<Vec<Option<mpsc::UnboundedReceiver<Queued>>>>,
    senders: Vec<mpsc::UnboundedSender<Queued>>,
    tasks: Mutex<Vec<common_runtime::JoinHandle<()>>>,
    lifecycle: Mutex<Lifecycle>,
    error: Arc<Mutex<Option<String>>>,
}

struct Lifecycle {
    claimed: usize,
    remaining: usize,
    stopped: bool,
}

struct OutputLease {
    shared: Arc<Shared>,
}
impl Drop for OutputLease {
    fn drop(&mut self) {
        self.shared.release_output();
    }
}

enum Queued {
    Row(StateSeries, bool, MemoryReservation),
    Eof,
    Failure(String),
}

impl Shared {
    fn start(
        input: &Arc<dyn ExecutionPlan>,
        outputs: usize,
        state: Arc<AggregateStateSchema>,
        series: Arc<SeriesSchema>,
        context: &Arc<TaskContext>,
    ) -> DataFusionResult<Arc<Self>> {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..outputs {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.push(tx);
            receivers.push(Some(rx));
        }
        let shared = Arc::new(Self {
            receivers: Mutex::new(receivers),
            senders,
            tasks: Mutex::new(Vec::new()),
            lifecycle: Mutex::new(Lifecycle {
                claimed: 0,
                remaining: outputs,
                stopped: false,
            }),
            error: Arc::new(Mutex::new(None)),
        });
        let partitions = input.properties().partitioning.partition_count();
        for p in 0..partitions {
            let frames = input.execute(p, Arc::clone(context))?;
            let mut rows =
                unpack_aggregate_state(frames, Some(Arc::clone(&state)), Some(Arc::clone(&series)))
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let senders = shared.senders.clone();
            let outcome = Arc::clone(&shared.error);
            let pool = Arc::clone(&context.runtime_env().memory_pool);
            let task = common_runtime::spawn_query(async move {
                while let Some(item) = rows.next().await {
                    match item {
                        Ok(row) => {
                            let is_last = row.is_last();
                            let value = match row.to_state_series() {
                                Ok(value) => value,
                                Err(e) => {
                                    let mut error =
                                        outcome.lock().unwrap_or_else(|x| x.into_inner());
                                    if error.is_none() {
                                        *error = Some(e.to_string());
                                    }
                                    drop(error);
                                    for sender in &senders {
                                        let _ = sender.send(Queued::Failure(e.to_string()));
                                    }
                                    return;
                                }
                            };
                            let output = canonical_output(&value, senders.len());
                            let charge = MemoryConsumer::new("StateRepartitionRow").register(&pool);
                            let bytes = std::mem::size_of::<StateSeries>() + value.states.iter().map(|v| v.len() * std::mem::size_of::<Option<datatypes::series::aggregate_state::AggregateStateValues>>()).sum::<usize>();
                            if let Err(e) = charge.try_grow(bytes) {
                                let mut error = outcome.lock().unwrap_or_else(|x| x.into_inner());
                                if error.is_none() {
                                    *error = Some(e.to_string());
                                }
                                drop(error);
                                for sender in &senders {
                                    let _ = sender.send(Queued::Failure(e.to_string()));
                                }
                                return;
                            }
                            if senders[output]
                                .send(Queued::Row(value, is_last, charge))
                                .is_err()
                            {}
                        }
                        Err(e) => {
                            let mut error = outcome.lock().unwrap_or_else(|x| x.into_inner());
                            if error.is_none() {
                                *error = Some(e.to_string());
                            }
                            drop(error);
                            for sender in &senders {
                                let _ = sender.send(Queued::Failure(e.to_string()));
                            }
                            return;
                        }
                    }
                    tokio::task::coop::consume_budget().await;
                }
                for sender in &senders {
                    let _ = sender.send(Queued::Eof);
                }
            });
            shared
                .tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(task);
        }
        Ok(shared)
    }
    fn take_receiver(&self, p: usize) -> DataFusionResult<mpsc::UnboundedReceiver<Queued>> {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
        let receiver = self
            .receivers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(p)
            .and_then(Option::take)
            .ok_or_else(|| DataFusionError::Execution("output was already executed".into()))?;
        lifecycle.remaining -= 1;
        lifecycle.claimed += 1;
        Ok(receiver)
    }

    fn release_output(&self) {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
        lifecycle.claimed -= 1;
        if lifecycle.claimed == 0 && lifecycle.remaining == 0 {
            self.stop(&mut lifecycle);
        }
    }

    fn stop(&self, lifecycle: &mut Lifecycle) {
        if lifecycle.stopped {
            return;
        }
        lifecycle.stopped = true;
        self.receivers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        for task in self.tasks.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            task.abort();
        }
    }
}
impl Drop for Shared {
    fn drop(&mut self) {
        for task in self
            .tasks
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}

// Mirrors Routing::output_labels: label_set canonicalization, XxHash64 with ROUTING_SEED, modulo outputs.
fn canonical_output(series: &StateSeries, outputs: usize) -> usize {
    let mut hasher = XxHash64::with_seed(ROUTING_SEED);
    label_set(&series.metric).hash(&mut hasher);
    (hasher.finish() % outputs as u64) as usize
}

struct StateOutputStream {
    schema: SchemaRef,
    packed: Option<SendableRecordBatchStream>,
    receiver: Option<mpsc::UnboundedReceiver<Queued>>,
    _lease: Option<OutputLease>,
    shared: Arc<Shared>,
    state: Arc<AggregateStateSchema>,
    series: Arc<SeriesSchema>,
    input_count: usize,
}
impl RecordBatchStream for StateOutputStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
impl Stream for StateOutputStream {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.packed.is_none() {
            let receiver = self
                .receiver
                .take()
                .expect("receiver exists until packer is built");
            let source: SendableStateSeriesStream = Box::pin(StateRowsStream {
                receiver,
                shared: Arc::clone(&self.shared),
                input_count: self.input_count,
                eof_count: 0,
                ended: false,
            });
            match pack_aggregate_state(source, Arc::clone(&self.state), Arc::clone(&self.series)) {
                Ok(stream) => self.packed = Some(stream),
                Err(error) => {
                    return Poll::Ready(Some(Err(DataFusionError::Execution(error.to_string()))));
                }
            }
        }
        self.packed
            .as_mut()
            .expect("packer was constructed")
            .as_mut()
            .poll_next(cx)
    }
}

struct StateRowsStream {
    receiver: mpsc::UnboundedReceiver<Queued>,
    shared: Arc<Shared>,
    input_count: usize,
    eof_count: usize,
    ended: bool,
}

impl Stream for StateRowsStream {
    type Item = Result<(StateSeries, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        loop {
            match self.receiver.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Queued::Row(series, is_last, _charge))) => {
                    return Poll::Ready(Some(Ok((series, is_last))));
                }
                Poll::Ready(Some(Queued::Failure(error))) => {
                    self.ended = true;
                    let error = BoxedError::new(common_query::error::Error::from(
                        DataFusionError::Execution(error),
                    ));
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Queued::Eof)) => {
                    self.eof_count += 1;
                    if self.eof_count == self.input_count {
                        self.ended = true;
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(None) => {
                    self.ended = true;
                    if let Some(error) = self
                        .shared
                        .error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take()
                    {
                        let error = BoxedError::new(common_query::error::Error::from(
                            DataFusionError::Execution(error),
                        ));
                        return Poll::Ready(Some(Err(error)));
                    }
                    return Poll::Ready(None);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use common_error::ext::BoxedError;
    use datafusion::arrow::array::{Array, ListArray, StructArray};
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datatypes::series::aggregate_state::{
        AggregateStateKind, AggregateStateValues, StateComponent,
    };
    use datatypes::series::samples::FieldSpec;
    use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
    use datatypes::series::{Label, SeriesLabels};
    use futures::stream;

    fn schema() -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec {
                name: Arc::from("v"),
            }]),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(0, 1, 1).unwrap()).unwrap(),
            )),
        })
    }
    fn state() -> Arc<AggregateStateSchema> {
        Arc::new(
            AggregateStateSchema::try_new(
                AggregateStateKind::Sum,
                vec![StateComponent::Value, StateComponent::Compensation],
            )
            .unwrap(),
        )
    }
    fn row(host: &str, value: f64) -> (StateSeries, bool) {
        (
            StateSeries {
                metric: SeriesLabels::try_new(
                    Some(Arc::from("m")),
                    vec![Label {
                        name: Arc::from("host"),
                        value: Arc::from(host),
                    }],
                )
                .unwrap(),
                time: TimeAxis::from_grid_spans(
                    schema().eval_grid.clone().unwrap(),
                    vec![GridSpan::new(0, 1)],
                )
                .unwrap(),
                states: vec![vec![Some(AggregateStateValues {
                    value,
                    ..Default::default()
                })]],
            },
            true,
        )
    }
    async fn plan(
        partitions: Vec<Vec<(StateSeries, bool)>>,
        outputs: usize,
    ) -> Arc<StateRepartitionExec> {
        let st = state();
        let sc = schema();
        let mut batches = Vec::new();
        for rows in partitions {
            let input: SendableStateSeriesStream =
                Box::pin(stream::iter(rows.into_iter().map(Ok::<_, BoxedError>)));
            let mut packed = pack_aggregate_state(input, Arc::clone(&st), Arc::clone(&sc)).unwrap();
            let mut partition = Vec::new();
            while let Some(batch) = packed.next().await {
                partition.push(batch.unwrap());
            }
            batches.push(partition);
        }
        let frame_schema = batches
            .iter()
            .flatten()
            .next()
            .map(|b| b.schema())
            .unwrap_or_else(|| {
                datatypes::series::frame::aggregate_state_frame_schema(
                    &st,
                    &sc,
                    datatypes::series::frame::FrameLabelMode::Explicit,
                )
                .unwrap()
            });
        let input_schema = Arc::new(Schema::new_with_metadata(
            frame_schema.fields().clone(),
            frame_schema.metadata().clone(),
        ));
        let source = MemorySourceConfig::try_new(&batches, input_schema, None).unwrap();
        Arc::new(
            StateRepartitionExec::try_new(
                Arc::new(datafusion::datasource::source::DataSourceExec::new(
                    Arc::new(source),
                )),
                outputs,
            )
            .unwrap(),
        )
    }
    async fn drain(exec: Arc<StateRepartitionExec>) -> Vec<(usize, usize)> {
        let ctx = datafusion::prelude::SessionContext::new();
        let mut out = Vec::new();
        for i in 0..exec.outputs {
            let mut s = exec.execute(i, ctx.task_ctx()).unwrap();
            while let Some(b) = s.next().await {
                let b = b.unwrap();
                out.push((i, b.num_rows()));
            }
        }
        out
    }
    #[tokio::test]
    async fn state_repartition_two_inputs_preserves_rows_and_routes_groups() {
        let exec = plan(
            vec![
                vec![row("a", 1.), row("b", 2.)],
                vec![row("c", 3.), row("a", 4.)],
            ],
            3,
        )
        .await;
        let got = drain(exec).await;
        assert_eq!(got.iter().map(|x| x.1).sum::<usize>(), 4);
    }
    #[tokio::test]
    async fn state_repartition_forwards_same_group_from_two_producers() {
        let exec = plan(vec![vec![row("a", 1.)], vec![row("a", 2.)]], 2).await;
        assert_eq!(drain(exec).await.iter().map(|x| x.1).sum::<usize>(), 2);
    }
    #[tokio::test]
    async fn state_repartition_waits_for_every_producer_eof() {
        let exec = plan(vec![vec![row("a", 1.)], vec![row("b", 2.)]], 2).await;
        assert_eq!(drain(exec).await.iter().map(|x| x.1).sum::<usize>(), 2);
    }
    #[tokio::test]
    async fn state_repartition_empty_input_ends_with_schema() {
        let exec = plan(vec![vec![]], 2).await;
        let ctx = datafusion::prelude::SessionContext::new();
        for i in 0..exec.outputs {
            let stream = exec.execute(i, ctx.task_ctx()).unwrap();
            assert!(!stream.schema().fields().is_empty());
        }
    }
    #[tokio::test]
    async fn state_repartition_rejects_malformed_frame_at_construction() {
        let st = state();
        let sc = schema();
        let input: SendableStateSeriesStream =
            Box::pin(stream::iter(vec![Ok::<_, BoxedError>(row("a", 1.))]));
        let mut packed = pack_aggregate_state(input, Arc::clone(&st), Arc::clone(&sc)).unwrap();
        let valid = packed.next().await.unwrap().unwrap();
        let outer = valid
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let list = outer
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let bad_list = Arc::new(
            ListArray::try_new(
                match list.data_type() {
                    DataType::List(field) => field.clone(),
                    _ => unreachable!(),
                },
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0])),
                list.values().clone(),
                list.nulls().cloned(),
            )
            .unwrap(),
        );
        let mut outer_columns = outer.columns().to_vec();
        outer_columns[0] = bad_list;
        let bad_outer = Arc::new(
            StructArray::try_new(
                outer.fields().clone(),
                outer_columns,
                outer.nulls().cloned(),
            )
            .unwrap(),
        );
        let mut columns = valid.columns().to_vec();
        columns[3] = bad_outer;
        let malformed =
            datafusion::arrow::record_batch::RecordBatch::try_new(valid.schema(), columns).unwrap();
        let frame_schema = Arc::new(Schema::new_with_metadata(
            malformed.schema().fields().clone(),
            malformed.schema().metadata().clone(),
        ));
        let source = MemorySourceConfig::try_new(&[vec![malformed]], frame_schema, None).unwrap();
        let exec = Arc::new(
            StateRepartitionExec::try_new(
                Arc::new(datafusion::datasource::source::DataSourceExec::new(
                    Arc::new(source),
                )),
                1,
            )
            .unwrap(),
        );
        let ctx = datafusion::prelude::SessionContext::new();
        let mut output = exec.execute(0, ctx.task_ctx()).unwrap();
        let error = output
            .next()
            .await
            .expect("malformed-frame error item")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .to_lowercase()
                .contains("state list length"),
            "{error}"
        );
        assert!(
            output.next().await.is_none(),
            "malformed frame terminates without rows or clean EOF"
        );
    }

    #[tokio::test]
    async fn state_repartition_forwards_rows_then_reports_input_failure_not_eof() {
        const FAILURE: &str = "injected mid-stream state input failure";
        let st = state();
        let sc = schema();
        let input: SendableStateSeriesStream =
            Box::pin(stream::iter(vec![Ok::<_, BoxedError>(row("a", 1.))]));
        let mut packed = pack_aggregate_state(input, Arc::clone(&st), Arc::clone(&sc)).unwrap();
        let valid = packed.next().await.unwrap().unwrap();
        let schema = valid.schema();
        let trigger = Arc::new(tokio::sync::Notify::new());
        let child = Arc::new(FailingInputExec::new(schema, valid, Arc::clone(&trigger)));
        let exec = StateRepartitionExec::try_new(child, 1).unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        let mut output = exec.execute(0, ctx.task_ctx()).unwrap();

        let batch = output
            .next()
            .await
            .expect("forwarded row batch")
            .unwrap_or_else(|e| panic!("expected forwarded row before injected error, got {e}"));
        assert_eq!(batch.num_rows(), 1);
        trigger.notify_one();
        let error = output
            .next()
            .await
            .expect("failure after forwarded row")
            .unwrap_err();
        assert!(error.to_string().contains(FAILURE), "{error}");
        assert!(
            output.next().await.is_none(),
            "stream terminates after its error, not cleanly before it"
        );
    }

    struct FailingInputExec {
        schema: datafusion::arrow::datatypes::SchemaRef,
        batch: datafusion::arrow::record_batch::RecordBatch,
        properties: Arc<PlanProperties>,
        trigger: Arc<tokio::sync::Notify>,
    }

    impl FailingInputExec {
        fn new(
            schema: datafusion::arrow::datatypes::SchemaRef,
            batch: datafusion::arrow::record_batch::RecordBatch,
            trigger: Arc<tokio::sync::Notify>,
        ) -> Self {
            Self {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::clone(&schema)),
                    Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                )),
                schema,
                batch,
                trigger,
            }
        }
    }

    impl fmt::Debug for FailingInputExec {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            f.debug_struct("FailingInputExec").finish()
        }
    }

    impl DisplayAs for FailingInputExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "FailingInputExec")
        }
    }

    impl ExecutionPlan for FailingInputExec {
        fn name(&self) -> &'static str {
            "FailingInputExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn apply_expressions(
            &self,
            _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
        ) -> DataFusionResult<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            partition: usize,
            _: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            if partition != 0 {
                return Err(DataFusionError::Execution("invalid partition".into()));
            }
            let schema = Arc::clone(&self.schema);
            let batch = self.batch.clone();
            let trigger = Arc::clone(&self.trigger);
            let items = stream::unfold(0, move |step| {
                let batch = batch.clone();
                let trigger = Arc::clone(&trigger);
                async move {
                    match step {
                        0 => Some((Ok(batch), 1)),
                        1 => {
                            trigger.notified().await;
                            Some((
                                Err(DataFusionError::Execution(
                                    "injected mid-stream state input failure".into(),
                                )),
                                2,
                            ))
                        }
                        _ => None,
                    }
                }
            });
            Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, items),
            ))
        }
    }
    #[tokio::test]
    async fn state_repartition_dropping_outputs_releases_memory() {
        let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build()
            .unwrap();
        let context = Arc::new(
            datafusion::execution::context::TaskContext::default().with_runtime(Arc::new(runtime)),
        );
        let rows = (0..5000).map(|i| row("a", i as f64)).collect();
        let exec = plan(vec![rows], 1).await;
        let mut output = exec.execute(0, context).unwrap();
        let first = output.next().await.expect("first output batch").unwrap();
        assert!(first.num_rows() > 0);
        for _ in 0..1000 {
            if pool.reserved() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(pool.reserved() > 0, "unread queued rows hold reservations");
        let shared = exec.shared.lock().unwrap().as_ref().unwrap().clone();
        drop(output);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if shared
                    .tasks
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|task| task.is_finished())
                    && pool.reserved() == 0
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("read tasks stop after the last output is dropped");
        assert!(
            shared.lifecycle.lock().unwrap().stopped,
            "dropping the last output stops the read"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "dropping output releases queued reservations"
        );
    }
}
