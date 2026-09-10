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
mod tests;

use std::cmp::Ordering;
use std::sync::Mutex;

use datafusion::arrow::compute::SortOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::channel::mpsc;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, SinkExt, TryStreamExt};

use crate::extension_plan::series_aggregate::*;

/// Merges partial series states without per-point hash repartitioning and sorting.
#[derive(Debug, Clone)]
pub struct SeriesFinalAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    original: Arc<dyn ExecutionPlan>,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    group_by: PhysicalGroupBy,
    input_schema: SchemaRef,
    ordering: LexOrdering,
    time_group: usize,
    grid: Grid,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl SeriesFinalAggregateExec {
    fn states(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<PartialState>>> {
        let partial = self
            .input
            .as_any()
            .downcast_ref::<SeriesAggregateExec>()
            .ok_or_else(|| {
                DataFusionError::Internal("missing series partial aggregate".to_string())
            })?;
        let partitions = partial.properties.partitioning.partition_count();
        if partitions == 1 {
            return Ok(partial.stream(0, context)?.into_states());
        }
        // Keep the partial partitions running in parallel, bounded so their states do not
        // pile up ahead of the merge.
        let (sender, receiver) = mpsc::channel(1);
        let tasks = FuturesUnordered::new();
        for partition in 0..partitions {
            let mut input = partial.stream(partition, context.clone())?.into_states();
            let mut sender = sender.clone();
            tasks.push(SpawnedTask::spawn(async move {
                while let Some(state) = input.next().await {
                    if sender.send(state?).await.is_err() {
                        break;
                    }
                }
                Ok::<(), DataFusionError>(())
            }));
        }
        drop(sender);
        // The stream owns the tasks, and every queued state owns its reservation.
        // Dropping the receiver cancels producers; task failures cannot become EOF.
        Ok(futures::stream::try_unfold(
            (receiver, tasks),
            |(mut receiver, mut tasks)| async move {
                loop {
                    if tasks.is_empty() {
                        return Ok(receiver.next().await.map(|state| (state, (receiver, tasks))));
                    }
                    futures::select! {
                        state = receiver.next().fuse() => {
                            if let Some(state) = state {
                                return Ok(Some((state, (receiver, tasks))));
                            }
                            while let Some(result) = tasks.next().await {
                                result.map_err(|error| DataFusionError::External(Box::new(error)))??;
                            }
                            return Ok(None);
                        }
                        result = tasks.next().fuse() => {
                            if let Some(result) = result {
                                result.map_err(|error| DataFusionError::External(Box::new(error)))??;
                            }
                        }
                    }
                }
            },
        )
        .boxed())
    }

    /// Constructs a single-partition final result when the caller has no partition contract.
    pub fn try_new_unordered(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<Self>> {
        let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() else {
            return Ok(None);
        };
        let Some(repartition) = aggregate.input().as_any().downcast_ref::<RepartitionExec>() else {
            return Ok(None);
        };
        let Some(partial) = repartition
            .input()
            .as_any()
            .downcast_ref::<SeriesAggregateExec>()
        else {
            return Ok(None);
        };
        if aggregate.group_expr().expr().len() != partial.columns.len() {
            return Ok(None);
        }
        let schema = plan.schema();
        let Some(ordering) = LexOrdering::new(
            (0..aggregate.group_expr().expr().len())
                .filter(|index| *index != partial.time_group)
                .chain(std::iter::once(partial.time_group))
                .map(|index| PhysicalSortExpr {
                    expr: Arc::new(Column::new(schema.field(index).name(), index)),
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                }),
        ) else {
            return Ok(None);
        };
        // Reuse the complete state/schema proof and spill path. This sort is a
        // rewrite template, not an additional execution stage on the fast path.
        let sort = Arc::new(
            SortExec::new(ordering.clone(), plan.clone()).with_preserve_partitioning(true),
        );
        let ordered: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, sort));
        Self::try_new(&ordered)
    }

    /// Requires an unchanged partial-state schema and a complete label/time sort.
    pub fn try_new(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<Self>> {
        let Some(merge) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() else {
            return Ok(None);
        };
        let Some(sort) = merge.input().as_any().downcast_ref::<SortExec>() else {
            return Ok(None);
        };
        let Some(aggregate) = sort.input().as_any().downcast_ref::<AggregateExec>() else {
            return Ok(None);
        };
        let Some(repartition) = aggregate.input().as_any().downcast_ref::<RepartitionExec>() else {
            return Ok(None);
        };
        let Some(partial) = repartition
            .input()
            .as_any()
            .downcast_ref::<SeriesAggregateExec>()
        else {
            return Ok(None);
        };
        let count = partial.columns.len();
        let expected_order = (0..count)
            .filter(|column| *column != partial.time_group)
            .chain(std::iter::once(partial.time_group))
            .collect::<Vec<_>>();
        if merge.fetch().is_some()
            || sort.fetch().is_some()
            || !sort.preserve_partitioning()
            || merge.expr() != sort.expr()
            || aggregate.mode() != &AggregateMode::FinalPartitioned
            || aggregate.limit_options().is_some()
            || !aggregate.group_expr().is_single()
            || aggregate.filter_expr().iter().any(Option::is_some)
            || aggregate.group_expr().expr().len() != count
            || aggregate.aggr_expr() != partial.aggregates.as_slice()
            || !matches!(partial.properties().boundedness, Boundedness::Bounded)
            || merge.expr().len() != count
            || merge
                .expr()
                .iter()
                .zip(&expected_order)
                .any(|(sort, column)| {
                    sort.options.descending
                        || sort.options.nulls_first
                        || sort
                            .expr
                            .as_any()
                            .downcast_ref::<Column>()
                            .map(Column::index)
                            != Some(*column)
                })
            || aggregate
                .group_expr()
                .expr()
                .iter()
                .enumerate()
                .any(|(index, (expr, _))| {
                    expr.as_any().downcast_ref::<Column>().map(Column::index) != Some(index)
                })
        {
            return Ok(None);
        }
        let ordering = merge.expr().clone();
        let properties = PlanProperties::new(
            EquivalenceProperties::new_with_orderings(plan.schema(), [ordering.clone()]),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Some(Self {
            input: repartition.input().clone(),
            original: plan.clone(),
            aggregates: aggregate.aggr_expr().to_vec(),
            group_by: aggregate.group_expr().clone(),
            input_schema: aggregate.input_schema(),
            ordering,
            time_group: partial.time_group,
            grid: partial.grid,
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn fallback(
        &self,
        stream: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = stream.schema();
        let input = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(OwnedStream {
                schema,
                stream: Mutex::new(Some(stream)),
            })],
            None,
            [],
            false,
            None,
        )?);
        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            self.group_by.clone(),
            self.aggregates.clone(),
            vec![None; self.aggregates.len()],
            input,
            self.input_schema.clone(),
        )?);
        SortExec::new(self.ordering.clone(), aggregate).execute(0, context)
    }
}

impl DisplayAs for SeriesFinalAggregateExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromSeriesFinalAggregateExec: compact_states=true, grid=[{}..{}; {}], aggr=[{}]",
            self.grid.start,
            self.grid.end,
            self.grid.step,
            self.aggregates
                .iter()
                .map(|a| a.name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl ExecutionPlan for SeriesFinalAggregateExec {
    fn name(&self) -> &str {
        "PromSeriesFinalAggregateExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 || children[0].schema() != self.input.schema() {
            return exec_err!("series final aggregate requires one child with the original schema");
        }
        // Rebuild and revalidate the original chain so a changed child cannot inherit
        // a stale series-grid proof or optimistic ordering properties.
        let sort = self.original.children()[0].clone();
        let aggregate = sort.children()[0].clone();
        let repartition = aggregate.children()[0].clone();
        let repartition = repartition.with_new_children(children)?;
        let aggregate = aggregate.with_new_children(vec![repartition])?;
        let sort = sort.with_new_children(vec![aggregate])?;
        let original = self.original.clone().with_new_children(vec![sort])?;
        Ok(match Self::try_new(&original)? {
            Some(plan) => Arc::new(plan),
            None => original,
        })
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("series final aggregate has one output partition");
        }
        let state_widths = self
            .aggregates
            .iter()
            .map(|a| Ok(a.state_fields()?.len()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::pin(FinalStream {
            plan: self.clone(),
            input: Some(self.states(context.clone())?),
            accumulators: self
                .aggregates
                .iter()
                .map(|a| a.create_groups_accumulator())
                .collect::<Result<_>>()?,
            state_widths,
            groups: SeriesGroups::default(),
            ids: Vec::new(),
            pending: None,
            fallback: None,
            finished: false,
            reservation: MemoryConsumer::new("PromSeriesFinalAggregate")
                .with_can_spill(true)
                .register(context.memory_pool()),
            metrics: BaselineMetrics::new(&self.metrics, partition),
            context,
        }))
    }
}

struct FinalStream {
    plan: SeriesFinalAggregateExec,
    input: Option<BoxStream<'static, Result<PartialState>>>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    state_widths: Vec<usize>,
    groups: SeriesGroups,
    ids: Vec<usize>,
    pending: Option<PendingOutput>,
    fallback: Option<SendableRecordBatchStream>,
    finished: bool,
    reservation: MemoryReservation,
    metrics: BaselineMetrics,
    context: Arc<TaskContext>,
}

impl FinalStream {
    fn merge(&mut self, partial: PartialState) -> Result<()> {
        let output = &partial.output;
        // The partial state names each label group once, so the labels are resolved per
        // group rather than per output point.
        let label_count = output.labels.first().map_or(1, |array| array.len());
        let mut mapping = Vec::with_capacity(label_count);
        for row in 0..label_count {
            let labels = output
                .labels
                .iter()
                .map(|array| string_at(array.as_ref(), row).map(|value| value.map(str::to_owned)))
                .collect::<Result<Vec<_>>>()?;
            let next = self.groups.labels.len();
            let id = *self.groups.labels.entry(labels).or_insert(next);
            if id == self.groups.slots.len() {
                self.groups
                    .slots
                    .push(TimeSlots::Sparse(HashMap::default()));
            }
            mapping.push(id);
        }
        self.ids.clear();
        for (label, timestamp) in &output.keys {
            let label = mapping[*label];
            let next = self.groups.keys.len();
            let id = self.groups.slots[label].intern(
                self.plan.grid.slot(*timestamp)?,
                next,
                self.plan.grid.len,
            );
            if id == next {
                self.groups.keys.push((label, *timestamp));
            }
            self.ids.push(id);
        }
        let mut offset = 0;
        for (accumulator, width) in self.accumulators.iter_mut().zip(&self.state_widths) {
            accumulator.merge_batch(
                &output.state[offset..offset + width],
                &self.ids,
                None,
                self.groups.keys.len(),
            )?;
            offset += width;
        }
        drop(partial);
        let size = self.groups.size()
            + self.ids.capacity() * size_of::<usize>()
            + self.accumulators.iter().map(|a| a.size()).sum::<usize>();
        match self.reservation.try_resize(size) {
            Ok(()) => Ok(()),
            Err(datafusion::error::DataFusionError::ResourcesExhausted(_)) => self.start_fallback(),
            Err(error) => Err(error),
        }
    }

    fn start_fallback(&mut self) -> Result<()> {
        let mut states = Vec::new();
        for accumulator in &mut self.accumulators {
            states.extend(accumulator.state(EmitTo::All)?);
        }
        let pending = std::mem::take(&mut self.groups)
            .into_output(states, self.plan.group_by.expr().len() - 1);
        self.accumulators.clear();
        self.ids = Vec::new();
        let schema = self.plan.input.schema();
        let time_group = self.plan.time_group;
        let batch_size = self.context.session_config().batch_size();
        let prefix_schema = schema.clone();
        // The emitted state stays in memory until the fallback has read it, so it has to
        // stay reserved and its reservation rides along with it. Resizing to what it
        // actually costs matters most when the growth that sent us here was the first
        // one: a failed try_resize leaves the reservation at its old size, which can be
        // zero, and handing the state over on that would put it outside the query's
        // budget. If even this does not fit, report the exhaustion rather than carry
        // untracked state into a plan that will go on to reserve more.
        self.reservation.try_resize(pending.size())?;
        let retained = self.reservation.take();
        let prefix =
            futures::stream::try_unfold((pending, retained), move |(mut pending, retained)| {
                let next = pending.next(&prefix_schema, time_group, batch_size);
                std::future::ready(
                    next.map(|batch| batch.map(|batch| (batch, (pending, retained)))),
                )
            });
        let input = self.input.take().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("missing series final input".to_string())
        })?;
        let input_schema = schema.clone();
        let input = input
            .map_ok(move |state| state.into_batches(input_schema.clone(), time_group, batch_size))
            .try_flatten();
        // No final values have been emitted. Hand incomplete states and unread
        // input to DataFusion, retaining its resource errors and disk-spill path.
        self.fallback = Some(self.plan.fallback(
            Box::pin(RecordBatchStreamAdapter::new(schema, prefix.chain(input))),
            self.context.clone(),
        )?);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        // Reserve what sorting the output into place costs before allocating it: per
        // group, the order and index vectors and the rebuilt key, plus one value per
        // aggregate. Reaching the limit here yields to the spill-capable plan instead
        // of failing.
        const PER_GROUP: usize =
            2 * size_of::<usize>() + size_of::<u64>() + size_of::<(usize, i64)>();
        let extra = self
            .groups
            .keys
            .len()
            .saturating_mul(PER_GROUP + self.accumulators.len() * size_of::<f64>());
        if let Err(error) = self.reservation.try_grow(extra) {
            return match error {
                datafusion::error::DataFusionError::ResourcesExhausted(_) => self.start_fallback(),
                error => Err(error),
            };
        }
        let mut state = self
            .accumulators
            .iter_mut()
            .map(|a| a.evaluate(EmitTo::All))
            .collect::<Result<Vec<_>>>()?;
        self.accumulators.clear();
        self.ids = Vec::new();
        // Reproduce the replaced sort: label groups ascending with nulls last, then time.
        let mut labels = self.groups.labels.iter().collect::<Vec<_>>();
        labels.sort_unstable_by(|(a, _), (b, _)| compare_labels(a, b));
        let mut order = Vec::with_capacity(self.groups.keys.len());
        for (_, label) in labels {
            match &self.groups.slots[*label] {
                TimeSlots::Dense(slots) => {
                    order.extend(slots.iter().copied().filter(|id| *id != UNSEEN))
                }
                TimeSlots::Sparse(slots) => {
                    let mut entries = slots.iter().collect::<Vec<_>>();
                    entries.sort_unstable_by_key(|(slot, _)| *slot);
                    order.extend(entries.into_iter().map(|(_, id)| *id));
                }
            }
        }
        let indices = UInt64Array::from_iter_values(order.iter().map(|id| *id as u64));
        for values in &mut state {
            *values = take(values.as_ref(), &indices, None)?;
        }
        self.groups.keys = order.iter().map(|id| self.groups.keys[*id]).collect();
        self.pending = Some(
            std::mem::take(&mut self.groups)
                .into_output(state, self.plan.group_by.expr().len() - 1),
        );
        self.finished = true;
        Ok(())
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        for _ in 0..8 {
            if let Some(fallback) = &mut self.fallback {
                return fallback.poll_next_unpin(cx);
            }
            if let Some(pending) = &mut self.pending {
                let started = Instant::now();
                let output = pending.next(
                    &self.plan.schema(),
                    self.plan.time_group,
                    self.context.session_config().batch_size(),
                );
                self.metrics.elapsed_compute().add_elapsed(started);
                if let Some(batch) = output? {
                    return Poll::Ready(Some(Ok(batch)));
                }
                self.pending = None;
                self.reservation.free();
            }
            if self.finished {
                return Poll::Ready(None);
            }
            let Some(input) = &mut self.input else {
                return Poll::Ready(None);
            };
            match ready!(input.poll_next_unpin(cx)) {
                Some(batch) => {
                    let started = Instant::now();
                    let result = self.merge(batch?);
                    self.metrics.elapsed_compute().add_elapsed(started);
                    result?;
                }
                None => {
                    let started = Instant::now();
                    let result = self.finish();
                    self.metrics.elapsed_compute().add_elapsed(started);
                    result?;
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RecordBatchStream for FinalStream {
    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
}
impl Stream for FinalStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        if matches!(poll, Poll::Ready(Some(Err(_)))) {
            self.finished = true;
            self.input = None;
            self.fallback = None;
            self.pending = None;
            self.groups = SeriesGroups::default();
            self.ids = Vec::new();
            self.accumulators.clear();
            self.reservation.free();
        }
        self.metrics.record_poll(poll)
    }
}

fn compare_labels(a: &[Option<String>], b: &[Option<String>]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => a.cmp(b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        })
        .find(|order| *order != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

// Constructed per execution only when the fast path yields to the spill-capable plan.
struct OwnedStream {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}
impl std::fmt::Debug for OwnedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("OwnedStream").finish_non_exhaustive()
    }
}
impl PartitionStream for OwnedStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
        if let Ok(mut stream) = self.stream.lock()
            && let Some(stream) = stream.take()
        {
            return stream;
        }
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(async {
                exec_err!("series final fallback stream already consumed or poisoned")
            }),
        ))
    }
}
