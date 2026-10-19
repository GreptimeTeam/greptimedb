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

//! Running a bound tree, node by node.
//!
//! [`NativeExecutionState`] owns the run-time state one execution of a [`NativeTree`] needs, keyed
//! by the position [`metrics::walk`] gives each node. The tree is what the compiler produced and
//! carries no state of its own, so a second execution builds a second state from the same tree and
//! the two never share a buffer, an evaluator or a memory reservation.
//!
//! [`NativeExecutor`] drives that state one chunk at a time. Nodes hand each other
//! [`EvaluatedChunk`]s: a source hands over raw samples, and everything above a window hands over
//! the evaluator's own chunk type. No row batch and no frame is materialised on an internal edge,
//! so a chunk a window produced is fed to the next node as it is.
//!
//! Every window owns a [`RangeEvaluator`], which is what keeps its input grid and its output grid
//! its own: `Window -> Map -> Window` evaluates the inner window on the inner grid and the outer
//! window on the outer grid, and the map in between carries the inner grid through untouched. The
//! outer window reads the inner result as the samples it is, not as something to be resampled onto
//! the outer grid first.
//!
//! A node kind this runtime does not execute yet is rejected when the execution is built, naming
//! the kind and its position. An unsupported tree therefore never starts, rather than answering
//! half a query or panicking halfway through one.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::Float64Array;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{SampleRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::map::{MathOp, map_math};
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::{self, NodeMetrics, NodePath};
use crate::native::plan::{BoundWindow, NativeExpr, NativeTree};
use crate::range_eval::RangeEvaluator;

#[cfg(test)]
mod tests;

/// A chunk on an internal edge.
///
/// Only a source hands over raw samples; everything above a window hands over evaluated chunks.
/// The two cannot be confused once the tree is built, because a window's bound input grid says
/// which one its input produces.
#[derive(Clone, Debug)]
enum NativeChunk {
    /// Raw source samples, with their own timestamps.
    Raw(RawSeriesChunk),
    /// Evaluated values, addressed on their absolute grid positions.
    Evaluated(EvaluatedChunk),
}

impl NativeChunk {
    /// The positions this chunk carries: decided grid positions, or raw timestamps.
    fn positions(&self) -> usize {
        match self {
            Self::Raw(chunk) => chunk.time.len(),
            Self::Evaluated(chunk) => chunk.range().len(),
        }
    }

    /// Whether this chunk closes its series.
    fn is_last(&self) -> bool {
        match self {
            Self::Raw(chunk) => chunk.is_last,
            Self::Evaluated(chunk) => chunk.is_last,
        }
    }

    /// An estimate of the bytes this chunk holds.
    fn bytes(&self) -> usize {
        match self {
            Self::Raw(chunk) => raw_bytes(chunk),
            Self::Evaluated(chunk) => evaluated_bytes(chunk),
        }
    }
}

/// An estimate of the bytes a raw chunk holds: its timestamps, one float per field and its
/// identity.
fn raw_bytes(chunk: &RawSeriesChunk) -> usize {
    std::mem::size_of::<RawSeriesChunk>()
        + chunk.primary_key.len()
        + chunk.time.len()
            * (std::mem::size_of::<i64>() + chunk.fields.len() * std::mem::size_of::<f64>())
}

/// An estimate of the bytes an evaluated chunk holds: one float per field and decided position.
///
/// The grid itself is shared by every chunk addressed on it, so it is not counted per chunk. The
/// figure only has to rank one chunk against another for the memory pool, which is what a
/// reservation is for.
fn evaluated_bytes(chunk: &EvaluatedChunk) -> usize {
    std::mem::size_of::<EvaluatedChunk>()
        + chunk.range().len() * chunk.series.fields.len() * std::mem::size_of::<f64>()
}

/// Which node kinds the runtime executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeTag {
    /// Forwards the chunks it is given.
    Input,
    /// Feeds its input to its own [`RangeEvaluator`].
    Window,
    /// Applies point-wise operations to its input chunks.
    Map,
}

impl NodeTag {
    /// What this tag is called in a rejection.
    fn describe(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Window => "window",
            Self::Map => "map",
        }
    }
}

/// The operation a node binds, kept aside until the whole tree has been visited.
///
/// A window's evaluator needs its input's schema, and the input is visited after the window, so
/// the binding is collected first and the run-time state is built from the complete set.
enum Seed {
    Input,
    Window(BoundWindow),
    Map(Arc<[MathOp]>),
}

/// One visited node, before its input's position is resolved.
struct PendingNode {
    tag: NodeTag,
    path: NodePath,
    metrics: Arc<NodeMetrics>,
    /// What this node produces, when it produces samples.
    output_schema: Option<Arc<SeriesSchema>>,
    seed: Seed,
}

/// One node of the flattened tree.
#[derive(Clone, Copy, Debug)]
struct NodeSpec {
    tag: NodeTag,
    /// The position of this node's input, resolved from the tree's own positions.
    input: Option<usize>,
}

/// What every node holds: its metric handles and the bytes it reserves.
///
/// The reservation is the pool's, so a node that buffers input competes with every other operator
/// in the query; the metrics report the same figure so a slow node can be told apart from a wide
/// one.
struct NodeBuffer {
    metrics: Arc<NodeMetrics>,
    reservation: MemoryReservation,
    /// The bytes `reservation` holds, mirrored so a chunk leaving shrinks it exactly.
    held: usize,
}

impl NodeBuffer {
    /// Registers a reservation for the node at `path` with `pool`.
    fn new(path: &NodePath, metrics: Arc<NodeMetrics>, pool: &Arc<dyn MemoryPool>) -> Self {
        let consumer = MemoryConsumer::new(format!("native.{}", path.as_str()));
        Self {
            metrics,
            reservation: consumer.register(pool),
            held: 0,
        }
    }

    /// Reserves the bytes of a chunk this node starts holding.
    fn hold(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_grow(bytes)?;
        self.held += bytes;
        self.metrics.record_reserved_bytes(self.held);
        Ok(())
    }

    /// Returns the bytes of a chunk this node no longer holds.
    fn shrink(&mut self, bytes: usize) -> Result<()> {
        let held = self.held.saturating_sub(bytes);
        self.reservation.try_resize(held)?;
        self.held = held;
        self.metrics.record_reserved_bytes(held);
        Ok(())
    }

    /// Returns every byte this node holds to the pool.
    fn release(&mut self) {
        self.held = 0;
        self.reservation.free();
        self.metrics.record_reserved_bytes(0);
    }
}

/// What a map node applies.
struct MapRuntime {
    ops: Arc<[MathOp]>,
    /// The schema of what the map produces, which is what its input is checked against.
    schema: Arc<SeriesSchema>,
}

/// What a window node holds.
struct WindowRuntime {
    /// This window's own evaluator, with its own input and output grid; `None` once closed.
    evaluator: Option<RangeEvaluator>,
}

impl WindowRuntime {
    /// Feeds one input chunk to the evaluator and returns the chunk it yields, if any.
    fn push(&mut self, path: &NodePath, chunk: &NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let evaluator = self.evaluator.as_mut().ok_or_else(|| closed(path))?;
        match chunk {
            // Raw samples are read window by window by the evaluator itself.
            NativeChunk::Raw(chunk) => evaluator.push(chunk),
            // An evaluated input is read as computed samples, one chunk at a time: the evaluator
            // carries the window state of one series across its chunks and closes it on the chunk
            // that says so. Handing each chunk over as it arrives is what lets this window report a
            // decision before its input series has ended; buffering the chunks into a whole series
            // first would hold the input's bytes and delay every result to the end of the series.
            NativeChunk::Evaluated(chunk) => evaluator.push_evaluated(chunk),
        }
    }

    /// Closes this window, reporting an input that ended mid-series.
    ///
    /// The evaluator is what knows whether a series is still open, for evaluated input as much as
    /// for raw: a chunk that did not say `is_last` leaves it holding the series.
    fn finish(&mut self) -> Result<()> {
        match self.evaluator.take() {
            Some(evaluator) => evaluator.finish(),
            None => Ok(()),
        }
    }
}

/// The per-node run-time state of a node.
struct NodeRuntime {
    buffer: NodeBuffer,
    /// Source chunks queued by the caller, for an input node.
    queued: VecDeque<NativeChunk>,
    /// What a window node holds.
    window: Option<WindowRuntime>,
    /// What a map node applies.
    map: Option<MapRuntime>,
}

impl NodeRuntime {
    /// Queues a source chunk, holding the bytes it needs until it leaves.
    fn enqueue(&mut self, chunk: NativeChunk) -> Result<()> {
        let (bytes, positions) = (chunk.bytes(), chunk.positions());
        self.buffer.hold(bytes)?;
        self.buffer.metrics.record_input(1, positions);
        self.queued.push_back(chunk);
        Ok(())
    }

    /// Hands over the next queued source chunk, if there is one.
    fn pop_source(&mut self) -> Result<Option<NativeChunk>> {
        let Some(chunk) = self.queued.pop_front() else {
            return Ok(None);
        };
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            // An input node's own work is handing the chunk over, so its self time covers that and
            // nothing its producer did.
            let _timer = metrics.timer();
            // The chunk leaves this node, so the pool no longer has to hold it.
            self.buffer.shrink(chunk.bytes())?;
        }
        metrics.record_output(1, chunk.positions());
        // An evaluated source is already addressed on its grid, so its decided positions are grid
        // points; a raw source carries timestamps, which are not positions on any grid.
        if let NativeChunk::Evaluated(chunk) = &chunk {
            metrics.record_grid_points(chunk.range().len());
        }
        if chunk.is_last() {
            metrics.record_completed_series(1);
        }
        Ok(Some(chunk))
    }

    /// Maps one evaluated chunk, keeping its grid and its closing.
    fn map(&mut self, path: &NodePath, chunk: EvaluatedChunk) -> Result<EvaluatedChunk> {
        let (bytes, positions, closing) =
            (evaluated_bytes(&chunk), chunk.range().len(), chunk.is_last);
        self.buffer.hold(bytes)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the input again.
            let _timer = metrics.timer();
            match self.map.as_ref() {
                Some(map) => map_chunk(&map.schema, &chunk, &map.ops),
                None => Err(DataFusionError::Execution(format!(
                    "the node at {path} holds no map"
                ))),
            }
        };
        metrics.record_input(1, positions);
        if closing {
            metrics.record_completed_series(1);
        }
        // An error leaves the reservation to the driver, which releases every node when it stops.
        let output = outcome?;
        metrics.record_grid_points(output.range().len());
        metrics.record_output(1, valid_values(&output));
        // A map holds one chunk for as long as it maps it.
        self.buffer.release();
        Ok(output)
    }

    /// Feeds one chunk to this node's window.
    fn window(&mut self, path: &NodePath, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let (bytes, positions, closing) = (chunk.bytes(), chunk.positions(), chunk.is_last());
        self.buffer.hold(bytes)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            match self.window.as_mut() {
                Some(window) => window.push(path, &chunk),
                None => Err(DataFusionError::Execution(format!(
                    "the node at {path} holds no window"
                ))),
            }
        };
        metrics.record_input(1, positions);
        if closing {
            // A series closes exactly once, on the chunk carrying `is_last`. A window that decided
            // nothing for it still closed it, so it is counted here rather than on its output.
            metrics.record_completed_series(1);
        }
        let output = outcome?;
        if let Some(output) = &output {
            metrics.record_grid_points(output.range().len());
            metrics.record_output(1, valid_values(output));
        }
        if closing {
            // The evaluator dropped what it retained for the series, so this node holds nothing
            // more of it; a series that has not closed is still buffered and stays reserved.
            self.buffer.release();
        }
        Ok(output)
    }

    /// Returns everything this node holds, including queued chunks.
    fn release(&mut self) {
        self.queued.clear();
        self.buffer.release();
    }
}

/// The run-time state of one execution of a bound tree.
///
/// The state is keyed by tree position: the same positions [`metrics::walk`] derives, so a node's
/// state, its metrics and the name it is shown under cannot drift apart. Because none of it lives
/// on the tree, one compiled tree serves every execution that builds a state from it.
pub struct NativeExecutionState {
    /// The tree's structure, flattened in traversal order.
    nodes: Vec<NodeSpec>,
    /// The position of each node, in the same order.
    paths: Vec<NodePath>,
    /// The per-node state, in the same order.
    runtimes: Vec<NodeRuntime>,
    /// The index of every node, so a caller names a node the way the metrics do.
    by_path: HashMap<NodePath, usize>,
    /// The index of the root node.
    root: usize,
    /// The index of the tree's input node.
    source: usize,
    /// Whether the source hands over evaluated chunks rather than raw samples.
    source_is_evaluated: bool,
}

impl NativeExecutionState {
    /// Builds the state of one execution of `tree`.
    ///
    /// Every node registers its metrics in `set` under its position and partition, and every node
    /// that buffers input gets its own reservation from `pool`.
    pub fn try_new(
        tree: &NativeTree,
        pool: Arc<dyn MemoryPool>,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Self> {
        // One traversal names and registers every node: the same walk `EXPLAIN` uses, so a node
        // cannot be shown at one position and registered at another.
        let mut pending: Vec<PendingNode> = Vec::new();
        let mut unsupported: Option<DataFusionError> = None;
        metrics::walk(tree.root(), &mut |node, path| {
            let handles = Arc::new(NodeMetrics::default());
            handles.register(path, metrics_set, partition);
            let plan = match &node.expr {
                NativeExpr::Input(_) => Some((NodeTag::Input, Seed::Input)),
                NativeExpr::Window { op, .. } => Some((NodeTag::Window, Seed::Window(op.clone()))),
                NativeExpr::Map { op, .. } => Some((NodeTag::Map, Seed::Map(Arc::clone(&op.ops)))),
                // The kinds below are legal in a bound tree, and `validate` stays the authority on
                // that; this runtime simply does not execute them yet.
                NativeExpr::Scalar(_) => {
                    unsupported.get_or_insert_with(|| not_implemented("Scalar", path));
                    None
                }
                NativeExpr::Instant { .. } => {
                    unsupported.get_or_insert_with(|| not_implemented("Instant", path));
                    None
                }
                NativeExpr::Aggregate { .. } => {
                    unsupported.get_or_insert_with(|| not_implemented("Aggregate", path));
                    None
                }
                NativeExpr::Binary { .. } => {
                    unsupported.get_or_insert_with(|| not_implemented("Binary", path));
                    None
                }
            };
            if let Some((tag, seed)) = plan {
                pending.push(PendingNode {
                    tag,
                    path: path.clone(),
                    metrics: handles,
                    output_schema: node.output.samples().cloned(),
                    seed,
                });
            }
        });
        // Rejected before a single chunk is read: a tree the runtime cannot execute must not
        // start, because a partial answer would be one the caller never asked for.
        if let Some(error) = unsupported {
            return Err(error);
        }

        // A node's input is the node at the same position extended by the step the traversal took,
        // so the structure is read off the positions instead of a second traversal that could
        // disagree with the first.
        let mut by_path = HashMap::with_capacity(pending.len());
        for (index, node) in pending.iter().enumerate() {
            by_path.insert(node.path.clone(), index);
        }

        let mut nodes = Vec::with_capacity(pending.len());
        let mut paths = Vec::with_capacity(pending.len());
        let mut runtimes = Vec::with_capacity(pending.len());
        for (index, node) in pending.iter().enumerate() {
            let input = match node.tag {
                NodeTag::Input => None,
                NodeTag::Window | NodeTag::Map => {
                    let path = node.path.input();
                    Some(*by_path.get(&path).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the native tree has no node at the input of {}",
                            node.path
                        ))
                    })?)
                }
            };
            let buffer = NodeBuffer::new(&node.path, Arc::clone(&node.metrics), &pool);
            let mut runtime = NodeRuntime {
                buffer,
                queued: VecDeque::new(),
                window: None,
                map: None,
            };
            match &node.seed {
                Seed::Input => {}
                Seed::Map(ops) => {
                    let schema = node.output_schema.clone().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the map node at {} does not produce samples",
                            node.path
                        ))
                    })?;
                    runtime.map = Some(MapRuntime {
                        ops: Arc::clone(ops),
                        schema,
                    });
                }
                Seed::Window(window) => {
                    let child = input.ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the window at {} has no input",
                            node.path
                        ))
                    })?;
                    let input_schema = pending[child].output_schema.clone().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the window at {} consumes samples, but its input produces none",
                            node.path
                        ))
                    })?;
                    // One evaluator per window, with the grids the node was bound to: the output
                    // grid is the node's own grid, which is also the grid its chunks are
                    // addressed on.
                    let evaluator = RangeEvaluator::try_new(
                        &input_schema,
                        Arc::clone(&window.output_grid),
                        window.window,
                        window.function,
                    )?;
                    runtime.window = Some(WindowRuntime {
                        evaluator: Some(evaluator),
                    });
                }
            }
            debug_assert_eq!(nodes.len(), index);
            nodes.push(NodeSpec {
                tag: node.tag,
                input,
            });
            paths.push(node.path.clone());
            runtimes.push(runtime);
        }

        // The runtime's output is an evaluated chunk. Raw samples have no grid, and a scalar or
        // aggregate state is not a query value, so neither can be the tree's answer.
        match tree.root().output.samples() {
            Some(schema) if schema.eval_grid.is_some() => {}
            Some(_) => {
                return Err(DataFusionError::Execution(
                    "the native runtime produces evaluated chunks, but the root of the tree \
                     produces raw samples"
                        .to_string(),
                ));
            }
            None => {
                return Err(DataFusionError::Execution(
                    "the native runtime produces evaluated chunks, but the root of the tree \
                     produces a scalar or aggregate state"
                        .to_string(),
                ));
            }
        }

        // The supported nodes are unary, so a tree of them has exactly one input node; it is where
        // the caller's chunks enter.
        let root = *by_path.get(&NodePath::root()).ok_or_else(|| {
            DataFusionError::Execution("the native tree has no root node".to_string())
        })?;
        let source = nodes
            .iter()
            .position(|node| node.tag == NodeTag::Input)
            .ok_or_else(|| {
                DataFusionError::Execution("the native tree has no input node".to_string())
            })?;
        let source_schema = pending[source].output_schema.clone().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "the input node at {} does not produce samples",
                pending[source].path
            ))
        })?;
        Ok(Self {
            nodes,
            paths,
            runtimes,
            by_path,
            root,
            source,
            source_is_evaluated: source_schema.eval_grid.is_some(),
        })
    }

    /// The metrics of the node at `path`.
    pub fn metrics(&self, path: &NodePath) -> Option<&Arc<NodeMetrics>> {
        let index = *self.by_path.get(path)?;
        Some(&self.runtimes[index].buffer.metrics)
    }

    /// The bytes this execution currently reserves from the pool.
    pub fn reserved_bytes(&self) -> usize {
        self.runtimes
            .iter()
            .map(|runtime| runtime.buffer.held)
            .sum()
    }

    /// The number of nodes this execution runs.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the tree this state was built from has no nodes at all.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Closes every node, reporting an input that ended mid-series.
    ///
    /// A drain finds nothing left to emit: a window reports its results on the chunk that closes
    /// the series, so what is left to do is to check that every series did close, and to return
    /// everything the execution reserved.
    fn close(&mut self) -> Result<()> {
        let mut result = Ok(());
        for runtime in &mut self.runtimes {
            if let Some(window) = runtime.window.as_mut()
                && let Err(error) = window.finish()
                && result.is_ok()
            {
                result = Err(error);
            }
            runtime.release();
        }
        result
    }
}

/// Drives one execution of a bound tree.
///
/// The driver owns the state and nothing else: every decision a node makes was bound before the
/// tree existed, so a pull only reads the position and runs the bound operation.
pub struct NativeExecutor {
    state: NativeExecutionState,
    phase: Phase,
}

/// Whether the execution can still produce chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    /// Chunks may still be queued and pulled.
    Running,
    /// The input ended and every node closed.
    Finished,
    /// A chunk failed, so the tree stopped: the evaluators must not be reused after an error.
    Failed(String),
}

impl NativeExecutor {
    /// Drives `state`, which was built from one execution's tree.
    pub fn new(state: NativeExecutionState) -> Self {
        Self {
            state,
            phase: Phase::Running,
        }
    }

    /// The state this driver runs.
    pub fn state(&self) -> &NativeExecutionState {
        &self.state
    }

    /// Queues one raw source chunk.
    pub fn push_raw(&mut self, chunk: RawSeriesChunk) -> Result<()> {
        self.push_source(NativeChunk::Raw(chunk))
    }

    /// Queues one evaluated source chunk.
    pub fn push_evaluated(&mut self, chunk: EvaluatedChunk) -> Result<()> {
        self.push_source(NativeChunk::Evaluated(chunk))
    }

    /// The next chunk the root produces, or `None` when nothing has settled yet.
    ///
    /// Every node takes whole chunks from its input, so a caller that keeps pushing and pulling
    /// materialises nothing beyond the chunk in hand.
    pub fn next_chunk(&mut self) -> Result<Option<EvaluatedChunk>> {
        self.check_running()?;
        let root = self.state.root;
        let result = self.pull(root).and_then(|chunk| match chunk {
            Some(NativeChunk::Evaluated(chunk)) => Ok(Some(chunk)),
            // Rejected when the state was built; kept as an error rather than a panic so an
            // impossible tree cannot take the process down.
            Some(NativeChunk::Raw(_)) => Err(DataFusionError::Execution(
                "the native runtime produces evaluated chunks, but its root produced raw samples"
                    .to_string(),
            )),
            None => Ok(None),
        });
        if let Err(error) = &result {
            self.stop(error);
        }
        result
    }

    /// Ends the input: drains what has settled, closes every node and releases every reservation.
    pub fn finish(&mut self) -> Result<Vec<EvaluatedChunk>> {
        let mut drained = Vec::new();
        loop {
            match self.next_chunk() {
                Ok(Some(chunk)) => drained.push(chunk),
                Ok(None) => break,
                Err(error) => return Err(error),
            }
        }
        match self.state.close() {
            Ok(()) => {
                self.phase = Phase::Finished;
                Ok(drained)
            }
            Err(error) => {
                self.stop(&error);
                Err(error)
            }
        }
    }

    /// The next chunk of the node at `index`.
    fn pull(&mut self, index: usize) -> Result<Option<NativeChunk>> {
        let tag = self.state.nodes[index].tag;
        let input = self.state.nodes[index].input;
        match tag {
            NodeTag::Input => self.state.runtimes[index].pop_source(),
            NodeTag::Map => {
                let child = input.ok_or_else(|| missing_input(tag))?;
                let Some(chunk) = self.pull(child)? else {
                    return Ok(None);
                };
                self.map_node(index, chunk).map(Some)
            }
            NodeTag::Window => {
                let child = input.ok_or_else(|| missing_input(tag))?;
                // A window reports on the chunk that closes a series, so a chunk that decides
                // nothing yields nothing and the window asks its input again.
                loop {
                    let Some(chunk) = self.pull(child)? else {
                        return Ok(None);
                    };
                    if let Some(output) = self.window_node(index, chunk)? {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
        }
    }

    /// Maps one chunk the node at `index` pulled from its input.
    fn map_node(&mut self, index: usize, chunk: NativeChunk) -> Result<NativeChunk> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(format!(
                "the map node at {} consumes an evaluated chunk, not raw samples",
                self.state.paths[index]
            )));
        };
        let path = &self.state.paths[index];
        let runtime = &mut self.state.runtimes[index];
        runtime.map(path, chunk).map(NativeChunk::Evaluated)
    }

    /// Feeds one chunk to the window at `index`.
    fn window_node(&mut self, index: usize, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let path = &self.state.paths[index];
        let runtime = &mut self.state.runtimes[index];
        runtime.window(path, chunk)
    }

    /// Queues a source chunk at the tree's input node.
    fn push_source(&mut self, chunk: NativeChunk) -> Result<()> {
        self.check_running()?;
        let evaluated = matches!(chunk, NativeChunk::Evaluated(_));
        if evaluated != self.state.source_is_evaluated {
            let error = DataFusionError::Execution(format!(
                "the native source is bound to {}, but an {} chunk was queued at it",
                if self.state.source_is_evaluated {
                    "an evaluated series"
                } else {
                    "raw samples"
                },
                if evaluated { "evaluated" } else { "raw" },
            ));
            self.stop(&error);
            return Err(error);
        }
        let source = self.state.source;
        let result = self.state.runtimes[source].enqueue(chunk);
        if let Err(error) = &result {
            self.stop(error);
        }
        result
    }

    /// Refuses to continue an execution that finished or stopped.
    fn check_running(&self) -> Result<()> {
        match &self.phase {
            Phase::Running => Ok(()),
            Phase::Finished => Err(DataFusionError::Execution(
                "the native execution already finished".to_string(),
            )),
            Phase::Failed(reason) => Err(DataFusionError::Execution(format!(
                "the native execution stopped after an earlier error: {reason}"
            ))),
        }
    }

    /// Stops the execution, returning every byte it reserved to the pool.
    ///
    /// A stopped execution is not resumed: an evaluator that failed must not be reused, so the
    /// chunks it already produced are the only ones the caller will see.
    fn stop(&mut self, error: &DataFusionError) {
        for runtime in &mut self.state.runtimes {
            runtime.release();
        }
        self.phase = Phase::Failed(error.to_string());
    }
}

/// Maps one chunk with `ops`, keeping its grid range and its closing.
///
/// Mapping is point-wise over decided positions, so the chunk keeps the range it came with: a
/// position the input decided is decided here too, with or without a value.
fn map_chunk(
    schema: &SeriesSchema,
    chunk: &EvaluatedChunk,
    ops: &[MathOp],
) -> Result<EvaluatedChunk> {
    let mut series = chunk.series.clone();
    for &op in ops {
        series = match map_math(schema, &series, op)? {
            Some(mapped) => mapped,
            // `clamp` with `max < min` is the empty vector: the positions are still decided, so
            // the chunk keeps its range and closes its series with no value in it.
            None => empty_like(schema, &series, chunk.range())?,
        };
    }
    EvaluatedChunk::try_new(series, chunk.is_last)
}

/// A series with the same identity, fields and range, holding no value.
fn empty_like(schema: &SeriesSchema, series: &Series, range: &GridRange) -> Result<Series> {
    let fields = (0..schema.fields.len())
        .map(|_| Samples::from_float(Float64Array::new_null(range.len())))
        .collect();
    let fields = SeriesFields::try_from_vec(fields).map_err(series_error)?;
    let range = GridRange::try_new(Arc::clone(range.grid()), range.start(), range.len())
        .map_err(series_error)?;
    Series::try_new(schema, series.metric.clone(), TimeAxis::Grid(range), fields)
        .map_err(series_error)
}

/// The valid values a chunk holds: one per valid field sample.
fn valid_values(chunk: &EvaluatedChunk) -> usize {
    chunk
        .series
        .fields
        .iter()
        .map(|field| {
            field
                .iter()
                .filter(|sample| !matches!(sample, SampleRef::Missing))
                .count()
        })
        .sum()
}

/// The rejection of a node kind this runtime does not execute yet.
///
/// It names the kind and the node's position: the position is what the metrics and `EXPLAIN` call
/// the same node, so a reader can find it in the tree.
fn not_implemented(kind: &str, path: &NodePath) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute {kind} nodes yet: {kind} at {path}"
    ))
}

/// The error for a window whose evaluator this execution already closed.
fn closed(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!("the window at {path} is already closed"))
}

/// The error for a node the flattened tree lost its input for.
fn missing_input(tag: NodeTag) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the native tree has no input for a {} node",
        tag.describe()
    ))
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
