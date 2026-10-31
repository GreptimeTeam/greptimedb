// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Workers only merge sealed source runs. A work-slot holder never waits for
//! another source head or channel message: cache pressure spills or fails instead
//! of waiting for a consumer. This keeps capacity-one channels live even with a
//! single work slot. Binary carries merge contiguous source intervals so equal
//! row keys retain the plan's source order across every merge pass.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use datatypes::arrow::datatypes::SchemaRef;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::compaction::multi_region::source::{SharedSource, SourceEvent};
use crate::compaction::multi_region::spill::{Reservation, Run, RunStore};
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionOutput, MultiRegionCompactionPlan,
    MultiRegionCompactionStats, SourceCounters, writer,
};
use crate::error::{InvalidMultiRegionCompactionSnafu, Result};
use crate::read::FlatSource;
use crate::read::flat_merge::FlatMergeIterator;
use crate::sst::file::FileMeta;
use crate::sst::parquet::SstInfo;

/// A closed channel is an error, not a substitute for either explicit boundary.
enum Message {
    Batch { window: i64, run: Run },
    SourceEnd,
    Finish,
}

struct RegionOutput {
    region_id: RegionId,
    files: Vec<FileMeta>,
    infos: Vec<SstInfo>,
}

pub(crate) async fn execute(
    plan: MultiRegionCompactionPlan,
    context: MultiRegionCompactionContext,
    source: Arc<SharedSource>,
    counters: Arc<SourceCounters>,
) -> Result<MultiRegionCompactionOutput> {
    let stop = CancellationToken::new();
    let _cancel_on_drop = stop.clone().drop_guard();
    let store = RunStore::new(&plan, &context, stop).await?;
    let slots = Arc::new(Semaphore::new(plan.max_active_workers));
    let plan = Arc::new(plan);
    let context = Arc::new(context);
    let resources = Arc::new(WorkerResources {
        plan: plan.clone(),
        context,
        slots: slots.clone(),
    });
    let mut senders = BTreeMap::new();
    let mut workers = FuturesUnordered::new();
    for &region_id in plan.targets.keys() {
        let (sender, receiver) = mpsc::channel(1);
        let state = RegionWorker {
            windows: BTreeMap::new(),
            completed_sources: 0,
            schema: source.schema(),
            // Includes the task and the channel's block-based allocation.
            _reservation: store.reserve_metadata(8 * 1024)?,
            store: store.clone(),
        };
        senders.insert(region_id, sender);
        workers.push(AbortOnDropHandle::new(common_runtime::spawn_compact(
            run_worker(region_id, state, receiver, resources.clone()),
        )));
    }
    let scan = dispatch(source, senders, store.clone(), slots);
    tokio::pin!(scan);
    let mut scan_finished = false;
    let mut output = MultiRegionCompactionOutput {
        files: BTreeMap::new(),
        sst_infos: BTreeMap::new(),
        stats: MultiRegionCompactionStats::default(),
    };
    while !scan_finished || !workers.is_empty() {
        tokio::select! {
            biased;
            Some(result) = workers.next(), if !workers.is_empty() => {
                let result = result.context(crate::error::JoinSnafu)??;
                output.stats.output_rows += result.files.iter().map(|file| file.num_rows).sum::<u64>();
                output.stats.output_bytes += result.files.iter().map(|file| file.file_size + file.index_file_size).sum::<u64>();
                output.files.insert(result.region_id, result.files);
                output.sst_infos.insert(result.region_id, result.infos);
            }
            result = &mut scan, if !scan_finished => {
                result?;
                scan_finished = true;
            }
        }
    }
    output.stats.source_files = counters.files.load(Ordering::Relaxed);
    output.stats.source_rows = counters.rows.load(Ordering::Relaxed);
    output.stats.source_store_page_bytes = counters.store_page_bytes.load(Ordering::Relaxed);
    output.stats.spilled_bytes = store.spilled_bytes();
    output.stats.peak_buffered_bytes = store.peak_buffered_bytes();
    ensure!(
        output.stats.source_files == plan.sources.len() as u64
            && output.stats.output_rows == output.stats.source_rows,
        InvalidMultiRegionCompactionSnafu {
            reason: "Multi-region compaction did not account for every source row"
        }
    );
    Ok(output)
}

async fn dispatch(
    source: Arc<SharedSource>,
    senders: BTreeMap<RegionId, mpsc::Sender<Message>>,
    store: Arc<RunStore>,
    slots: Arc<Semaphore>,
) -> Result<()> {
    let mut batches = source.batches();
    while let Some(event) = batches.try_next().await? {
        store.check_cancelled()?;
        match event {
            SourceEvent::Batch {
                region_id,
                window,
                batch,
            } => {
                let buffer_store = store.clone();
                let run =
                    blocking(slots.clone(), move || Run::from_batch(batch, &buffer_store)).await?;
                let sender =
                    senders
                        .get(&region_id)
                        .context(InvalidMultiRegionCompactionSnafu {
                            reason: "Routed fragment names an unknown region",
                        })?;
                send(sender, Message::Batch { window, run }).await;
            }
            SourceEvent::SourceEnd => {
                for sender in senders.values() {
                    send(sender, Message::SourceEnd).await;
                }
            }
        }
    }
    for sender in senders.values() {
        send(sender, Message::Finish).await;
    }
    Ok(())
}

async fn send(sender: &mpsc::Sender<Message>, message: Message) {
    if sender.send(message).await.is_err() {
        // The supervisor polls every worker concurrently with dispatch. Let it
        // report the worker's original error/panic, not a racing channel error.
        futures::future::pending::<()>().await;
    }
}

async fn acquire_slot(slots: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    slots.clone().acquire_owned().await.map_err(|_| {
        InvalidMultiRegionCompactionSnafu {
            reason: "Compaction work semaphore closed",
        }
        .build()
    })
}

async fn blocking<T: Send + 'static>(
    slots: Arc<Semaphore>,
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let permit = acquire_slot(&slots).await?;
    common_runtime::spawn_blocking_compact(move || {
        let _permit = permit;
        operation()
    })
    .await
    .context(crate::error::JoinSnafu)?
}

struct RegionWorker {
    windows: BTreeMap<i64, WindowRuns>,
    completed_sources: usize,
    schema: SchemaRef,
    _reservation: Reservation,
    store: Arc<RunStore>,
}

impl RegionWorker {
    fn accept(&mut self, message: Message) -> Result<()> {
        self.store.check_cancelled()?;
        match message {
            Message::Batch { window, run } => {
                let window = match self.windows.entry(window) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(WindowRuns::new(&self.store)?)
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                };
                window.current.append(run, &self.store)?;
            }
            Message::SourceEnd => {
                for window in self.windows.values_mut() {
                    window.seal(&self.schema, &self.store)?;
                }
                self.completed_sources += 1;
            }
            Message::Finish => {}
        }
        Ok(())
    }
}

struct WindowRuns {
    current: Run,
    /// Occupied higher levels precede lower levels in the source order.
    levels: Vec<Option<Run>>,
    _reservation: Reservation,
}

impl WindowRuns {
    fn new(store: &RunStore) -> Result<Self> {
        // Includes all binary merge levels, not just the current run.
        let bytes = std::mem::size_of::<Self>()
            + 128
            + usize::BITS as usize * std::mem::size_of::<Option<Run>>();
        Ok(Self {
            current: Run::default(),
            levels: Vec::new(),
            _reservation: store.reserve_metadata(bytes)?,
        })
    }

    fn seal(&mut self, schema: &SchemaRef, store: &RunStore) -> Result<()> {
        let mut run = std::mem::take(&mut self.current);
        if run.is_empty() {
            return Ok(());
        }
        for level in &mut self.levels {
            let Some(previous) = level.take() else {
                *level = Some(run);
                return Ok(());
            };
            run = merge(previous, run, schema, store)?;
        }
        self.levels.push(Some(run));
        Ok(())
    }

    fn finish(self, schema: &SchemaRef, store: &RunStore) -> Result<Run> {
        ensure!(
            self.current.is_empty(),
            InvalidMultiRegionCompactionSnafu {
                reason: "Cannot merge an unfinished source",
            }
        );
        let mut result = None;
        for run in self.levels.into_iter().rev().flatten() {
            result = Some(match result {
                None => run,
                Some(previous) => merge(previous, run, schema, store)?,
            });
        }
        result.context(InvalidMultiRegionCompactionSnafu {
            reason: "Empty output window",
        })
    }
}

fn merge(left: Run, right: Run, schema: &SchemaRef, store: &RunStore) -> Result<Run> {
    store.check_cancelled()?;
    let iterator = FlatMergeIterator::new_stable(
        schema.clone(),
        vec![left.into_iter()?, right.into_iter()?],
        1024,
    )?;
    let mut output = Run::default();
    for batch in iterator {
        output.push(batch?, store)?;
    }
    Ok(output)
}

struct WorkerResources {
    plan: Arc<MultiRegionCompactionPlan>,
    context: Arc<MultiRegionCompactionContext>,
    slots: Arc<Semaphore>,
}

async fn run_worker(
    region_id: RegionId,
    mut state: RegionWorker,
    mut receiver: mpsc::Receiver<Message>,
    resources: Arc<WorkerResources>,
) -> Result<RegionOutput> {
    let WorkerResources {
        plan,
        context,
        slots,
    } = resources.as_ref();
    loop {
        let message = receiver
            .recv()
            .await
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Source channel closed without Finish",
            })?;
        if matches!(message, Message::Finish) {
            break;
        }
        state = blocking(slots.clone(), move || {
            state.accept(message)?;
            Ok(state)
        })
        .await?;
    }
    ensure!(
        state.completed_sources == plan.sources.len(),
        InvalidMultiRegionCompactionSnafu {
            reason: "Region worker did not receive every source boundary",
        }
    );
    let target = plan
        .targets
        .get(&region_id)
        .context(InvalidMultiRegionCompactionSnafu {
            reason: "Missing worker target",
        })?;
    let mut output = RegionOutput {
        region_id,
        files: Vec::new(),
        infos: Vec::new(),
    };
    while let Some((_, window)) = state.windows.pop_first() {
        let schema = state.schema.clone();
        let store = state.store.clone();
        let run = blocking(slots.clone(), move || window.finish(&schema, &store)).await?;
        // Writers share work slots with merges. No holder ever waits for a
        // channel or a not-yet-sealed source, even when there is only one slot.
        let permit = acquire_slot(slots).await?;
        let (mut iterator, permit) = common_runtime::spawn_blocking_compact(move || {
            Ok::<_, crate::error::Error>((run.into_iter()?, permit))
        })
        .await
        .context(crate::error::JoinSnafu)??;
        let store = state.store.clone();
        let stream = async_stream::try_stream! {
            loop {
                let checking = store.clone();
                let (next, returned) = common_runtime::spawn_blocking_compact(move || {
                    checking.check_cancelled()?;
                    let next = iterator.next().transpose()?;
                    Ok::<_, crate::error::Error>((next, iterator))
                }).await.context(crate::error::JoinSnafu)??;
                iterator = returned;
                let Some(batch) = next else { break; };
                yield batch;
            }
        };
        let source = FlatSource::new_stream(state.schema.clone(), Box::pin(stream));
        let (files, infos) = writer::write_target_group(target, source, plan, context).await?;
        drop(permit);
        output.files.extend(files);
        output.infos.extend(infos);
    }
    Ok(output)
}
