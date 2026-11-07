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

//! Incremental sinks for the already merged stream. All resident writers are
//! admitted together. No writer holds a work permit while waiting for input,
//! and no sink waits for another source head to consume its channel.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use datatypes::arrow::record_batch::RecordBatch;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use crate::compaction::multi_region::source::SharedSource;
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionOutput, MultiRegionCompactionPlan,
    MultiRegionCompactionStats, SourceCounters, writer,
};
use crate::error::{InvalidMultiRegionCompactionSnafu, Result};
use crate::read::FlatSource;

/// Transport closure is failure; only the dispatcher can signal successful EOF.
enum Message {
    Batch(RecordBatch),
    Finish,
}

pub(crate) async fn execute(
    plan: MultiRegionCompactionPlan,
    context: MultiRegionCompactionContext,
    source: Arc<SharedSource>,
    counters: Arc<SourceCounters>,
) -> Result<MultiRegionCompactionOutput> {
    let _cancel_on_drop = source.cancel_on_drop();
    let plan = Arc::new(plan);
    let context = Arc::new(context);
    let mut senders = BTreeMap::new();
    let mut workers = FuturesUnordered::new();
    for &id in plan.targets.keys() {
        let (sender, mut receiver) = mpsc::channel(1);
        senders.insert(id, sender);
        let schema = source.schema();
        let limits = source.limits.clone();
        let index_plans = source.index_plans.get(&id).cloned().unwrap_or_default();
        let plan = plan.clone();
        let context = context.clone();
        workers.push(AbortOnDropHandle::new(common_runtime::spawn_compact(
            async move {
                let stream = async_stream::try_stream! {
                    while let Message::Batch(batch) = receiver
                        .recv()
                        .await
                        .context(InvalidMultiRegionCompactionSnafu {
                            reason: "Source channel closed without Finish",
                        })?
                    {
                        yield batch;
                    }
                };
                let input = FlatSource::new_stream(schema, Box::pin(stream));
                let (files, infos) = writer::TargetWriter::new(
                    &plan.targets[&id],
                    &context,
                    &limits,
                    plan.max_output_file_size,
                    index_plans,
                )
                .write(input)
                .await?;
                Ok::<_, crate::error::Error>((id, files, infos))
            },
        )));
    }
    let scan = dispatch(source.clone(), senders);
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
                let (id, files, infos) = result.context(crate::error::JoinSnafu)??;
                output.stats.output_rows += files
                    .iter()
                    .map(|file| file.num_rows)
                    .sum::<u64>();
                output.stats.output_bytes += files
                    .iter()
                    .map(|file| file.file_size + file.index_file_size)
                    .sum::<u64>();
                output.files.insert(id, files);
                output.sst_infos.insert(id, infos);
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
    output.stats.estimated_memory_bytes = source.limits.estimated;
    output.stats.peak_group_bytes = source.peak_group_bytes();
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
) -> Result<()> {
    let mut batches = source.batches();
    while let Some(event) = batches.try_next().await? {
        let sender = senders
            .get(&event.region_id)
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Unknown routing target",
            })?;
        send(sender, Message::Batch(event.batch)).await;
    }
    for sender in senders.values() {
        send(sender, Message::Finish).await;
    }
    Ok(())
}

async fn send(sender: &mpsc::Sender<Message>, message: Message) {
    if sender.send(message).await.is_err() {
        // The supervisor reports the writer's original error, not a racing
        // channel-closed error. It also aborts siblings on failure or drop.
        futures::future::pending::<()>().await;
    }
}
