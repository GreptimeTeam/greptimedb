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

//! Experimental key-partitioned streaming merge. Each input has one forward-only
//! reader. Each nonempty destination has one bounded channel to its final SST writer.
use super::{
    MultiRegionCompactionContext, MultiRegionCompactionOutput, MultiRegionCompactionPlan,
    MultiRegionCompactionStats, SourceCounters, source::SharedSource, writer,
};
use crate::error::{self, InvalidMultiRegionCompactionSnafu as Invalid, Result};
use crate::read::{FlatSource, flat_merge::FlatMergeReader};
use datatypes::arrow::array::UInt64Array;
use datatypes::arrow::compute::take_record_batch;
use snafu::{ResultExt, ensure};
use std::collections::BTreeMap;
use std::sync::{Arc, atomic::Ordering};
use store_api::storage::RegionId;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

mod writer_block;
use writer_block::{Packet, Sink};

pub(super) async fn execute(
    plan: MultiRegionCompactionPlan,
    context: MultiRegionCompactionContext,
    source: Arc<SharedSource>,
    counters: Arc<SourceCounters>,
) -> Result<MultiRegionCompactionOutput> {
    let metadata = &plan.targets.first_key_value().unwrap().1.metadata;
    ensure!(
        plan.partition_rule
            .partition_columns()
            .iter()
            .all(|name| metadata
                .column_by_name(name)
                .is_some_and(|col| col.semantic_type == api::v1::SemanticType::Tag)),
        Invalid {
            reason: "Streaming compaction requires key-based partitioning"
        }
    );
    let target_rows = std::env::var("TESTBED_SHARED_WRITER_BLOCK_ROWS")
        .map_or(Ok(1usize), |s| s.parse())
        .map_err(|_| {
            Invalid {
                reason: "invalid writer block row target",
            }
            .build()
        })?;
    let limit = plan.targets.len() * 2;
    let batch_limit = plan.memory_limit / (plan.sources.len() + limit * 3 + 2);
    let mut output = MultiRegionCompactionOutput {
        files: plan.targets.keys().map(|id| (*id, vec![])).collect(),
        sst_infos: plan.targets.keys().map(|id| (*id, vec![])).collect(),
        stats: MultiRegionCompactionStats::default(),
    };
    let start = std::time::Instant::now();
    let mut writer_wait = std::time::Duration::ZERO;
    let mut merge = FlatMergeReader::new_stable(
        source.schema(),
        plan.sources
            .iter()
            .map(|f| source.clone().ordered_input(f.clone()))
            .collect(),
        1024,
    )
    .await?;
    let plan = Arc::new(plan);
    let context = Arc::new(context);
    let mut sinks = BTreeMap::new();
    let mut workers = JoinSet::new();
    while let Some(batch) = merge.next_batch().await? {
        if context.cancellation.is_cancelled() {
            return error::CompactionCancelledSnafu.fail();
        }
        ensure!(
            batch.get_array_memory_size() <= batch_limit,
            Invalid {
                reason: "Streaming merge batch exceeds resident slot budget"
            }
        );
        let mut groups: BTreeMap<(RegionId, i64), Vec<u64>> = BTreeMap::new();
        for (row, key) in source
            .row_owners(&batch)?
            .into_iter()
            .zip(source.time_windows(&batch)?)
            .enumerate()
        {
            groups.entry(key).or_default().push(row as u64);
        }
        for (key, rows) in groups {
            if !sinks.contains_key(&key) {
                ensure!(
                    sinks.len() < limit,
                    Invalid {
                        reason: "Too many active region/window streams"
                    }
                );
                let (tx, mut rx) = mpsc::channel::<Packet>(1);
                let schema = source.schema();
                let plan = plan.clone();
                let context = context.clone();
                workers.spawn(async move {
                    let batches=async_stream::try_stream! { while let Some(packet)=rx.recv().await { for batch in packet { yield batch; } } };
                    let source=FlatSource::new_stream(schema,Box::pin(batches));
                    let (files,infos)=writer::write_target_group(&plan.targets[&key.0],source,&plan,&context).await?;
                    Ok::<_,error::Error>((key.0,files,infos))
                });
                sinks.insert(key, Sink::new(tx, target_rows, batch_limit));
            }
            let fragment = take_record_batch(&batch, &UInt64Array::from(rows))
                .context(error::ComputeArrowSnafu)?;
            if !sinks
                .get_mut(&key)
                .unwrap()
                .push(fragment, &mut writer_wait)
                .await
            {
                if let Some(done) = workers.join_next().await {
                    done.context(error::JoinSnafu)??;
                }
                return Invalid {
                    reason: "Output writer stopped before end of input",
                }
                .fail();
            }
        }
    }
    for sink in sinks.values_mut() {
        if !sink.flush(&mut writer_wait).await {
            if let Some(done) = workers.join_next().await {
                done.context(error::JoinSnafu)??;
            }
            return Invalid {
                reason: "Output writer stopped before end of input",
            }
            .fail();
        }
    }
    let drain = std::time::Instant::now();
    drop(sinks);
    while let Some(done) = workers.join_next().await {
        let (id, files, infos) = done.context(error::JoinSnafu)??;
        output.stats.output_rows += files.iter().map(|f| f.num_rows).sum::<u64>();
        output.stats.output_bytes += files
            .iter()
            .map(|f| f.file_size + f.index_file_size)
            .sum::<u64>();
        output.files.get_mut(&id).unwrap().extend(files);
        output.sst_infos.get_mut(&id).unwrap().extend(infos);
    }
    let writer_drain = drain.elapsed();
    output.stats.write_wait_us = writer_wait.as_micros() as u64;
    output.stats.write_drain_us = writer_drain.as_micros() as u64;
    output.stats.merge_route_us = (start.elapsed() - writer_drain - writer_wait).as_micros() as u64;
    output.stats.source_files = counters.files.load(Ordering::Relaxed);
    output.stats.source_rows = counters.rows.load(Ordering::Relaxed);
    output.stats.source_store_page_bytes = counters.store_page_bytes.load(Ordering::Relaxed);
    ensure!(
        output.stats.source_rows == output.stats.output_rows
            && output.stats.source_files == plan.sources.len() as u64,
        Invalid {
            reason: "Streaming compaction did not account for all input"
        }
    );
    Ok(output)
}
