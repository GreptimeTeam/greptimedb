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

//! Lossless multi-region SST compaction using a captured table partition rule.
//!
//! The caller selects and pins the inputs (for example, from LEVEL_SHARED), and
//! supplies their effective metadata in every target. This primitive deliberately
//! does not select files, change existing persisted levels, publish manifests, or
//! fold row versions. Output protection and eventual publication belong to its
//! caller; see [MultiRegionCompactionOutput::files] for the output level convention.

mod memory;
mod source;
#[cfg(test)]
mod tests;
mod worker;
mod writer;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use common_time::Timestamp;
use partition::PartitionRuleRef;
use snafu::{OptionExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use tokio_util::sync::CancellationToken;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::config::MitoConfig;
use crate::error::CompactionCancelledSnafu;
use crate::error::{InvalidMultiRegionCompactionSnafu, Result};
use crate::region::options::RegionOptions;
use crate::sst::file::{FileMeta, RegionFileId};
use crate::sst::index::handler::IndexKindProviderRef;
use crate::sst::index::registry::IndexRegistry;
use crate::sst::parquet::SstInfo;
use crate::sst::parquet::writer::SstWriteGuard;

/// A target partition and the source versions currently visible in its sequence domain.
#[derive(Debug, Clone)]
pub struct MultiRegionTarget {
    pub metadata: RegionMetadataRef,
    pub options: RegionOptions,
    /// All selected sources, with the sequence metadata used by this target's reader.
    pub inputs: HashMap<RegionFileId, FileMeta>,
}

/// Immutable inputs, routing snapshot and execution limits for one compaction.
pub struct MultiRegionCompactionPlan {
    /// Captured table routing rule; the executor never fetches mutable routing metadata.
    pub partition_rule: PartitionRuleRef,
    /// Unique sources. The caller retains their GC protection.
    pub sources: Vec<FileMeta>,
    /// Metadata and routing must belong to the same physical-table snapshot.
    pub targets: BTreeMap<RegionId, MultiRegionTarget>,
    /// Total working-set admission budget for computation and all resident uploads.
    /// Upload chunks follow the destination backend's limits independently of the Arrow
    /// batch limit. The executor reserves uploads before sizing the compute buffers and
    /// checks the estimated working set before creating outputs. This is not a process RSS limit.
    pub memory_limit: usize,
    /// Soft size limit, in bytes, of one output SST. A file rolls over at the next
    /// series boundary after it is exceeded, so a single series may stay in one file.
    pub max_output_file_size: usize,
    /// Aligned start of the only task window, in seconds since the epoch.
    pub window_start: i64,
    /// Width of the only task window in seconds; the window is
    /// `[window_start, window_start + window_seconds)`.
    pub window_seconds: i64,
}

/// Shared storage services and attempt-owned output resources.
pub struct MultiRegionCompactionContext {
    /// Optional enterprise index implementation. Each routed output builds its own state.
    pub index_kind_provider: Option<IndexKindProviderRef>,
    pub access_layer: AccessLayerRef,
    pub cache: CacheManagerRef,
    pub config: Arc<MitoConfig>,
    pub protection: Arc<dyn SstWriteGuard>,
    pub cancellation: CancellationToken,
}

/// Source I/O and lossless output accounting, excluding footer and query reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiRegionCompactionStats {
    pub source_files: u64,
    /// Physical rows read from the source objects, before any reference drops a row.
    pub source_physical_rows: u64,
    /// Rows admitted by their owner's reference, i.e. rows this compaction kept.
    pub source_rows: u64,
    /// Row-group page bytes fetched from object storage, excluding footer traffic and retries.
    pub source_store_page_bytes: u64,
    pub output_rows: u64,
    pub output_bytes: u64,
    /// Admission estimate for reader pages, encoders, indexes, uploads, and finish workspace.
    pub estimated_memory_bytes: u64,
    /// Peak retained equal-key group storage (including shared buffer capacities).
    pub peak_group_bytes: u64,
}

/// Complete per-target outputs, including explicit empty partitions.
pub struct MultiRegionCompactionOutput {
    /// Output files, already assigned to region-local L0.
    ///
    /// Window merging and semantic folding stay with ordinary region compaction,
    /// which runs after publication; the caller does not adjust the level.
    pub files: BTreeMap<RegionId, Vec<FileMeta>>,
    /// Complete footer/index statistics for target hook delivery; not fabricated from FileMeta.
    pub sst_infos: BTreeMap<RegionId, Vec<SstInfo>>,
    pub stats: MultiRegionCompactionStats,
}

#[derive(Default)]
struct SourceCounters {
    files: AtomicU64,
    /// Rows admitted by their owner's reference.
    rows: AtomicU64,
    /// Rows physically present in the scanned objects, admitted or not.
    physical_rows: AtomicU64,
    store_page_bytes: AtomicU64,
}

/// Computes a complete result or an error. Any already-written outputs remain
/// owned by the attempt's durable protection; this function never purges sources.
/// Outputs that legacy reading would reinterpret as newly admitted versions are rejected.
pub async fn compact_regions(
    plan: MultiRegionCompactionPlan,
    context: MultiRegionCompactionContext,
) -> Result<MultiRegionCompactionOutput> {
    let metadata = validate_plan(&plan)?;
    // Invalid registrations still fail, but unavailable sketches only warn so
    // compaction can continue after a downgrade.
    let registry =
        IndexRegistry::from_config(&context.config, context.index_kind_provider.clone())?;
    for target in plan.targets.values() {
        registry.warn_missing_compaction_handlers(
            &target.metadata,
            plan.sources.iter().chain(target.inputs.values()),
        );
    }
    if context.cancellation.is_cancelled() {
        return CompactionCancelledSnafu.fail();
    }
    let cancellation = context.cancellation.clone();
    let execution = async move {
        let counters = Arc::new(SourceCounters::default());
        let source =
            source::SharedSource::build(&plan, &context, metadata, counters.clone()).await?;
        worker::execute(plan, context, source, counters).await
    };
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => crate::error::CompactionCancelledSnafu.fail(),
        result = execution => result,
    }
}

/// Rejects a plan whose routing would retire a row that a departing reference can
/// still read.
///
/// The source scan is physical and each row is routed to the one target its
/// partition selects, so a row can only be lost when the routed target's own
/// reference rejects it and every departing reference rejects it too. Deciding
/// that needs row values, so planning rejects the provable case here - a target
/// with a restricted reference next to a holder that authorizes the whole file -
/// and `SharedSource::authorize` verifies every remaining dropped row against all
/// holders, failing the compaction rather than losing it.
fn validate_row_authorizations(plan: &MultiRegionCompactionPlan) -> Result<()> {
    for file in &plan.sources {
        let id = file.file_id();
        for (target_id, target) in &plan.targets {
            let Some(input) = target.inputs.get(&id) else {
                continue;
            };
            // A target with an unrestricted reference drops nothing.
            if input.owned_range.is_none() {
                continue;
            }
            for (holder_id, holder) in &plan.targets {
                if holder_id == target_id {
                    continue;
                }
                let Some(holder_input) = holder.inputs.get(&id) else {
                    continue;
                };
                ensure!(
                    holder_input.owned_range.is_some(),
                    InvalidMultiRegionCompactionSnafu {
                        reason: format!(
                            "Source file {id} is unrestricted in region {holder_id} but restricted in region {target_id}; a row routed to {target_id} could be retired while {holder_id} can still read it"
                        ),
                    }
                );
            }
        }
    }
    Ok(())
}

fn validate_plan(plan: &MultiRegionCompactionPlan) -> Result<RegionMetadataRef> {
    ensure!(
        plan.targets
            .values()
            .all(|target| target.options.sst_format != Some(crate::sst::FormatType::Narthex)),
        InvalidMultiRegionCompactionSnafu {
            reason: "Table compaction does not support Narthex storage"
        }
    );
    ensure!(
        !plan.sources.is_empty(),
        InvalidMultiRegionCompactionSnafu {
            reason: "sources must not be empty"
        }
    );
    ensure!(
        plan.memory_limit >= 1024 * 1024,
        InvalidMultiRegionCompactionSnafu {
            reason: "memory_limit must be at least 1 MiB"
        }
    );
    ensure!(
        plan.max_output_file_size > 0,
        InvalidMultiRegionCompactionSnafu {
            reason: "max_output_file_size must be greater than 0"
        }
    );
    ensure!(
        plan.window_seconds > 0,
        InvalidMultiRegionCompactionSnafu {
            reason: "window_seconds must be greater than 0"
        }
    );
    let control_bytes = plan.targets.len().saturating_mul(8 * 1024).saturating_add(
        plan.sources
            .len()
            .saturating_mul(std::mem::size_of::<FileMeta>()),
    );
    ensure!(
        control_bytes <= plan.memory_limit,
        crate::error::MultiRegionCompactionResourceSnafu {
            reason: "Input/target control state exceeds the task budget",
        }
    );
    let first = plan
        .targets
        .values()
        .next()
        .context(InvalidMultiRegionCompactionSnafu {
            reason: "No routing targets",
        })?;
    let window_end = plan.window_start.checked_add(plan.window_seconds).context(
        InvalidMultiRegionCompactionSnafu {
            reason: "Task window overflow",
        },
    )?;
    ensure!(
        plan.window_start.rem_euclid(plan.window_seconds) == 0,
        InvalidMultiRegionCompactionSnafu {
            reason: "Task window start is not aligned"
        }
    );
    let start = Timestamp::new_second(plan.window_start);
    let end = Timestamp::new_second(window_end);
    let mut sources = HashSet::new();
    for file in &plan.sources {
        ensure!(
            file.time_range.0 >= start
                && file.time_range.1 < end
                && file.time_range.0 <= file.time_range.1,
            InvalidMultiRegionCompactionSnafu {
                reason: "Source is not wholly inside the task window"
            }
        );
        ensure!(
            file.region_id.table_id() == first.metadata.region_id.table_id()
                && sources.insert(file.file_id()),
            InvalidMultiRegionCompactionSnafu {
                reason: "Expected unique source files from one physical table"
            }
        );
    }
    for (id, target) in &plan.targets {
        target.options.validate()?;
        ensure!(
            *id == target.metadata.region_id
                && id.table_id() == first.metadata.region_id.table_id()
                && target.metadata.column_metadatas == first.metadata.column_metadatas
                && target.metadata.primary_key == first.metadata.primary_key
                && target.metadata.primary_key_encoding == first.metadata.primary_key_encoding,
            InvalidMultiRegionCompactionSnafu {
                reason: "Target schemas are not a consistent physical-table snapshot"
            }
        );
        ensure!(
            target.inputs.len() == sources.len()
                && target
                    .inputs
                    .iter()
                    .all(|(key, file)| *key == file.file_id() && sources.contains(key)),
            InvalidMultiRegionCompactionSnafu {
                reason: "Every target must describe the effective metadata of every selected source"
            }
        );
    }
    validate_row_authorizations(plan)?;
    Ok(first.metadata.clone())
}
