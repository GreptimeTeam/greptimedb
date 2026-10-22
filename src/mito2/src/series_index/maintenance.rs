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

//! Worker-owned series-index reconciliation and publication.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_telemetry::{debug, info};
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};

use crate::error::Result;
use crate::metrics::{SERIES_INDEX_RECONCILE_ELAPSED, SERIES_INDEX_RECONCILE_TOTAL};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::version::VersionRef;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::series_index::bucket::{
    group_files_into_series_buckets, plan_series_indexes_with_mode, rounded_bucket_width,
};
use crate::series_index::builder::{build_range_index, build_series_index};
use crate::series_index::catalog::{
    RangeIndexCatalog, SeriesIndexCatalog, delete_catalogs, range_catalog_path,
    series_catalog_path, store_catalog,
};
use crate::series_index::purger::IndexFilePurger;
use crate::series_index::version::{SeriesIndexFileHandle, SeriesIndexVersion};

/// Retires newly completed series files unless their snapshot is published.
#[derive(Default)]
struct UnpublishedSeriesFiles(
    Vec<SeriesIndexFileHandle>,
    Vec<crate::series_index::artifact::SamplesFileHandle>,
);

impl UnpublishedSeriesFiles {
    fn disarm(&mut self) {
        self.0.clear();
        self.1.clear();
    }
}

impl Drop for UnpublishedSeriesFiles {
    fn drop(&mut self) {
        for handle in &self.1 {
            handle.mark_deleted();
        }
        for handle in &self.0 {
            handle.mark_deleted();
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReconcileStats {
    pub(crate) built_samples: usize,
    pub(crate) removed_samples: usize,
    pub(crate) source_files: usize,
    pub(crate) built_range: usize,
    pub(crate) built_series: usize,
    pub(crate) removed_range: usize,
    pub(crate) removed_series: usize,
    pub(crate) computed_buckets: usize,
    pub(crate) skipped_buckets: usize,
}

impl ReconcileStats {
    fn changed(&self) -> bool {
        self.built_samples
            + self.removed_samples
            + self.built_range
            + self.built_series
            + self.removed_range
            + self.removed_series
            > 0
    }
}

/// Reconciles indexes for one region snapshot, persists catalogs, then atomically publishes it.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn reconcile_series_indexes(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
    enable_range_index: bool,
    allow_builds: bool,
) -> Result<ReconcileStats> {
    reconcile_series_indexes_with_mode(
        worker_id,
        store,
        region,
        requested_bucket_width,
        now_ms,
        purger,
        enable_range_index,
        allow_builds,
        false,
    )
    .await
}

/// Manual reconciliation bypasses maintenance eligibility, but not capacity limits.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconcile_series_indexes_with_mode(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
    enable_range_index: bool,
    allow_builds: bool,
    manual: bool,
) -> Result<ReconcileStats> {
    let total_start = Instant::now();
    // Use this snapshot throughout reconciliation, even if the region version advances.
    let version = region.version_control.current().version;
    if !is_sparse_metric_metadata(&version.metadata) {
        SERIES_INDEX_RECONCILE_TOTAL
            .with_label_values(&["noop"])
            .inc();
        return Ok(ReconcileStats::default());
    }
    let build_start = Instant::now();
    let mut unpublished = UnpublishedSeriesFiles::default();
    let (next, mut stats) = build_index_version(
        worker_id,
        &store,
        &region,
        &version,
        requested_bucket_width,
        now_ms,
        &purger,
        &mut unpublished,
        enable_range_index,
        allow_builds,
        manual,
    )
    .await?;
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["build"])
        .observe(build_start.elapsed().as_secs_f64());
    // Persist changed catalogs before making the new snapshot visible to readers.
    let publish_result: Result<()> = async {
        if let Some(next) = next {
            persist_index_catalogs(&store, region.region_id, &next, &stats).await?;
            publish_index_version(&region, Arc::new(next));
            unpublished.disarm();
        }
        Ok(())
    }
    .await;
    // Drop may have cleaned up while catalogs were being written, even if a write failed.
    // If dropping starts after this check, the normal drop path cleans up our publication.
    // This assumes the region ID is not reopened or replaced during cleanup.
    if region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping) {
        delete_catalogs(&store, region.region_id).await;
        region.series_index_version_control.mark_dropped();
        stats = ReconcileStats::default();
    }
    publish_result?;
    let result = if stats.changed() { "changed" } else { "noop" };
    SERIES_INDEX_RECONCILE_TOTAL
        .with_label_values(&[result])
        .inc();
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["total"])
        .observe(total_start.elapsed().as_secs_f64());
    if stats.changed() {
        info!(
            "Reconciled series-index snapshot, worker: {worker_id}, region: {}, elapsed: {:?}, stats: {:?}",
            region.region_id,
            total_start.elapsed(),
            stats
        );
    } else {
        debug!(
            "Series-index reconciliation made no changes, worker: {worker_id}, region: {}",
            region.region_id
        );
    }
    Ok(stats)
}

/// Builds a changed snapshot, retaining reusable indexes and removing obsolete coverage.
/// Returns `None` when no range or series indexes were added or removed.
#[allow(clippy::too_many_arguments)]
async fn build_index_version(
    worker_id: u32,
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: &IndexFilePurger,
    unpublished: &mut UnpublishedSeriesFiles,
    enable_range_index: bool,
    allow_builds: bool,
    manual: bool,
) -> Result<(Option<SeriesIndexVersion>, ReconcileStats)> {
    let mut stats = ReconcileStats::default();
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .cloned()
        .collect::<Vec<_>>();
    stats.source_files = files.len();
    let visible = files
        .iter()
        .map(|file| file.file_id().file_id())
        .collect::<HashSet<_>>();
    let current = region.series_index_version();
    // Empty build inputs still let the planner expire established bucket coverage.
    let buckets = match (allow_builds, version.compaction_time_window) {
        (true, Some(window)) => rounded_bucket_width(requested_bucket_width, window)
            // Successful rounding guarantees the window fits in i64; subsecond windows
            // use the same one-second minimum as rounded_bucket_width.
            .map(|width| {
                group_files_into_series_buckets(&files, width, (window.as_secs() as i64).max(1))
            })
            .unwrap_or_default(),
        (true, None) if manual => {
            let width = i64::try_from(requested_bucket_width.as_secs())
                .unwrap_or(i64::MAX)
                .max(1);
            group_files_into_series_buckets(&files, width, width)
        }
        (true, None) => {
            debug!(
                "Deferring series indexes without compaction window, worker: {worker_id}, region: {}",
                region.region_id
            );
            Vec::new()
        }
        (false, _) => Vec::new(),
    };
    let formats = region
        .series_index_version_control
        .formats
        .read()
        .unwrap()
        .clone();
    let selected_format = formats.index.as_ref().map_or(
        crate::series_index::artifact::ArtifactFormat::Parquet,
        |backend| backend.format(),
    );
    let mut unusable = HashSet::new();
    for handle in current.series_indexes.values() {
        let entry = handle.entry();
        let mut reusable = entry.format() == selected_format;
        // Legacy Parquet indexes retain their existing maintenance behavior.
        if reusable && let Some(artifact) = &entry.artifact {
            for object in &artifact.objects {
                reusable &= store
                    .exists(&object.path)
                    .await
                    .context(crate::error::OpenDalSnafu)?;
            }
        }
        if manual && reusable && entry.artifact.is_none() {
            reusable = store
                .exists(&crate::series_index::catalog::series_index_path(
                    region.region_id,
                    entry.index_uuid,
                ))
                .await
                .context(crate::error::OpenDalSnafu)?;
        }
        if !reusable {
            unusable.insert(entry.index_uuid);
        }
    }
    let mut reusable_samples = HashSet::new();
    if let Some(backend) = &formats.samples {
        for (id, handle) in &current.samples {
            if !visible.contains(id) || handle.entry().artifact.format != backend.format() {
                continue;
            }
            let mut exists = true;
            for object in &handle.entry().artifact.objects {
                exists &= store
                    .exists(&object.path)
                    .await
                    .context(crate::error::OpenDalSnafu)?;
            }
            if exists {
                reusable_samples.insert(*id);
            }
        }
    }
    let mut index_buckets = current.index_buckets.clone();
    for bucket in index_buckets.values_mut() {
        if bucket.index_ids.iter().any(|id| unusable.contains(id)) {
            // Force replacement only for the affected bucket. Retain its geometry and
            // handles until a replacement is built and its catalog is published.
            bucket.compaction_window_secs = 0;
            bucket.window_sequences.clear();
        }
    }
    let plan =
        plan_series_indexes_with_mode(buckets, index_buckets, version.options.ttl, now_ms, manual);
    stats.computed_buckets = plan.computed_buckets;
    stats.skipped_buckets = plan.skipped_buckets;
    let mut next = prune_index_version(&current, &visible, &plan.expired_index_ids, &mut stats);
    if !allow_builds {
        if manual
            && files.iter().any(|file| {
                !current.series_indexes.values().any(|handle| {
                    handle.entry().format() == selected_format
                        && !unusable.contains(&handle.entry().index_uuid)
                        && handle
                            .entry()
                            .covers_file(file.meta_ref(), region.region_id)
                }) || (formats.samples.is_some()
                    && !reusable_samples.contains(&file.file_id().file_id()))
            })
        {
            return crate::error::InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "series-index capacity limit prevents complete manual coverage",
            }
            .fail();
        }
        if let Some(next) = &mut next {
            next.index_buckets = plan.index_buckets;
        }
        return Ok((next, stats));
    }
    if !manual
        && plan.builds.is_empty()
        && next.is_none()
        && formats.samples.is_none()
        && current.samples.is_empty()
        && (!enable_range_index || current.range_indexes.len() == visible.len())
    {
        return Ok((None, stats));
    }
    let SeriesIndexVersion {
        mut range_indexes,
        mut series_indexes,
        mut samples,
        ..
    } = next.unwrap_or_else(|| SeriesIndexVersion {
        samples: current.samples.clone(),
        range_indexes: current.range_indexes.clone(),
        series_indexes: current.series_indexes.clone(),
        index_buckets: Default::default(),
    });
    let index_buckets = plan.index_buckets;
    if formats.samples.is_none() {
        stats.removed_samples += samples.len();
        samples.clear();
    }
    if let Some(backend) = &formats.samples {
        for file in &files {
            let id = file.file_id().file_id();
            let reusable = reusable_samples.contains(&id);
            if reusable {
                continue;
            }
            if let Some(handle) = crate::series_index::builder::build_samples(
                store,
                region,
                version,
                file.clone(),
                backend.as_ref(),
                purger,
            )
            .await?
            {
                unpublished.1.push(handle.clone());
                samples.insert(id, handle);
                stats.built_samples += 1;
            } else if manual {
                return crate::error::InvalidRequestSnafu {
                    region_id: region.region_id,
                    reason: format!("SST {id} has no readable input for samples"),
                }
                .fail();
            }
        }
    }
    for id in &plan.superseded_index_ids {
        series_indexes.remove(id);
    }
    for (bucket, expected) in plan.builds {
        // Complete companion indexes independently so a failed series build preserves them.
        for file in &bucket.files {
            if enable_range_index
                && !range_indexes.contains_key(&file.file_id().file_id())
                && let Some(entry) = build_range_index(store, region, version, file.clone()).await?
            {
                stats.built_range += 1;
                range_indexes.insert(entry.file_id, entry);
            }
        }
        let series_handle = build_series_index(
            store,
            region,
            version,
            &bucket,
            &expected,
            formats.index.as_deref(),
            purger,
        )
        .await?;
        unpublished.0.push(series_handle.clone());
        stats.built_series += 1;
        series_indexes.insert(expected.index_uuid, series_handle);
    }
    // Cover SSTs outside planned aggregate builds, including skipped buckets.
    if enable_range_index {
        for file in &files {
            let file_id = file.file_id().file_id();
            if range_indexes.contains_key(&file_id) {
                continue;
            }
            if let Some(entry) = build_range_index(store, region, version, file.clone()).await? {
                stats.built_range += 1;
                range_indexes.insert(entry.file_id, entry);
            }
        }
    }
    if manual
        && files.iter().any(|file| {
            !series_indexes.values().any(|handle| {
                handle.entry().format() == selected_format
                    && handle
                        .entry()
                        .covers_file(file.meta_ref(), region.region_id)
            }) || (formats.samples.is_some() && !samples.contains_key(&file.file_id().file_id()))
        })
    {
        return crate::error::InvalidRequestSnafu {
            region_id: region.region_id,
            reason: "manual series-index build did not cover every SST",
        }
        .fail();
    }
    stats.removed_series = current
        .series_indexes
        .keys()
        .filter(|id| !series_indexes.contains_key(id))
        .count();
    // Bucket reconciliation is speculative until indexes change. Recompute its coverage
    // next time rather than replacing the published snapshot on a no-op pass.
    if !stats.changed() {
        return Ok((None, stats));
    }
    let next = SeriesIndexVersion {
        samples,
        range_indexes,
        series_indexes,
        index_buckets,
    };
    Ok((Some(next), stats))
}

/// Prunes obsolete metadata without building indexes or retiring published handles.
/// Returns `None` without cloning the current version when no entries need removal.
/// Physical range deletion belongs to the SST purger; series handles are retired only
/// after the cleaned catalogs and snapshot have been published successfully.
fn prune_index_version(
    current: &SeriesIndexVersion,
    visible: &HashSet<FileId>,
    expired_index_ids: &[FileId],
    stats: &mut ReconcileStats,
) -> Option<SeriesIndexVersion> {
    if current.samples.keys().all(|id| visible.contains(id))
        && current.range_indexes.keys().all(|id| visible.contains(id))
        && expired_index_ids
            .iter()
            .all(|id| !current.series_indexes.contains_key(id))
    {
        return None;
    }
    let mut next = SeriesIndexVersion {
        samples: current.samples.clone(),
        range_indexes: current.range_indexes.clone(),
        series_indexes: current.series_indexes.clone(),
        index_buckets: current.index_buckets.clone(),
    };
    next.samples.retain(|id, _| visible.contains(id));
    stats.removed_samples = current.samples.len() - next.samples.len();
    next.range_indexes
        .retain(|file_id, _| visible.contains(file_id));
    for id in expired_index_ids {
        next.series_indexes.remove(id);
    }
    stats.removed_range = current.range_indexes.len() - next.range_indexes.len();
    stats.removed_series = current.series_indexes.len() - next.series_indexes.len();
    Some(next)
}

/// Writes changed catalogs in a stable order; the two writes are not atomic together.
async fn persist_index_catalogs(
    store: &ObjectStore,
    region_id: RegionId,
    next: &SeriesIndexVersion,
    stats: &ReconcileStats,
) -> Result<()> {
    let samples_path = crate::series_index::catalog::samples_catalog_path(region_id);
    if !next.samples.is_empty()
        || stats.removed_samples > 0
        || store
            .exists(&samples_path)
            .await
            .context(crate::error::OpenDalSnafu)?
    {
        // A single object write commits all references. A failed write leaves the prior
        // catalog usable while the unpublished-output guard retires the new artifacts.
        return store_catalog(
            store,
            &samples_path,
            &crate::series_index::catalog::SamplesCatalog {
                entries: next.samples.values().map(|h| h.entry().clone()).collect(),
                series: next
                    .series_indexes
                    .values()
                    .map(|h| h.entry().clone())
                    .collect(),
                ranges: next.range_indexes.values().copied().collect(),
            },
        )
        .await;
    }
    if stats.built_range + stats.removed_range > 0 {
        let mut range_entries = next.range_indexes.values().copied().collect::<Vec<_>>();
        range_entries
            .sort_unstable_by(|left, right| left.file_id.as_bytes().cmp(right.file_id.as_bytes()));
        store_catalog(
            store,
            &range_catalog_path(region_id),
            &RangeIndexCatalog {
                indexes: range_entries,
            },
        )
        .await?;
    }
    if stats.built_series + stats.removed_series > 0 {
        let mut series_entries = next
            .series_indexes
            .values()
            .map(|handle| handle.entry().clone())
            .collect::<Vec<_>>();
        series_entries.sort_unstable_by_key(|entry| {
            (
                entry.bucket_start,
                entry.bucket_end,
                entry.min_file_sequence,
                entry.max_file_sequence,
            )
        });
        store_catalog(
            store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog {
                indexes: series_entries,
            },
        )
        .await?;
    }
    Ok(())
}

/// Publishes the snapshot and retires series files absent from the new version.
fn publish_index_version(region: &MitoRegionRef, next: Arc<SeriesIndexVersion>) {
    let previous = region.series_index_version_control.publish(next.clone());
    for (id, handle) in &previous.samples {
        if next.samples.get(id).is_none_or(|next| {
            next.entry().artifact.generation != handle.entry().artifact.generation
        }) {
            handle.mark_deleted();
        }
    }
    for (id, handle) in &previous.series_indexes {
        if !next.series_indexes.contains_key(id) {
            // Purge only after readers release their retained handles.
            handle.mark_deleted();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_time::Timestamp;
    use object_store::services::Memory;

    use super::*;
    use crate::series_index::catalog::{RangeIndexEntry, SeriesIndexEntry};
    use crate::series_index::purger::series_index_channel;

    #[rstest::rstest]
    fn test_prune_index_version(
        #[values(false, true)] remove_range: bool,
        #[values(false, true)] remove_series: bool,
    ) {
        let range_id = FileId::random();
        let series_id = FileId::random();
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, mut receiver) = series_index_channel(store);
        let current = SeriesIndexVersion::new(
            HashMap::from([(
                range_id,
                RangeIndexEntry {
                    file_id: range_id,
                    file_size: 10,
                },
            )]),
            HashMap::from([(
                series_id,
                SeriesIndexFileHandle::new(
                    RegionId::new(1, 1),
                    SeriesIndexEntry {
                        artifact: None,
                        index_uuid: series_id,
                        file_size: 20,
                        bucket_start: Timestamp::new_second(0),
                        bucket_end: Timestamp::new_second(100),
                        source_file_ids: vec![range_id],
                        min_file_sequence: 1,
                        max_file_sequence: 1,
                        compaction_window_secs: 100,
                        window_sequences: Default::default(),
                    },
                    purger,
                ),
            )]),
        );
        let visible = if remove_range {
            HashSet::new()
        } else {
            HashSet::from([range_id])
        };
        // Unknown and repeated IDs must not inflate removal counts.
        let mut expired = vec![FileId::random()];
        if remove_series {
            expired.extend([series_id, series_id]);
        }
        let mut stats = ReconcileStats::default();
        let next = prune_index_version(&current, &visible, &expired, &mut stats);
        assert_eq!(remove_range || remove_series, next.is_some());
        assert_eq!(usize::from(remove_range), stats.removed_range);
        assert_eq!(usize::from(remove_series), stats.removed_series);
        if let Some(next) = next {
            assert_eq!(!remove_range, next.range_indexes.contains_key(&range_id));
            assert_eq!(!remove_series, next.series_indexes.contains_key(&series_id));
        }
        assert_eq!(1, current.range_indexes.len());
        assert_eq!(1, current.series_indexes.len());
        drop(current);
        // Pruning alone must never retire published files.
        assert!(receiver.try_recv().is_err());
    }
}
