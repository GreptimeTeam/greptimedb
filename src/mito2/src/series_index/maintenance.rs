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
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metrics::{SERIES_INDEX_RECONCILE_ELAPSED, SERIES_INDEX_RECONCILE_TOTAL};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::MitoRegionRef;
use crate::region::version::VersionRef;
use crate::series_index::bucket::{
    group_files_into_series_buckets, plan_series_indexes, rounded_bucket_width,
};
use crate::series_index::builder::{build_range_index, build_series_index};
use crate::series_index::catalog::{
    RangeIndexCatalog, SeriesIndexCatalog, range_catalog_path, series_catalog_path, store_catalog,
};
use crate::series_index::purger::IndexFilePurger;
use crate::series_index::version::{SeriesIndexFileHandle, SeriesIndexVersion};

/// Retires newly completed series files unless their snapshot is published.
#[derive(Default)]
struct UnpublishedSeriesFiles(Vec<SeriesIndexFileHandle>);

impl UnpublishedSeriesFiles {
    fn disarm(&mut self) {
        self.0.clear();
    }
}

impl Drop for UnpublishedSeriesFiles {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.mark_deleted();
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReconcileStats {
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
        self.built_range + self.built_series + self.removed_range + self.removed_series > 0
    }
}

/// Reconciles indexes for one region snapshot, persists catalogs, then atomically publishes it.
pub(crate) async fn reconcile_series_indexes(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
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
    let (next, stats) = build_index_version(
        worker_id,
        &store,
        &region,
        &version,
        requested_bucket_width,
        now_ms,
        &purger,
        &mut unpublished,
    )
    .await?;
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["build"])
        .observe(build_start.elapsed().as_secs_f64());
    // Persist both catalogs before making the new snapshot visible to readers.
    if let Some(next) = next {
        persist_index_catalogs(&store, region.region_id, &next).await?;
        publish_index_version(&region, Arc::new(next));
    }
    unpublished.disarm();
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
    // The SST purger deletes companion range files after final handle release. Prune
    // metadata here using the captured SST snapshot, independently of physical deletion;
    // a later region-version change is picked up by the next reconciliation.
    stats.removed_range = current.range_indexes.difference(&visible).count();
    let buckets = match version.compaction_time_window {
        Some(window) => rounded_bucket_width(requested_bucket_width, window)
            // Successful rounding guarantees the window fits in i64; subsecond windows
            // use the same one-second minimum as rounded_bucket_width.
            .map(|width| {
                group_files_into_series_buckets(&files, width, (window.as_secs() as i64).max(1))
            })
            .unwrap_or_default(),
        None => {
            debug!(
                "Deferring series indexes without compaction window, worker: {worker_id}, region: {}",
                region.region_id
            );
            Vec::new()
        }
    };
    let plan = plan_series_indexes(
        buckets,
        current.index_buckets.clone(),
        version.options.ttl,
        now_ms,
    );
    stats.computed_buckets = plan.computed_buckets;
    stats.skipped_buckets = plan.skipped_buckets;
    if plan.builds.is_empty()
        && plan.expired_index_ids.is_empty()
        && stats.removed_range == 0
        && current.range_indexes.len() == visible.len()
    {
        return Ok((None, stats));
    }
    let mut range_indexes = current.range_indexes.clone();
    range_indexes.retain(|file_id| visible.contains(file_id));
    let mut series_indexes = current.series_indexes.clone();
    for id in plan
        .expired_index_ids
        .iter()
        .chain(&plan.superseded_index_ids)
    {
        series_indexes.remove(id);
    }
    for (bucket, expected) in plan.builds {
        // Complete companion indexes independently so a failed series build preserves them.
        for file in &bucket.files {
            if !range_indexes.contains(&file.file_id().file_id())
                && let Some(file_id) =
                    build_range_index(store, region, version, file.clone()).await?
            {
                stats.built_range += 1;
                range_indexes.insert(file_id);
            }
        }
        let series_handle =
            build_series_index(store, region, version, &bucket, &expected, purger).await?;
        unpublished.0.push(series_handle.clone());
        stats.built_series += 1;
        series_indexes.insert(expected.index_uuid, series_handle);
    }
    // Cover SSTs outside planned aggregate builds, including skipped buckets.
    for file in files {
        let file_id = file.file_id().file_id();
        if range_indexes.contains(&file_id) {
            continue;
        }
        if let Some(file_id) = build_range_index(store, region, version, file).await? {
            stats.built_range += 1;
            range_indexes.insert(file_id);
        }
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
        range_indexes,
        series_indexes,
        index_buckets: plan.index_buckets,
    };
    Ok((Some(next), stats))
}

/// Writes catalogs in a stable order; the two writes are not atomic together.
async fn persist_index_catalogs(
    store: &ObjectStore,
    region_id: RegionId,
    next: &SeriesIndexVersion,
) -> Result<()> {
    let mut range_entries = next.range_indexes.iter().copied().collect::<Vec<_>>();
    range_entries.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
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
        &range_catalog_path(region_id),
        &RangeIndexCatalog {
            indexes: range_entries,
        },
    )
    .await?;
    store_catalog(
        store,
        &series_catalog_path(region_id),
        &SeriesIndexCatalog {
            indexes: series_entries,
        },
    )
    .await?;
    Ok(())
}

/// Publishes the snapshot and retires series files absent from the new version.
fn publish_index_version(region: &MitoRegionRef, next: Arc<SeriesIndexVersion>) {
    let previous = region.series_index_version_control.publish(next.clone());
    for (id, handle) in &previous.series_indexes {
        if !next.series_indexes.contains_key(id) {
            // Purge only after readers release their retained handles.
            handle.mark_deleted();
        }
    }
}
