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
use common_time::Timestamp;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metrics::{SERIES_INDEX_RECONCILE_ELAPSED, SERIES_INDEX_RECONCILE_TOTAL};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::version::VersionRef;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::series_index::bucket::{
    group_files_into_series_buckets, plan_series_indexes, rounded_bucket_width,
};
use crate::series_index::builder::{
    build_range_index, build_range_index_with_budget, build_series_index,
};
use crate::series_index::catalog::{
    RangeIndexCatalog, SeriesIndexCatalog, delete_catalogs_with_budget, load_catalog,
    range_catalog_path, range_index_path, series_catalog_path, series_index_path,
    store_catalog_with_budget,
};
use crate::series_index::disk_budget::{SeriesIndexDiskBudget, is_capacity_error};
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
    deferred: Vec<(Timestamp, Vec<String>, u64)>,
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
    enable_range_index: bool,
) -> Result<ReconcileStats> {
    let total_start = Instant::now();
    let budget = purger.budget().cloned();
    let _guard = match &budget {
        Some(budget) => Some(budget.maintenance.lock().await),
        None => None,
    };
    if let Some(budget) = &budget {
        budget.retry_cleanup(&store).await;
    }
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
    )
    .await?;
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["build"])
        .observe(build_start.elapsed().as_secs_f64());
    // Persist changed catalogs before making the new snapshot visible to readers.
    let publish_result: Result<()> = async {
        if let Some(next) = next {
            persist_index_catalogs(&store, region.region_id, &next, &stats, budget.as_ref())
                .await?;
            publish_index_version(&region, Arc::new(next));
            unpublished.disarm();
        } else if let Some(budget) = &budget {
            let current = region.series_index_version();
            let missing = ReconcileStats {
                built_range: usize::from(
                    !current.range_indexes.is_empty()
                        && !store
                            .exists(&range_catalog_path(region.region_id))
                            .await
                            .context(crate::error::OpenDalSnafu)?,
                ),
                built_series: usize::from(
                    !current.series_indexes.is_empty()
                        && !store
                            .exists(&series_catalog_path(region.region_id))
                            .await
                            .context(crate::error::OpenDalSnafu)?,
                ),
                ..Default::default()
            };
            persist_index_catalogs(&store, region.region_id, &current, &missing, Some(budget))
                .await?;
        }
        Ok(())
    }
    .await;
    // Drop may have cleaned up while catalogs were being written, even if a write failed.
    // If dropping starts after this check, the normal drop path cleans up our publication.
    // This assumes the region ID is not reopened or replaced during cleanup.
    if region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping) {
        delete_catalogs_with_budget(&store, region.region_id, budget.as_ref()).await;
        region.series_index_version_control.mark_dropped();
        stats = ReconcileStats::default();
    }
    publish_result?;
    if let Some(budget) = &budget {
        for (end, superseded, required) in &stats.deferred {
            crate::metrics::SERIES_INDEX_CAPACITY_DEFERRED.inc();
            if *required > 0 && *required <= budget.capacity_bytes() {
                evict_older_indexes(&store, budget, &region, *end, superseded, *required).await?;
            }
        }
    }
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
    let mut plan = plan_series_indexes(
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
        && (!enable_range_index || current.range_indexes.len() == visible.len())
    {
        return Ok((None, stats));
    }
    let mut range_indexes = current.range_indexes.clone();
    range_indexes.retain(|file_id| visible.contains(file_id));
    let mut series_indexes = current.series_indexes.clone();
    for id in &plan.expired_index_ids {
        series_indexes.remove(id);
    }
    if purger.budget().is_some() {
        plan.builds
            .sort_by_key(|(_, entry)| std::cmp::Reverse(entry.bucket_end));
    }
    for (bucket, expected) in plan.builds {
        let key = format!(
            "{}/series/{:?}/{}",
            region.region_id, expected.bucket_start, expected.max_file_sequence
        );
        let superseded = current
            .series_indexes
            .iter()
            .filter(|(id, handle)| {
                plan.superseded_index_ids.contains(id)
                    && handle.entry().bucket_start < expected.bucket_end
                    && expected.bucket_start < handle.entry().bucket_end
            })
            .map(|(id, _)| series_index_path(region.region_id, *id))
            .collect::<Vec<_>>();
        if let Some(budget) = purger.budget() {
            let required = budget.required_bytes(&key);
            if required > budget.available_bytes() {
                stats
                    .deferred
                    .push((expected.bucket_end, superseded, required));
                continue;
            }
        }
        // Complete companion indexes independently so a failed series build preserves them.
        for file in &bucket.files {
            if purger.budget().is_none()
                && enable_range_index
                && !range_indexes.contains(&file.file_id().file_id())
                && let Some(file_id) =
                    build_range_index(store, region, version, file.clone()).await?
            {
                stats.built_range += 1;
                range_indexes.insert(file_id);
            }
        }
        let series_handle =
            match build_series_index(store, region, version, &bucket, &expected, purger).await {
                Ok(handle) => handle,
                Err(error) if purger.budget().is_some() && is_capacity_error(&error) => {
                    if let Some(budget) = purger.budget() {
                        let required = budget.defer(key);
                        stats
                            .deferred
                            .push((expected.bucket_end, superseded, required));
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
        if let Some(budget) = purger.budget() {
            budget.built(&key);
        }
        series_indexes
            .retain(|id, _| !superseded.contains(&series_index_path(region.region_id, *id)));
        unpublished.0.push(series_handle.clone());
        stats.built_series += 1;
        series_indexes.insert(expected.index_uuid, series_handle);
    }
    // Cover SSTs outside planned aggregate builds, including skipped buckets.
    if enable_range_index {
        let mut files = files;
        if purger.budget().is_some() {
            files.sort_by_key(|file| std::cmp::Reverse(file.meta_ref().time_range.1));
        }
        for file in files {
            let file_id = file.file_id().file_id();
            if range_indexes.contains(&file_id) {
                continue;
            }
            let end = file.meta_ref().time_range.1;
            let key = range_index_path(region.region_id, file_id);
            if let Some(budget) = purger.budget() {
                let required = budget.required_bytes(&key);
                if required > budget.available_bytes() {
                    stats.deferred.push((end, Vec::new(), required));
                    continue;
                }
            }
            match build_range_index_with_budget(store, region, version, file, purger.budget()).await
            {
                Ok(Some(file_id)) => {
                    if let Some(budget) = purger.budget() {
                        budget.built(&key);
                    }
                    stats.built_range += 1;
                    range_indexes.insert(file_id);
                }
                Ok(None) => {}
                Err(error) if purger.budget().is_some() && is_capacity_error(&error) => {
                    if let Some(budget) = purger.budget() {
                        let required = budget.defer(key);
                        stats.deferred.push((end, Vec::new(), required));
                    }
                }
                Err(error) => return Err(error),
            }
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
    let mut next = SeriesIndexVersion::new(range_indexes, series_indexes)
        .with_disk_pins(region.region_id, purger.budget());
    if purger.budget().is_none() {
        next.index_buckets = plan.index_buckets;
    }
    Ok((Some(next), stats))
}

/// Writes changed catalogs in a stable order; the two writes are not atomic together.
async fn persist_index_catalogs(
    store: &ObjectStore,
    region_id: RegionId,
    next: &SeriesIndexVersion,
    stats: &ReconcileStats,
    budget: Option<&Arc<SeriesIndexDiskBudget>>,
) -> Result<()> {
    if next.range_indexes.is_empty() && budget.is_some() {
        if let Some(budget) = budget {
            budget.delete(store, &range_catalog_path(region_id)).await?;
        }
    } else if stats.built_range + stats.removed_range > 0 {
        let mut range_entries = next.range_indexes.iter().copied().collect::<Vec<_>>();
        range_entries.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        persist_catalog(
            store,
            &range_catalog_path(region_id),
            &RangeIndexCatalog {
                indexes: range_entries,
            },
            budget,
        )
        .await?;
    }
    if next.series_indexes.is_empty() && budget.is_some() {
        if let Some(budget) = budget {
            budget
                .delete(store, &series_catalog_path(region_id))
                .await?;
        }
    } else if stats.built_series + stats.removed_series > 0 {
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
        persist_catalog(
            store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog {
                indexes: series_entries,
            },
            budget,
        )
        .await?;
    }
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

/// A missing catalog durably invalidates its previous coverage and can be rebuilt.
async fn persist_catalog<T: serde::Serialize>(
    store: &ObjectStore,
    path: &str,
    catalog: &T,
    budget: Option<&Arc<SeriesIndexDiskBudget>>,
) -> Result<()> {
    match store_catalog_with_budget(store, path, catalog, budget).await {
        Err(error) if is_capacity_error(&error) => {
            if let Some(budget) = budget {
                if !budget.delete(store, path).await? {
                    return Err(error);
                }
                match store_catalog_with_budget(store, path, catalog, Some(budget)).await {
                    Err(error) if is_capacity_error(&error) => Ok(()),
                    result => result,
                }
            } else {
                Err(error)
            }
        }
        result => result,
    }
}

/// Retires coverage before requesting deletion; pinned snapshots keep their permits.
async fn evict_older_indexes(
    store: &ObjectStore,
    budget: &Arc<SeriesIndexDiskBudget>,
    current_region: &MitoRegionRef,
    end: Timestamp,
    superseded: &[String],
    required: u64,
) -> Result<()> {
    let mut candidates = budget.candidates();
    candidates.retain(|(path, age)| age.is_none_or(|age| age < end) || superseded.contains(path));
    candidates.sort_by_key(|(path, age)| (*age, path.clone()));
    let mut regions = budget.regions();
    if !regions
        .iter()
        .any(|region| region.region_id == current_region.region_id)
    {
        regions.push(current_region.clone());
    }
    for (path, _) in candidates {
        if budget.available_bytes() >= required {
            break;
        }
        let Some(region_id) = path
            .split('/')
            .next()
            .and_then(|id| id.parse::<u64>().ok())
            .map(RegionId::from)
        else {
            continue;
        };
        let is_series = path.contains("/series/");
        let catalog_path = if is_series {
            series_catalog_path(region_id)
        } else {
            range_catalog_path(region_id)
        };
        let closed_series =
            if !regions.iter().any(|region| region.region_id == region_id) && is_series {
                load_catalog::<SeriesIndexCatalog>(store, &catalog_path).await
            } else {
                None
            };
        let closed_range =
            if !regions.iter().any(|region| region.region_id == region_id) && !is_series {
                load_catalog::<RangeIndexCatalog>(store, &catalog_path).await
            } else {
                None
            };
        // Delete instead of allocating a replacement while the budget is full.
        if !budget.delete(store, &catalog_path).await? {
            continue;
        }
        if let Some(region) = regions.iter().find(|region| region.region_id == region_id) {
            let current = region.series_index_version();
            let mut series = current.series_indexes.clone();
            let mut range = current.range_indexes.clone();
            series.retain(|id, _| series_index_path(region_id, *id) != path);
            range.retain(|id| range_index_path(region_id, *id) != path);
            let next = Arc::new(
                SeriesIndexVersion::new(range, series).with_disk_pins(region_id, Some(budget)),
            );
            publish_index_version(region, next.clone());
            drop(current);
            // Recreate both catalogs where space permits, including earlier invalidations.
            persist_index_catalogs(
                store,
                region_id,
                &next,
                &ReconcileStats {
                    removed_range: 1,
                    removed_series: 1,
                    ..Default::default()
                },
                Some(budget),
            )
            .await?;
        }
        budget.delete(store, &path).await?;
        if let Some(mut catalog) = closed_series {
            catalog
                .indexes
                .retain(|entry| series_index_path(region_id, entry.index_uuid) != path);
            persist_catalog(store, &catalog_path, &catalog, Some(budget)).await?;
        }
        if let Some(mut catalog) = closed_range {
            catalog
                .indexes
                .retain(|id| range_index_path(region_id, *id) != path);
            persist_catalog(store, &catalog_path, &catalog, Some(budget)).await?;
        }
        crate::metrics::SERIES_INDEX_EVICTED.inc();
    }
    Ok(())
}
