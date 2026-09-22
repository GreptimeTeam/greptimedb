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
use crate::metrics::{
    SERIES_INDEX_CAPACITY_DEFERRED, SERIES_INDEX_EVICTED, SERIES_INDEX_RECONCILE_ELAPSED,
    SERIES_INDEX_RECONCILE_TOTAL,
};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::series_index::bucket::{
    group_files_into_series_buckets, plan_series_indexes, rounded_bucket_width,
};
use crate::series_index::builder::{build_range_index, build_series_index};
use crate::series_index::catalog::{
    IndexFileMetadata, RangeIndexCatalog, SeriesIndexCatalog, delete_catalogs_with_budget,
    load_catalog, range_catalog_path, range_index_path, series_catalog_path, series_index_path,
    store_catalog,
};
use crate::series_index::disk_budget::SeriesIndexDiskBudget;
use crate::series_index::purger::IndexFilePurger;
use crate::series_index::version::{
    SeriesIndexFileHandle, SeriesIndexVersion, SeriesIndexVersionControl,
};

/// A failed publication must retire its completed series output, not its predecessor.
struct UnpublishedSeriesFile(Option<SeriesIndexFileHandle>);
impl Drop for UnpublishedSeriesFile {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.mark_deleted();
        }
    }
}

/// Completed range outputs are retired if admission/publication is cancelled or fails.
struct UnpublishedRangeFile {
    budget: Option<Arc<SeriesIndexDiskBudget>>,
    path: String,
    installed: bool,
}
impl Drop for UnpublishedRangeFile {
    fn drop(&mut self) {
        if !self.installed
            && let Some(budget) = &self.budget
        {
            budget.retire(&self.path);
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

/// Reconciles one captured SST snapshot. Each catalog is published independently.
pub(crate) async fn reconcile_series_indexes(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: IndexFilePurger,
    enable_range_index: bool,
) -> Result<ReconcileStats> {
    let start = Instant::now();
    let budget = purger.budget().cloned();
    let _guard = match &budget {
        Some(budget) => Some(budget.maintenance.lock().await),
        None => None,
    };
    if let Some(budget) = &budget {
        budget.register_version(region.region_id, &region.series_index_version_control);
        budget.retry_cleanup(&store).await;
    }
    let result = reconcile(
        &store,
        &region,
        requested_bucket_width,
        now_ms,
        &purger,
        enable_range_index,
    )
    .await;
    // Drop may begin while a catalog write is in flight.
    let dropping = region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping);
    if dropping {
        delete_catalogs_with_budget(&store, region.region_id, budget.as_ref()).await;
        region.series_index_version_control.mark_dropped();
    }
    // Failed and cancelled publications retire their completed outputs independently of usage.
    if let Some(budget) = &budget {
        budget.retry_cleanup(&store).await;
    }
    let stats = if dropping {
        result.map(|_| ReconcileStats::default())
    } else {
        result
    }?;
    debug!(
        "Reconciled {} source SSTs in {} buckets",
        stats.source_files, stats.computed_buckets
    );
    SERIES_INDEX_RECONCILE_ELAPSED
        .with_label_values(&["total"])
        .observe(start.elapsed().as_secs_f64());
    SERIES_INDEX_RECONCILE_TOTAL
        .with_label_values(&[if stats.changed() { "changed" } else { "noop" }])
        .inc();
    if stats.changed() {
        info!(
            "Reconciled series indexes, worker: {worker_id}, region: {}, elapsed: {:?}, stats: {:?}",
            region.region_id,
            start.elapsed(),
            stats
        );
    }
    Ok(stats)
}

async fn reconcile(
    store: &ObjectStore,
    region: &MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    purger: &IndexFilePurger,
    enable_range_index: bool,
) -> Result<ReconcileStats> {
    let version = region.version_control.current().version;
    if !is_sparse_metric_metadata(&version.metadata) {
        return Ok(ReconcileStats::default());
    }
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .cloned()
        .collect::<Vec<_>>();
    let visible = files
        .iter()
        .map(|file| file.file_id().file_id())
        .collect::<HashSet<_>>();
    let current = region.series_index_version();
    let buckets = version
        .compaction_time_window
        .and_then(|window| {
            rounded_bucket_width(requested_bucket_width, window).map(|width| {
                group_files_into_series_buckets(&files, width, (window.as_secs() as i64).max(1))
            })
        })
        .unwrap_or_default();
    let mut plan = plan_series_indexes(
        buckets,
        current.index_buckets.clone(),
        version.options.ttl,
        now_ms,
    );
    let mut stats = ReconcileStats {
        source_files: files.len(),
        computed_buckets: plan.computed_buckets,
        skipped_buckets: plan.skipped_buckets,
        ..Default::default()
    };
    let budget = purger.budget();
    let ranges = current
        .range_indexes
        .intersection(&visible)
        .copied()
        .collect::<HashSet<_>>();
    stats.removed_range = current.range_indexes.len() - ranges.len();
    if stats.removed_range > 0 {
        publish_catalog(
            store,
            region.region_id,
            &region.series_index_version_control,
            SeriesIndexVersion::new(ranges, current.series_indexes.clone())
                .with_range_metadata(current.range_metadata.clone()),
            false,
            budget,
        )
        .await?;
    }
    if !plan.expired_index_ids.is_empty() {
        let current = region.series_index_version();
        let mut series = current.series_indexes.clone();
        series.retain(|id, _| !plan.expired_index_ids.contains(id));
        stats.removed_series += current.series_indexes.len() - series.len();
        publish_catalog(
            store,
            region.region_id,
            &region.series_index_version_control,
            SeriesIndexVersion::new(current.range_indexes.clone(), series)
                .with_range_metadata(current.range_metadata.clone()),
            true,
            budget,
        )
        .await?;
    }
    drop(current);
    plan.builds.sort_by_key(|(bucket, _)| {
        std::cmp::Reverse(
            bucket
                .files
                .iter()
                .map(|file| file.time_range().0)
                .min()
                .unwrap_or(bucket.start),
        )
    });
    let mut skipped_ranges = HashSet::new();
    // Samples from this pass also seed estimates when the first completed file is too large.
    let mut series_sample = None;
    let mut range_sample = None;
    for (bucket, expected) in plan.builds {
        if region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping) {
            break;
        }
        let min_timestamp = bucket
            .files
            .iter()
            .map(|file| file.time_range().0)
            .min()
            .unwrap_or(bucket.start);
        let path = series_index_path(region.region_id, expected.index_uuid);
        let current = region.series_index_version();
        let replaced = current
            .series_indexes
            .iter()
            .filter(|(id, handle)| {
                plan.superseded_index_ids.contains(id)
                    && handle.entry().bucket_start < expected.bucket_end
                    && expected.bucket_start < handle.entry().bucket_end
            })
            .map(|(id, _)| series_index_path(region.region_id, *id))
            .collect::<Vec<_>>();
        if let Some(budget) = budget {
            let series_size = budget.average_size(true).or(series_sample);
            let range_size = budget.average_size(false).or(range_sample).unwrap_or(0);
            let missing_ranges = if enable_range_index {
                bucket
                    .files
                    .iter()
                    .filter(|file| !current.range_indexes.contains(&file.file_id().file_id()))
                    .count() as u64
            } else {
                0
            };
            if let Some(series_size) = series_size
                && budget
                    .admission(
                        &path,
                        IndexFileMetadata {
                            file_size: series_size
                                .saturating_add(range_size.saturating_mul(missing_ranges)),
                            min_timestamp,
                        },
                        &replaced,
                    )
                    .is_none()
            {
                skipped_ranges.extend(bucket.files.iter().map(|file| file.file_id().file_id()));
                stats.skipped_buckets += 1;
                SERIES_INDEX_CAPACITY_DEFERRED.inc();
                continue;
            }
        }
        drop(current);
        let handle =
            build_series_index(store, region, &version, &bucket, &expected, purger).await?;
        let mut unpublished = UnpublishedSeriesFile(Some(handle.clone()));
        series_sample = Some(handle.metadata().file_size);
        if !make_room(store, budget, &path, handle.metadata(), &replaced).await? {
            if let Some(budget) = budget {
                budget.retire(&path);
            }
            skipped_ranges.extend(bucket.files.iter().map(|file| file.file_id().file_id()));
            continue;
        }
        let current = region.series_index_version();
        let mut series = current.series_indexes.clone();
        series.retain(|id, _| !replaced.contains(&series_index_path(region.region_id, *id)));
        stats.removed_series += current.series_indexes.len() - series.len();
        series.insert(expected.index_uuid, handle);
        publish_catalog(
            store,
            region.region_id,
            &region.series_index_version_control,
            SeriesIndexVersion::new(current.range_indexes.clone(), series)
                .with_range_metadata(current.range_metadata.clone()),
            true,
            budget,
        )
        .await?;
        unpublished.0 = None;
        stats.built_series += 1;
    }
    if enable_range_index {
        let mut files = files;
        files.sort_by_key(|file| std::cmp::Reverse(file.time_range().0));
        for file in files {
            if region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping) {
                break;
            }
            let id = file.file_id().file_id();
            if skipped_ranges.contains(&id)
                || region.series_index_version().range_indexes.contains(&id)
            {
                continue;
            }
            let path = range_index_path(region.region_id, id);
            if let Some(budget) = budget {
                if budget.is_retired(&path) {
                    continue;
                }
                if let Some(size) = budget.average_size(false).or(range_sample)
                    && budget
                        .admission(
                            &path,
                            IndexFileMetadata {
                                file_size: size,
                                min_timestamp: file.time_range().0,
                            },
                            &[],
                        )
                        .is_none()
                {
                    SERIES_INDEX_CAPACITY_DEFERRED.inc();
                    continue;
                }
            }
            let Some((id, metadata)) = build_range_index(store, region, &version, file).await?
            else {
                continue;
            };
            range_sample = Some(metadata.file_size);
            if let Some(budget) = budget {
                budget.track(path.clone(), metadata);
            }
            let mut unpublished = UnpublishedRangeFile {
                budget: budget.cloned(),
                path: path.clone(),
                installed: false,
            };
            let result = async {
                if !make_room(store, budget, &path, metadata, &[]).await? {
                    return Ok(false);
                }
                let current = region.series_index_version();
                let mut ranges = current.range_indexes.clone();
                ranges.insert(id);
                let mut range_metadata = current.range_metadata.clone();
                range_metadata.insert(id, metadata);
                publish_catalog(
                    store,
                    region.region_id,
                    &region.series_index_version_control,
                    SeriesIndexVersion::new(ranges, current.series_indexes.clone())
                        .with_range_metadata(range_metadata),
                    false,
                    budget,
                )
                .await?;
                Ok(true)
            }
            .await;
            match result {
                Ok(true) => {
                    unpublished.installed = true;
                    stats.built_range += 1;
                }
                result => {
                    if let Some(budget) = budget {
                        budget.retire(&path);
                    } else if let Err(error) = store.delete(&path).await {
                        common_telemetry::warn!(error; "Failed to remove unpublished range index, path: {path}");
                    }
                    result?;
                }
            }
        }
    }
    Ok(stats)
}

/// Evicts only after an exact admission decision; an oversized/older candidate changes nothing.
async fn make_room(
    store: &ObjectStore,
    budget: Option<&Arc<SeriesIndexDiskBudget>>,
    path: &str,
    metadata: IndexFileMetadata,
    replaced: &[String],
) -> Result<bool> {
    let Some(budget) = budget else {
        return Ok(true);
    };
    let Some(evicted) = budget.admission(path, metadata, replaced) else {
        SERIES_INDEX_CAPACITY_DEFERRED.inc();
        return Ok(false);
    };
    for path in evicted {
        evict_index(store, budget, &path).await?;
    }
    Ok(true)
}

/// Commits one index type at a time so a later catalog failure cannot undo its accounting.
async fn publish_catalog(
    store: &ObjectStore,
    region_id: RegionId,
    control: &SeriesIndexVersionControl,
    next: SeriesIndexVersion,
    series: bool,
    budget: Option<&Arc<SeriesIndexDiskBudget>>,
) -> Result<()> {
    let next = next.with_disk_pins(region_id, budget);
    if series {
        let mut indexes = next
            .series_indexes
            .values()
            .map(|handle| handle.entry().clone())
            .collect::<Vec<_>>();
        indexes.sort_by(|left, right| {
            (left.bucket_start, left.index_uuid.as_bytes())
                .cmp(&(right.bucket_start, right.index_uuid.as_bytes()))
        });
        let file_metadata = next
            .series_indexes
            .iter()
            .map(|(id, handle)| (*id, handle.metadata()))
            .collect();
        store_catalog(
            store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog {
                indexes,
                file_metadata,
            },
        )
        .await?;
    } else {
        let mut indexes = next.range_indexes.iter().copied().collect::<Vec<_>>();
        indexes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        store_catalog(
            store,
            &range_catalog_path(region_id),
            &RangeIndexCatalog {
                indexes,
                file_metadata: next.range_metadata.clone(),
            },
        )
        .await?;
    }
    let next = Arc::new(next);
    let previous = control.publish(next.clone());
    for (id, handle) in &previous.series_indexes {
        if !next.series_indexes.contains_key(id) {
            handle.mark_deleted();
            if let Some(budget) = budget {
                budget.retire(&series_index_path(region_id, *id));
            }
        }
    }
    if let Some(budget) = budget {
        for id in previous.range_indexes.difference(&next.range_indexes) {
            budget.retire(&range_index_path(region_id, *id));
        }
        for id in &next.range_indexes {
            budget.install(&range_index_path(region_id, *id));
        }
        for id in next.series_indexes.keys() {
            budget.install(&series_index_path(region_id, *id));
        }
        debug!(
            "Published index usage, region: {}, bytes: {}",
            region_id,
            next.disk_usage()
        );
    }
    Ok(())
}

/// Removes catalog coverage before scheduling deletion, including for closed regions.
pub(crate) async fn evict_index(
    store: &ObjectStore,
    budget: &Arc<SeriesIndexDiskBudget>,
    path: &str,
) -> Result<()> {
    let Some(region_id) = path
        .split('/')
        .next()
        .and_then(|id| id.parse::<u64>().ok())
        .map(RegionId::from)
    else {
        return Ok(());
    };
    let series = path.contains("/series/");
    if let Some(control) = budget.version(region_id) {
        let current = control.current();
        let mut ranges = current.range_indexes.clone();
        let mut indexes = current.series_indexes.clone();
        ranges.retain(|id| range_index_path(region_id, *id) != path);
        indexes.retain(|id, _| series_index_path(region_id, *id) != path);
        publish_catalog(
            store,
            region_id,
            &control,
            SeriesIndexVersion::new(ranges, indexes)
                .with_range_metadata(current.range_metadata.clone()),
            series,
            Some(budget),
        )
        .await?;
    } else if series {
        let catalog_path = series_catalog_path(region_id);
        let mut catalog = load_catalog::<SeriesIndexCatalog>(store, &catalog_path)
            .await
            .unwrap_or_default();
        catalog
            .indexes
            .retain(|entry| series_index_path(region_id, entry.index_uuid) != path);
        let retained = catalog
            .indexes
            .iter()
            .map(|entry| entry.index_uuid)
            .collect::<HashSet<_>>();
        catalog.file_metadata.retain(|id, _| retained.contains(id));
        store_catalog(store, &catalog_path, &catalog).await?;
    } else {
        let catalog_path = range_catalog_path(region_id);
        let mut catalog = load_catalog::<RangeIndexCatalog>(store, &catalog_path)
            .await
            .unwrap_or_default();
        catalog
            .indexes
            .retain(|id| range_index_path(region_id, *id) != path);
        catalog
            .file_metadata
            .retain(|id, _| catalog.indexes.contains(id));
        store_catalog(store, &catalog_path, &catalog).await?;
    }
    budget.retire(path);
    SERIES_INDEX_EVICTED.inc();
    Ok(())
}
