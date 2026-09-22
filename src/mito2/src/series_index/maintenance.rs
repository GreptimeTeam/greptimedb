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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, info};
use object_store::ObjectStore;
use store_api::storage::RegionId;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::metrics::{
    SERIES_INDEX_CAPACITY_DEFERRED, SERIES_INDEX_DISK_BYTES, SERIES_INDEX_EVICTED,
    SERIES_INDEX_RECONCILE_ELAPSED, SERIES_INDEX_RECONCILE_TOTAL,
};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::series_index::bucket::{
    group_files_into_series_buckets, plan_series_indexes, rounded_bucket_width,
};
use crate::series_index::builder::{build_range_index, build_series_index};
use crate::series_index::catalog::{
    IndexFileMetadata, RangeIndexCatalog, SeriesIndexCatalog, delete_catalogs,
    load_version_control, range_catalog_path, range_index_path, series_catalog_path,
    series_index_path, store_catalog,
};
use crate::series_index::purger::{IndexFilePurger, IndexFileType};
use crate::series_index::version::{
    IndexFileHandle, SeriesIndexFileHandle, SeriesIndexVersion, SeriesIndexVersionControl,
};

/// Shared across workers; all catalog changes and quota decisions hold this lock.
#[derive(Debug)]
pub(crate) struct SeriesIndexMaintenance {
    pub(crate) state: Mutex<MaintenanceState>,
    pub(crate) purger: IndexFilePurger,
}

#[derive(Debug)]
pub(crate) struct MaintenanceState {
    pub(crate) capacity: u64,
    pub(crate) used: u64,
    pub(crate) versions: HashMap<RegionId, Arc<SeriesIndexVersionControl>>,
}

impl SeriesIndexMaintenance {
    pub(crate) fn new(limit: ReadableSize, purger: IndexFilePurger) -> Self {
        Self {
            state: Mutex::new(MaintenanceState {
                capacity: limit.as_bytes(),
                used: 0,
                versions: HashMap::new(),
            }),
            purger,
        }
    }

    pub(crate) async fn open(store: &ObjectStore, limit: ReadableSize) -> Result<Arc<Self>> {
        let maintenance = Arc::new(Self::new(limit, IndexFilePurger::start(store.clone())));
        crate::series_index::recovery::recover(store, &maintenance).await?;
        Ok(maintenance)
    }

    pub(crate) async fn open_region(
        &self,
        store: &ObjectStore,
        region: RegionId,
    ) -> Arc<SeriesIndexVersionControl> {
        let mut state = self.state.lock().await;
        if let Some(control) = state.versions.get(&region) {
            return control.clone();
        }
        let control = load_version_control(store, region, &self.purger).await;
        state.register(region, control.clone());
        control
    }

    pub(crate) async fn drop_region(&self, store: &ObjectStore, region: RegionId) {
        self.state.lock().await.drop_region(store, region).await;
    }
}

impl MaintenanceState {
    pub(crate) fn register(&mut self, region: RegionId, control: Arc<SeriesIndexVersionControl>) {
        if self.versions.contains_key(&region) {
            return;
        }
        let bytes = control.current().disk_usage();
        self.used += bytes;
        SERIES_INDEX_DISK_BYTES.add(bytes as i64);
        self.versions.insert(region, control);
    }

    async fn drop_region(&mut self, store: &ObjectStore, region: RegionId) {
        delete_catalogs(store, region).await;
        if let Some(control) = self.versions.remove(&region) {
            let bytes = control.current().disk_usage();
            control.mark_dropped();
            self.used -= bytes;
            SERIES_INDEX_DISK_BYTES.sub(bytes as i64);
        }
    }

    pub(crate) fn candidates(&self) -> Vec<(String, IndexFileMetadata)> {
        self.versions
            .iter()
            .flat_map(|(region, control)| {
                let snapshot = control.current();
                snapshot
                    .range_indexes
                    .iter()
                    .map(|(id, handle)| (range_index_path(*region, *id), handle.metadata()))
                    .chain(
                        snapshot.series_indexes.iter().map(|(id, handle)| {
                            (series_index_path(*region, *id), handle.metadata())
                        }),
                    )
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn average_size(&self, series: bool) -> Option<u64> {
        let (bytes, count) = self
            .candidates()
            .iter()
            .filter(|(path, _)| path.contains("/series/") == series)
            .fold((0u64, 0u64), |(bytes, count), (_, meta)| {
                (bytes + meta.file_size, count + 1)
            });
        (count > 0).then(|| bytes.div_ceil(count))
    }

    fn admission(
        &self,
        path: &str,
        metadata: IndexFileMetadata,
        replaced: &[String],
    ) -> Option<Vec<String>> {
        admission(self.capacity, self.candidates(), path, metadata, replaced)
    }
}

impl Drop for MaintenanceState {
    fn drop(&mut self) {
        SERIES_INDEX_DISK_BYTES.sub(self.used as i64);
    }
}

/// Stable ordering also decides whether an incoming file wins an equal-timestamp tie.
pub(crate) fn eviction_key(
    path: &str,
    metadata: IndexFileMetadata,
) -> (common_time::Timestamp, u64, bool, &str) {
    let region = path
        .split('/')
        .next()
        .and_then(|id| id.parse().ok())
        .unwrap_or(0);
    (
        metadata.min_timestamp,
        region,
        path.contains("/series/"),
        path,
    )
}

fn admission(
    capacity: u64,
    mut candidates: Vec<(String, IndexFileMetadata)>,
    path: &str,
    metadata: IndexFileMetadata,
    replaced: &[String],
) -> Option<Vec<String>> {
    if metadata.file_size > capacity {
        return None;
    }
    candidates.retain(|(candidate, _)| candidate != path && !replaced.contains(candidate));
    let mut used = candidates
        .iter()
        .map(|(_, meta)| meta.file_size as u128)
        .sum::<u128>()
        + metadata.file_size as u128;
    candidates
        .sort_by(|(left, lm), (right, rm)| eviction_key(left, *lm).cmp(&eviction_key(right, *rm)));
    let incoming = eviction_key(path, metadata);
    let mut evicted = Vec::new();
    for (candidate, meta) in candidates {
        if used <= capacity as u128 {
            break;
        }
        if eviction_key(&candidate, meta) >= incoming {
            return None;
        }
        used -= meta.file_size as u128;
        evicted.push(candidate);
    }
    (used <= capacity as u128).then_some(evicted)
}

/// Failed or cancelled publications retire only their completed output.
struct UnpublishedFile(Option<IndexFileHandle>);

impl Drop for UnpublishedFile {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
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

/// Reconciles one captured SST snapshot. Each catalog is published independently.
pub(crate) async fn reconcile_series_indexes(
    worker_id: u32,
    store: ObjectStore,
    region: MitoRegionRef,
    requested_bucket_width: Duration,
    now_ms: i64,
    maintenance: Arc<SeriesIndexMaintenance>,
    enable_range_index: bool,
) -> Result<ReconcileStats> {
    let start = Instant::now();
    let mut state = maintenance.state.lock().await;
    state.register(
        region.region_id,
        region.series_index_version_control.clone(),
    );
    let result = reconcile(
        &store,
        &region,
        requested_bucket_width,
        now_ms,
        &maintenance.purger,
        &mut state,
        enable_range_index,
    )
    .await;
    // Drop may begin while a catalog write is in flight.
    let dropping = region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping);
    if dropping {
        state.drop_region(&store, region.region_id).await;
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
    state: &mut MaintenanceState,
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
    let ranges: HashMap<_, _> = current
        .range_indexes
        .iter()
        .filter(|(id, _)| visible.contains(id))
        .map(|(id, handle)| (*id, handle.clone()))
        .collect();
    stats.removed_range = current.range_indexes.len() - ranges.len();
    if stats.removed_range > 0 {
        publish_catalog(
            store,
            region.region_id,
            &region.series_index_version_control,
            SeriesIndexVersion::new(ranges, current.series_indexes.clone()),
            false,
            state,
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
            SeriesIndexVersion::new(current.range_indexes.clone(), series),
            true,
            state,
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
        {
            let series_size = state.average_size(true).or(series_sample);
            let range_size = state.average_size(false).or(range_sample).unwrap_or(0);
            let missing_ranges = if enable_range_index {
                bucket
                    .files
                    .iter()
                    .filter(|file| {
                        !current
                            .range_indexes
                            .contains_key(&file.file_id().file_id())
                    })
                    .count() as u64
            } else {
                0
            };
            if let Some(series_size) = series_size
                && state
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
        let metadata = build_series_index(store, region, &version, &bucket, &expected).await?;
        let handle = SeriesIndexFileHandle::with_metadata(
            region.region_id,
            expected.clone(),
            metadata,
            purger.clone(),
        );
        let mut unpublished = UnpublishedFile(Some(handle.file_handle().clone()));
        series_sample = Some(handle.metadata().file_size);
        if !make_room(store, state, &path, handle.metadata(), &replaced).await? {
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
            SeriesIndexVersion::new(current.range_indexes.clone(), series),
            true,
            state,
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
                || region
                    .series_index_version()
                    .range_indexes
                    .contains_key(&id)
            {
                continue;
            }
            let path = range_index_path(region.region_id, id);
            if purger.is_retired(&path) {
                continue;
            }
            if let Some(size) = state.average_size(false).or(range_sample)
                && state
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
            let Some((id, metadata)) = build_range_index(store, region, &version, file).await?
            else {
                continue;
            };
            range_sample = Some(metadata.file_size);
            let handle = IndexFileHandle::new(
                region.region_id,
                id,
                IndexFileType::Range,
                metadata,
                purger.clone(),
            );
            let mut unpublished = UnpublishedFile(Some(handle.clone()));
            if !make_room(store, state, &path, metadata, &[]).await? {
                continue;
            }
            let current = region.series_index_version();
            let mut ranges = current.range_indexes.clone();
            ranges.insert(id, handle);
            publish_catalog(
                store,
                region.region_id,
                &region.series_index_version_control,
                SeriesIndexVersion::new(ranges, current.series_indexes.clone()),
                false,
                state,
            )
            .await?;
            unpublished.0 = None;
            stats.built_range += 1;
        }
    }
    Ok(stats)
}

/// Evicts only after an exact admission decision; an oversized/older candidate changes nothing.
async fn make_room(
    store: &ObjectStore,
    state: &mut MaintenanceState,
    path: &str,
    metadata: IndexFileMetadata,
    replaced: &[String],
) -> Result<bool> {
    let Some(evicted) = state.admission(path, metadata, replaced) else {
        SERIES_INDEX_CAPACITY_DEFERRED.inc();
        return Ok(false);
    };
    for path in evicted {
        evict_index(store, state, &path).await?;
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
    state: &mut MaintenanceState,
) -> Result<()> {
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
        let mut indexes = next.range_indexes.keys().copied().collect::<Vec<_>>();
        indexes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        store_catalog(
            store,
            &range_catalog_path(region_id),
            &RangeIndexCatalog {
                indexes,
                file_metadata: next
                    .range_indexes
                    .iter()
                    .map(|(id, h)| (*id, h.metadata()))
                    .collect(),
            },
        )
        .await?;
    }
    let next = Arc::new(next);
    let previous = control.publish(next.clone());
    for (id, handle) in &previous.series_indexes {
        if !next.series_indexes.contains_key(id) {
            handle.mark_deleted();
        }
    }
    for (id, handle) in &previous.range_indexes {
        if !next.range_indexes.contains_key(id) {
            handle.mark_deleted();
        }
    }
    let old_bytes = previous.disk_usage();
    let new_bytes = next.disk_usage();
    state.used = state.used - old_bytes + new_bytes;
    SERIES_INDEX_DISK_BYTES.sub(old_bytes as i64);
    SERIES_INDEX_DISK_BYTES.add(new_bytes as i64);
    Ok(())
}

/// Removes catalog coverage before scheduling deletion, including for closed regions.
pub(crate) async fn evict_index(
    store: &ObjectStore,
    state: &mut MaintenanceState,
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
    if let Some(control) = state.versions.get(&region_id).cloned() {
        let current = control.current();
        let mut ranges = current.range_indexes.clone();
        let mut indexes = current.series_indexes.clone();
        ranges.retain(|id, _| range_index_path(region_id, *id) != path);
        indexes.retain(|id, _| series_index_path(region_id, *id) != path);
        publish_catalog(
            store,
            region_id,
            &control,
            SeriesIndexVersion::new(ranges, indexes),
            series,
            state,
        )
        .await?;
    }
    SERIES_INDEX_EVICTED.inc();
    Ok(())
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;

    use super::*;

    #[test]
    fn test_admission_boundaries() {
        let metadata = |file_size, start| IndexFileMetadata {
            file_size,
            min_timestamp: Timestamp::new_second(start),
        };
        let candidates = vec![
            ("1/series/old.parquet".to_string(), metadata(1025, 1)),
            ("2/range/new.parquet".to_string(), metadata(2047, 9)),
        ];
        for (size, start, replaced, expected) in [
            (1024, 0, vec![], Some(vec![])),
            (4097, 20, vec![], None),
            (1025, 0, vec![], None),
            (
                2049,
                5,
                vec![],
                Some(vec!["1/series/old.parquet".to_string()]),
            ),
            (2050, 5, vec![], None),
            (
                2049,
                1,
                vec!["1/series/old.parquet".to_string()],
                Some(vec![]),
            ),
            (
                1025,
                1,
                vec![],
                Some(vec!["1/series/old.parquet".to_string()]),
            ),
        ] {
            assert_eq!(
                expected,
                admission(
                    4096,
                    candidates.clone(),
                    "3/series/incoming.parquet",
                    metadata(size, start),
                    &replaced
                )
            );
        }
    }
}
