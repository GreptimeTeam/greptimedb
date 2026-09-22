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

use common_telemetry::{debug, info};
use object_store::ObjectStore;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metrics::{SERIES_INDEX_RECONCILE_ELAPSED, SERIES_INDEX_RECONCILE_TOTAL};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::series_index::bucket::{
    group_files_into_series_buckets, plan_series_indexes, rounded_bucket_width,
};
use crate::series_index::builder::{build_range_index, build_series_index};
use crate::series_index::catalog::{
    RangeIndexCatalog, SeriesIndexCatalog, delete_catalogs, range_catalog_path, range_index_path,
    series_catalog_path, store_catalog,
};
use crate::series_index::purger::IndexFilePurger;
use crate::series_index::version::{
    SeriesIndexFileHandle, SeriesIndexVersion, SeriesIndexVersionControl,
};

/// Failed or cancelled publications retire only their completed output.
struct UnpublishedFile(Option<SeriesIndexFileHandle>);

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
    purger: IndexFilePurger,
    enable_range_index: bool,
) -> Result<ReconcileStats> {
    let start = Instant::now();
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
        delete_catalogs(&store, region.region_id).await;
        region.series_index_version_control.mark_dropped();
    }
    let stats = if dropping {
        result.map(|_| ReconcileStats::default())
    } else {
        result
    }?;
    debug!(
        "Reconciled {} source SSTs in {} buckets, skipped {} buckets",
        stats.source_files, stats.computed_buckets, stats.skipped_buckets
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
    let plan = plan_series_indexes(
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
        .map(|(id, entry)| (*id, *entry))
        .collect();
    stats.removed_range = current.range_indexes.len() - ranges.len();
    if stats.removed_range > 0 {
        publish_catalog(
            store,
            region.region_id,
            &region.series_index_version_control,
            SeriesIndexVersion::new(ranges, current.series_indexes.clone()),
            false,
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
        )
        .await?;
    }
    drop(current);
    for (bucket, expected) in plan.builds {
        if region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping) {
            break;
        }
        let current = region.series_index_version();
        let replaced = current
            .series_indexes
            .iter()
            .filter(|(id, handle)| {
                plan.superseded_index_ids.contains(id)
                    && handle.entry().bucket_start < expected.bucket_end
                    && expected.bucket_start < handle.entry().bucket_end
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        drop(current);
        let entry = build_series_index(store, region, &version, &bucket, &expected).await?;
        let handle = SeriesIndexFileHandle::new(region.region_id, entry, purger.clone());
        let mut unpublished = UnpublishedFile(Some(handle.clone()));
        let current = region.series_index_version();
        let mut series = current.series_indexes.clone();
        series.retain(|id, _| !replaced.contains(id));
        stats.removed_series += current.series_indexes.len() - series.len();
        series.insert(expected.index_uuid, handle);
        publish_catalog(
            store,
            region.region_id,
            &region.series_index_version_control,
            SeriesIndexVersion::new(current.range_indexes.clone(), series),
            true,
        )
        .await?;
        unpublished.0 = None;
        stats.built_series += 1;
    }
    if enable_range_index {
        for file in files {
            if region.state() == RegionRoleState::Leader(RegionLeaderState::Dropping) {
                break;
            }
            let id = file.file_id().file_id();
            if region
                .series_index_version()
                .range_indexes
                .contains_key(&id)
            {
                continue;
            }
            let Some(entry) = build_range_index(store, region, &version, file).await? else {
                continue;
            };
            let current = region.series_index_version();
            let mut ranges = current.range_indexes.clone();
            ranges.insert(id, entry);
            if let Err(error) = publish_catalog(
                store,
                region.region_id,
                &region.series_index_version_control,
                SeriesIndexVersion::new(ranges, current.series_indexes.clone()),
                false,
            )
            .await
            {
                let path = range_index_path(region.region_id, id);
                if let Err(cleanup_error) = store.delete(&path).await {
                    common_telemetry::warn!(cleanup_error; "Failed to remove unpublished range index, path: {path}");
                }
                return Err(error);
            }
            stats.built_range += 1;
        }
    }
    Ok(stats)
}

/// Publishes each catalog before making its coverage visible to readers.
async fn publish_catalog(
    store: &ObjectStore,
    region_id: RegionId,
    control: &SeriesIndexVersionControl,
    next: SeriesIndexVersion,
    series: bool,
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
        store_catalog(
            store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog { indexes },
        )
        .await?;
    } else {
        let mut indexes = next.range_indexes.values().copied().collect::<Vec<_>>();
        indexes
            .sort_unstable_by(|left, right| left.file_id.as_bytes().cmp(right.file_id.as_bytes()));
        store_catalog(
            store,
            &range_catalog_path(region_id),
            &RangeIndexCatalog { indexes },
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
    Ok(())
}
