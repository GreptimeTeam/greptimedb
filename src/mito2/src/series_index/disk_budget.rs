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

//! Installed index accounting and deferred physical deletion, shared by all workers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use common_base::readable_size::ReadableSize;
use common_telemetry::warn;
use object_store::ObjectStore;
use snafu::{ResultExt, ensure};
use store_api::storage::RegionId;
use tokio::sync::Mutex as AsyncMutex;

use crate::error::{InvalidConfigSnafu, OpenDalSnafu, Result};
use crate::metrics::SERIES_INDEX_DISK_BYTES;
use crate::series_index::catalog::IndexFileMetadata;
use crate::series_index::version::SeriesIndexVersionControl;

pub(crate) fn validate_limit(limit: ReadableSize) -> Result<()> {
    ensure!(
        limit.as_bytes() >= 1024,
        InvalidConfigSnafu {
            reason: "experimental_series_index_max_size must be at least 1KiB"
        }
    );
    Ok(())
}

#[derive(Debug)]
struct DiskFile {
    metadata: IndexFileMetadata,
    installed: bool,
    pin: Weak<()>,
    retired: bool,
}

/// One quota per local index directory. Only published files consume capacity.
/// Maintenance serializes catalog changes; the file mutex also protects reader pins.
#[derive(Debug)]
pub(crate) struct SeriesIndexDiskBudget {
    capacity: u64,
    files: Mutex<HashMap<String, DiskFile>>,
    versions: Mutex<HashMap<RegionId, Weak<SeriesIndexVersionControl>>>,
    pub(crate) maintenance: AsyncMutex<()>,
    deletion: AsyncMutex<()>,
}

impl SeriesIndexDiskBudget {
    pub(crate) async fn open(store: &ObjectStore, limit: ReadableSize) -> Result<Arc<Self>> {
        validate_limit(limit)?;
        let budget = Arc::new(Self {
            capacity: limit.as_bytes(),
            files: Mutex::default(),
            versions: Mutex::default(),
            maintenance: AsyncMutex::new(()),
            deletion: AsyncMutex::new(()),
        });
        crate::series_index::recovery::recover(store, &budget).await?;
        Ok(budget)
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|file| file.installed)
            .map(|file| file.metadata.file_size)
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn available_bytes(&self) -> u64 {
        self.capacity.saturating_sub(self.used_bytes())
    }

    /// Tracks a completed file without charging it before publication.
    pub(crate) fn track(&self, path: String, metadata: IndexFileMetadata) {
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(path)
            .or_insert(DiskFile {
                metadata,
                installed: false,
                pin: Weak::new(),
                retired: false,
            });
    }

    /// Called only after the corresponding catalog and snapshot are published.
    pub(crate) fn install(&self, path: &str) {
        if let Some(file) = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(path)
            && !file.installed
            && !file.retired
        {
            file.installed = true;
            SERIES_INDEX_DISK_BYTES.add(file.metadata.file_size as i64);
        }
    }

    /// Releases logical usage immediately; pins independently delay physical deletion.
    pub(crate) fn retire(&self, path: &str) {
        if let Some(file) = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(path)
        {
            if file.installed {
                file.installed = false;
                SERIES_INDEX_DISK_BYTES.sub(file.metadata.file_size as i64);
            }
            file.retired = true;
        }
    }

    pub(crate) fn retire_region(&self, region_id: RegionId) {
        let prefix = format!("{}/", region_id.as_u64());
        let paths = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .filter(|path| path.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        for path in paths {
            self.retire(&path);
        }
    }

    /// Registers restored snapshots before their region enters the worker map.
    pub(crate) fn register_version(
        &self,
        region_id: RegionId,
        control: &Arc<SeriesIndexVersionControl>,
    ) {
        let mut versions = self
            .versions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        versions.retain(|_, version| version.strong_count() > 0);
        versions.insert(region_id, Arc::downgrade(control));
    }

    pub(crate) fn version(&self, region_id: RegionId) -> Option<Arc<SeriesIndexVersionControl>> {
        self.versions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&region_id)
            .and_then(Weak::upgrade)
    }

    pub(crate) fn candidates(&self) -> Vec<(String, IndexFileMetadata)> {
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(_, file)| file.installed)
            .map(|(path, file)| (path.clone(), file.metadata))
            .collect()
    }

    /// Separate averages avoid estimating small range files as large aggregate files.
    pub(crate) fn average_size(&self, series: bool) -> Option<u64> {
        let files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (bytes, count) = files
            .iter()
            .filter(|(path, file)| file.installed && path.contains("/series/") == series)
            .fold((0u64, 0u64), |(bytes, count), (_, file)| {
                (bytes + file.metadata.file_size, count + 1)
            });
        (count > 0).then(|| bytes.div_ceil(count))
    }

    /// Plans eviction without changing state. Rejected candidates evict nothing.
    pub(crate) fn admission(
        &self,
        path: &str,
        metadata: IndexFileMetadata,
        replaced: &[String],
    ) -> Option<Vec<String>> {
        if metadata.file_size > self.capacity {
            return None;
        }
        let mut candidates = self.candidates();
        candidates.retain(|(candidate, _)| candidate != path && !replaced.contains(candidate));
        let mut used = candidates
            .iter()
            .map(|(_, meta)| meta.file_size as u128)
            .sum::<u128>()
            + metadata.file_size as u128;
        candidates.sort_by(|(left, lmeta), (right, rmeta)| {
            eviction_key(left, *lmeta).cmp(&eviction_key(right, *rmeta))
        });
        let incoming = eviction_key(path, metadata);
        let mut evicted = Vec::new();
        for (candidate, meta) in candidates {
            if used <= self.capacity as u128 {
                break;
            }
            if eviction_key(&candidate, meta) >= incoming {
                return None;
            }
            used -= meta.file_size as u128;
            evicted.push(candidate);
        }
        (used <= self.capacity as u128).then_some(evicted)
    }

    pub(crate) fn is_retired(&self, path: &str) -> bool {
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(path)
            .is_some_and(|file| file.retired)
    }

    pub(crate) fn pin(&self, path: &str) -> Option<Arc<()>> {
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let file = files.get_mut(path)?;
        if file.retired && !file.installed {
            return None;
        }
        let pin = file.pin.upgrade().unwrap_or_else(|| Arc::new(()));
        file.pin = Arc::downgrade(&pin);
        Some(pin)
    }

    /// SST garbage collection may request deletion before reconciliation prunes metadata.
    /// In that case keep charging the installed entry until its catalog is updated.
    pub(crate) async fn delete(&self, store: &ObjectStore, path: &str) -> Result<bool> {
        let _guard = self.deletion.lock().await;
        {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(file) = files.get_mut(path) {
                file.retired = true;
                if file.installed || file.pin.strong_count() > 0 {
                    return Ok(false);
                }
            }
        }
        store.delete(path).await.context(OpenDalSnafu)?;
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(path);
        Ok(true)
    }

    pub(crate) async fn retry_cleanup(&self, store: &ObjectStore) {
        let paths = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(_, file)| file.retired && !file.installed)
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        for path in paths {
            if let Err(error) = self.delete(store, &path).await {
                warn!(error; "Failed to retry index deletion, path: {path}");
            }
        }
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

impl Drop for SeriesIndexDiskBudget {
    fn drop(&mut self) {
        SERIES_INDEX_DISK_BYTES.sub(self.used_bytes() as i64);
    }
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;
    use object_store::services::Memory;

    use super::*;

    fn metadata(bytes: u64, start: i64) -> IndexFileMetadata {
        IndexFileMetadata {
            file_size: bytes,
            min_timestamp: Timestamp::new_second(start),
        }
    }

    #[tokio::test]
    async fn test_admission_uses_actual_bytes_and_minimum_timestamp() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let budget = SeriesIndexDiskBudget::open(&store, ReadableSize(4096))
            .await
            .unwrap();
        for (path, meta) in [
            ("1/series/old.parquet", metadata(1025, 1)),
            ("2/range/new.parquet", metadata(2047, 9)),
        ] {
            budget.track(path.to_string(), meta);
            budget.install(path);
            budget.install(path);
        }
        assert_eq!(3072, budget.used_bytes());
        assert_eq!(
            Some(vec![]),
            budget.admission("3/series/exact.parquet", metadata(1024, 0), &[])
        );
        assert_eq!(
            None,
            budget.admission("3/series/oversized.parquet", metadata(4097, 20), &[])
        );
        assert_eq!(
            None,
            budget.admission("3/series/older.parquet", metadata(1025, 0), &[])
        );
        assert_eq!(
            Some(vec!["1/series/old.parquet".to_string()]),
            budget.admission("3/series/middle.parquet", metadata(2049, 5), &[])
        );
        assert_eq!(
            None,
            budget.admission("3/series/middle.parquet", metadata(2050, 5), &[])
        );
        assert_eq!(
            3072,
            budget.used_bytes(),
            "planning must not change accounting"
        );
        assert_eq!(
            Some(vec![]),
            budget.admission(
                "1/series/replacement.parquet",
                metadata(2049, 1),
                &["1/series/old.parquet".to_string()]
            )
        );
    }

    #[tokio::test]
    async fn test_average_size_and_equal_timestamp_order() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let budget = SeriesIndexDiskBudget::open(&store, ReadableSize(4096))
            .await
            .unwrap();
        assert_eq!(None, budget.average_size(true));
        budget.track("1/series/a.parquet".into(), metadata(1001, 0));
        assert_eq!(
            None,
            budget.average_size(true),
            "unpublished files are excluded"
        );
        budget.install("1/series/a.parquet");
        budget.track("2/series/b.parquet".into(), metadata(2000, 0));
        budget.install("2/series/b.parquet");
        budget.track("2/range/c.parquet".into(), metadata(500, 0));
        budget.install("2/range/c.parquet");
        assert_eq!(Some(1501), budget.average_size(true));
        assert_eq!(Some(500), budget.average_size(false));
        assert_eq!(
            Some(vec!["1/series/a.parquet".into()]),
            budget.admission("10/series/d.parquet", metadata(1000, 0), &[])
        );
        budget.retire("1/series/a.parquet");
        budget.retire("1/series/a.parquet");
        assert_eq!(2500, budget.used_bytes());
        assert_eq!(Some(2000), budget.average_size(true));
    }

    #[tokio::test]
    async fn test_retirement_releases_usage_before_reader_and_deletion() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let budget = SeriesIndexDiskBudget::open(&store, ReadableSize(1024))
            .await
            .unwrap();
        let path = "1/range/a.parquet";
        store.write(path, vec![0; 1024]).await.unwrap();
        budget.track(path.into(), metadata(1024, 0));
        budget.install(path);
        let pin = budget.pin(path).unwrap();
        budget.retire(path);
        assert_eq!(1024, budget.available_bytes());
        assert!(budget.pin(path).is_none());
        assert!(!budget.delete(&store, path).await.unwrap());
        assert!(store.exists(path).await.unwrap());
        drop(pin);
        budget.retry_cleanup(&store).await;
        assert!(!store.exists(path).await.unwrap());
        assert!(!budget.is_retired(path));
        assert_eq!(0, budget.used_bytes());
        // The fixed range path becomes reusable only after deletion completes.
        budget.track(path.into(), metadata(512, 0));
        budget.install(path);
        assert_eq!(512, budget.used_bytes());
    }

    struct FailingDeleter;

    impl object_store::layers::mock::oio::Delete for FailingDeleter {
        async fn delete(
            &mut self,
            _: &str,
            _: object_store::layers::mock::OpDelete,
        ) -> object_store::layers::mock::Result<()> {
            Err(object_store::layers::mock::Error::new(
                object_store::layers::mock::ErrorKind::Unexpected,
                "injected deletion failure",
            ))
        }
        async fn close(&mut self) -> object_store::layers::mock::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_failed_delete_does_not_charge_retired_file() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let budget = SeriesIndexDiskBudget::open(&store, ReadableSize(1024))
            .await
            .unwrap();
        let path = "1/range/a.parquet";
        store.write(path, vec![0; 1024]).await.unwrap();
        budget.track(path.into(), metadata(1024, 0));
        budget.install(path);
        // SST GC requests deletion, but its still-published metadata remains charged.
        assert!(!budget.delete(&store, path).await.unwrap());
        assert_eq!(1024, budget.used_bytes());
        budget.retire(path);
        let layer = object_store::layers::mock::MockLayerBuilder::default()
            .deleter_factory(Arc::new(|_| Box::new(FailingDeleter)))
            .build()
            .unwrap();
        assert!(
            budget
                .delete(&store.clone().layer(layer), path)
                .await
                .is_err()
        );
        assert_eq!(0, budget.used_bytes());
        assert!(budget.is_retired(path));
        assert!(store.exists(path).await.unwrap());
        budget.retry_cleanup(&store).await;
        assert!(!store.exists(path).await.unwrap());
        assert!(!budget.is_retired(path));
    }

    #[test]
    fn test_limit_has_no_semaphore_upper_bound() {
        assert!(validate_limit(ReadableSize(1023)).is_err());
        assert!(validate_limit(ReadableSize(1024)).is_ok());
        assert!(validate_limit(ReadableSize(u64::MAX)).is_ok());
    }
}
