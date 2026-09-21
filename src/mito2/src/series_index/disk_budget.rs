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

//! Shared KiB reservations and the lifecycle of local index output.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use common_base::readable_size::ReadableSize;
use common_telemetry::warn;
use common_time::Timestamp;
use object_store::{ErrorKind, ObjectStore, Writer};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::FileId;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use crate::error::{self, InvalidConfigSnafu, OpenDalSnafu, Result, SeriesIndexCapacitySnafu};
use crate::metrics::SERIES_INDEX_DISK_BYTES;
use crate::region::{RegionMap, RegionMapRef};

const UNIT: u64 = 1024;

pub(crate) fn validate_limit(limit: ReadableSize) -> Result<()> {
    ensure!(
        limit.as_bytes() >= UNIT && limit.as_bytes() / UNIT <= Semaphore::MAX_PERMITS as u64,
        InvalidConfigSnafu {
            reason: "experimental_series_index_max_size must be at least 1KiB and fit the semaphore capacity"
        }
    );
    Ok(())
}

pub(crate) fn is_capacity_error(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if matches!(
            error.downcast_ref::<error::Error>(),
            Some(error::Error::SeriesIndexCapacity { .. })
        ) {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

#[derive(Debug, Default)]
struct Charge(Option<OwnedSemaphorePermit>);

impl Charge {
    fn units(&self) -> usize {
        self.0.as_ref().map_or(0, OwnedSemaphorePermit::num_permits)
    }

    fn merge(&mut self, mut other: Self) {
        if let Some(permit) = other.0.take() {
            if let Some(current) = &mut self.0 {
                current.merge(permit);
            } else {
                self.0 = Some(permit);
            }
        }
    }

    fn trim(&mut self, units: usize) {
        let excess = self.units().saturating_sub(units);
        if let Some(permit) = self.0.as_mut().and_then(|permit| permit.split(excess)) {
            SERIES_INDEX_DISK_BYTES.sub((excess as u64 * UNIT) as i64);
            drop(permit);
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        SERIES_INDEX_DISK_BYTES.sub((self.units() as u64 * UNIT) as i64);
    }
}

#[derive(Debug, Default)]
struct DiskFile {
    charge: Charge,
    pin: Weak<()>,
    end: Option<Timestamp>,
    deleting: bool,
}

/// One budget per local series-index directory, shared by every worker.
#[derive(Debug)]
pub(crate) struct SeriesIndexDiskBudget {
    semaphore: Arc<Semaphore>,
    capacity: usize,
    required: AtomicU64,
    deferred: Mutex<HashMap<String, u64>>,
    files: Mutex<HashMap<String, DiskFile>>,
    paths: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    pending_deletes: Mutex<HashSet<String>>,
    pending_outputs: Mutex<Vec<DiskReservation>>,
    regions: Mutex<Vec<Weak<RegionMap>>>,
    pub(crate) maintenance: AsyncMutex<()>,
}

impl SeriesIndexDiskBudget {
    pub(crate) async fn open(store: &ObjectStore, limit: ReadableSize) -> Result<Arc<Self>> {
        validate_limit(limit)?;
        let capacity = (limit.as_bytes() / UNIT) as usize;
        let budget = Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
            required: AtomicU64::new(0),
            deferred: Mutex::default(),
            files: Mutex::default(),
            paths: Mutex::default(),
            pending_deletes: Mutex::default(),
            pending_outputs: Mutex::default(),
            regions: Mutex::default(),
            maintenance: AsyncMutex::new(()),
        });
        crate::series_index::recovery::recover(store, &budget).await?;
        Ok(budget)
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity as u64 * UNIT
    }

    pub(crate) fn available_bytes(&self) -> u64 {
        self.semaphore.available_permits() as u64 * UNIT
    }

    pub(crate) fn required_bytes(&self, key: &str) -> u64 {
        self.deferred
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn defer(&self, key: String) -> u64 {
        let required = self.required.swap(0, Ordering::Relaxed);
        self.deferred
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, required);
        required
    }

    pub(crate) fn built(&self, key: &str) {
        self.deferred
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }

    pub(crate) fn register_regions(&self, regions: &RegionMapRef) {
        self.regions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::downgrade(regions));
    }

    pub(crate) fn regions(&self) -> Vec<crate::region::MitoRegionRef> {
        self.regions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(Weak::upgrade)
            .flat_map(|regions| regions.list_regions())
            .collect()
    }

    fn acquire(&self, units: usize) -> Result<Charge> {
        let mut charge = Charge::default();
        let mut remaining = units;
        while remaining > 0 {
            let count = remaining.min(u32::MAX as usize) as u32;
            let permit = self
                .semaphore
                .clone()
                .try_acquire_many_owned(count)
                .ok()
                .context(SeriesIndexCapacitySnafu)?;
            SERIES_INDEX_DISK_BYTES.add(count as i64 * UNIT as i64);
            charge.merge(Charge(Some(permit)));
            remaining -= count as usize;
        }
        Ok(charge)
    }

    pub(crate) fn register_file(
        &self,
        path: String,
        bytes: u64,
        end: Option<Timestamp>,
    ) -> Result<()> {
        let charge = self.acquire(bytes.div_ceil(UNIT) as usize)?;
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                path,
                DiskFile {
                    charge,
                    end,
                    ..Default::default()
                },
            );
        Ok(())
    }

    pub(crate) fn set_end(&self, path: &str, end: Timestamp) {
        if let Some(file) = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(path)
        {
            file.end = Some(end);
        }
    }

    pub(crate) fn candidates(&self) -> Vec<(String, Option<Timestamp>)> {
        let pending = self
            .pending_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(path, _)| path.ends_with(".parquet") && !pending.contains(*path))
            .map(|(path, file)| (path.clone(), file.end))
            .collect()
    }

    pub(crate) fn is_retired(&self, path: &str) -> bool {
        self.pending_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(path)
    }

    pub(crate) fn pin(&self, path: &str) -> Option<Arc<()>> {
        if self.is_retired(path) {
            return None;
        }
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let file = files.get_mut(path)?;
        if file.deleting {
            return None;
        }
        let pin = file.pin.upgrade().unwrap_or_else(|| Arc::new(()));
        file.pin = Arc::downgrade(&pin);
        Some(pin)
    }

    fn path_lock(&self, path: &str) -> Arc<AsyncMutex<()>> {
        let mut paths = self
            .paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        paths.retain(|_, lock| lock.strong_count() > 0);
        let entry = paths.entry(path.to_string()).or_default();
        let lock = entry
            .upgrade()
            .unwrap_or_else(|| Arc::new(AsyncMutex::new(())));
        *entry = Arc::downgrade(&lock);
        lock
    }

    pub(crate) async fn output(
        self: &Arc<Self>,
        store: ObjectStore,
        path: &str,
    ) -> Result<IndexOutput> {
        let guard = self
            .path_lock(path)
            .try_lock_owned()
            .ok()
            .context(SeriesIndexCapacitySnafu)?;
        let stage = format!("tmp/{}.index", FileId::random());
        let reservation = DiskReservation {
            target: path.to_string(),
            stage: stage.clone(),
            charge: Charge::default(),
            bytes: 0,
            _guard: guard,
        };
        let mut output = IndexOutput {
            store: store.clone(),
            budget: self.clone(),
            reservation: Some(reservation),
            writer: None,
        };
        output.writer = Some(store.writer(&stage).await.context(OpenDalSnafu)?);
        Ok(output)
    }

    /// Returns false while a snapshot still pins the file or another mutation owns its path.
    pub(crate) async fn delete(&self, store: &ObjectStore, path: &str) -> Result<bool> {
        self.pending_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(path.to_string());
        let Ok(_guard) = self.path_lock(path).try_lock_owned() else {
            return Ok(false);
        };
        {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(file) = files.get_mut(path) {
                if file.pin.strong_count() > 0 {
                    return Ok(false);
                }
                file.deleting = true;
            }
        }
        store.delete(path).await.context(OpenDalSnafu)?;
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(path);
        self.pending_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(path);
        Ok(true)
    }

    pub(crate) async fn retry_cleanup(self: &Arc<Self>, store: &ObjectStore) {
        let outputs = std::mem::take(
            &mut *self
                .pending_outputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        if !outputs.is_empty() {
            let budget = self.clone();
            let store = store.clone();
            // Cancellation of maintenance must not drop reservations for failed output.
            if let Err(error) = common_runtime::spawn_compact(async move {
                for output in outputs {
                    budget.cleanup_output(&store, output).await;
                }
            })
            .await
            {
                warn!(error; "Failed to join deferred index cleanup");
            }
        }
        let paths = self
            .pending_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for path in paths {
            if let Err(error) = self.delete(store, &path).await {
                warn!(error; "Failed to retry index deletion, path: {path}");
            }
        }
    }

    async fn cleanup_output(&self, store: &ObjectStore, mut output: DiskReservation) {
        let result: Result<()> = async {
            store.delete(&output.stage).await.context(OpenDalSnafu)?;
            // Fs hides its atomic-write suffix. Each staging basename belongs to exactly one output.
            let name = output.stage.rsplit('/').next().unwrap_or(&output.stage);
            for entry in store
                .list(object_store::ATOMIC_WRITE_DIR)
                .await
                .context(OpenDalSnafu)?
            {
                if !entry.metadata().is_dir() && entry.name().starts_with(name) {
                    store.delete(entry.path()).await.context(OpenDalSnafu)?;
                }
            }
            // A cancelled rename may already have replaced the target. Reconcile it under
            // the same path lock, using the old and new charges already held by this operation.
            let bytes = match store.stat(&output.target).await {
                Ok(meta) => meta.content_length(),
                Err(error) if error.kind() == ErrorKind::NotFound => 0,
                Err(error) => return Err(error).context(OpenDalSnafu),
            };
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut file = files.remove(&output.target).unwrap_or_default();
            output.charge.merge(std::mem::take(&mut file.charge));
            let units = bytes.div_ceil(UNIT) as usize;
            ensure!(
                units <= output.charge.units(),
                InvalidConfigSnafu {
                    reason: "index file grew outside its disk budget"
                }
            );
            output.charge.trim(units);
            if bytes > 0 {
                file.charge = std::mem::take(&mut output.charge);
                files.insert(output.target.clone(), file);
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            warn!(error; "Failed to clean index output, path: {}", output.stage);
            self.pending_outputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(output);
        }
    }
}

#[derive(Debug)]
struct DiskReservation {
    target: String,
    stage: String,
    charge: Charge,
    bytes: u64,
    _guard: OwnedMutexGuard<()>,
}

/// Budgeted output shared by catalog writes and the existing Parquet writer.
/// Staging gives cancellation cleanup an unambiguous file identity.
pub(crate) struct IndexOutput {
    store: ObjectStore,
    budget: Arc<SeriesIndexDiskBudget>,
    reservation: Option<DiskReservation>,
    writer: Option<Writer>,
}

impl IndexOutput {
    pub(crate) async fn write(&mut self, bytes: Bytes) -> Result<()> {
        let reservation = self
            .reservation
            .as_mut()
            .context(SeriesIndexCapacitySnafu)?;
        let total = reservation
            .bytes
            .checked_add(bytes.len() as u64)
            .context(SeriesIndexCapacitySnafu)?;
        let units = total.div_ceil(UNIT) as usize;
        let charge = self
            .budget
            .acquire(units.saturating_sub(reservation.charge.units()))
            .inspect_err(|_| {
                self.budget
                    .required
                    .store(units as u64 * UNIT, Ordering::Relaxed);
            })?;
        reservation.charge.merge(charge);
        reservation.bytes = total;
        self.writer
            .as_mut()
            .context(SeriesIndexCapacitySnafu)?
            .write(bytes)
            .await
            .context(OpenDalSnafu)
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        self.writer
            .as_mut()
            .context(SeriesIndexCapacitySnafu)?
            .close()
            .await
            .context(OpenDalSnafu)?;
        drop(self.writer.take());
        let reservation = self
            .reservation
            .as_mut()
            .context(SeriesIndexCapacitySnafu)?;
        self.store
            .rename(&reservation.stage, &reservation.target)
            .await
            .context(OpenDalSnafu)?;
        let mut files = self
            .budget
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = files.remove(&reservation.target).unwrap_or_default();
        files.insert(
            reservation.target.clone(),
            DiskFile {
                charge: std::mem::take(&mut reservation.charge),
                pin: previous.pin,
                end: previous.end,
                deleting: false,
            },
        );
        self.reservation.take();
        Ok(())
    }

    pub(crate) async fn abort(&mut self) {
        if let Some(task) = self.schedule_cleanup()
            && let Err(error) = task.await
        {
            warn!(error; "Failed to join index output cleanup");
        }
    }

    fn schedule_cleanup(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        let reservation = self.reservation.take()?;
        let writer = self.writer.take();
        let budget = self.budget.clone();
        let store = self.store.clone();
        Some(common_runtime::spawn_compact(async move {
            if let Some(mut writer) = writer {
                if let Err(error) = writer.abort().await {
                    warn!(error; "Failed to abort budgeted index output");
                }
                drop(writer);
            }
            budget.cleanup_output(&store, reservation).await;
        }))
    }
}

impl Drop for IndexOutput {
    fn drop(&mut self) {
        self.schedule_cleanup();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use object_store::layers::mock::{self, MockLayerBuilder, oio};
    use tokio::sync::Notify;

    use super::*;
    use crate::config::MitoConfig;

    struct PausedWriter {
        inner: oio::Writer,
        written: Arc<Notify>,
        resume: Arc<Notify>,
    }

    impl oio::Write for PausedWriter {
        async fn write(&mut self, bytes: mock::Buffer) -> mock::Result<()> {
            self.inner.write(bytes).await?;
            self.written.notify_one();
            self.resume.notified().await;
            Ok(())
        }
        async fn close(&mut self) -> mock::Result<mock::Metadata> {
            self.inner.close().await
        }
        async fn abort(&mut self) -> mock::Result<()> {
            self.inner.abort().await
        }
    }

    struct FailingDeleter {
        inner: oio::Deleter,
        fail: Arc<AtomicBool>,
        attempted: Arc<Notify>,
    }

    impl oio::Delete for FailingDeleter {
        async fn delete(&mut self, path: &str, args: mock::OpDelete) -> mock::Result<()> {
            if self.fail.load(Ordering::Relaxed) {
                self.attempted.notify_one();
                return Err(mock::Error::new(
                    mock::ErrorKind::Unexpected,
                    "injected cleanup failure",
                ));
            }
            self.inner.delete(path, args).await
        }
        async fn close(&mut self) -> mock::Result<()> {
            self.inner.close().await
        }
    }

    #[tokio::test]
    async fn test_cancelled_replacement_retains_charge_until_cleanup() {
        let root = common_test_util::temp_dir::create_temp_dir("index-budget-cancel");
        let store = crate::access_layer::new_fs_cache_store(root.path().to_str().unwrap())
            .await
            .unwrap();
        let config = MitoConfig {
            experimental_series_index_max_size: ReadableSize::kb(4),
            ..Default::default()
        };
        let budget = SeriesIndexDiskBudget::open(&store, config.experimental_series_index_max_size)
            .await
            .unwrap();
        let path = "1/series-index.json";
        let original = Bytes::from(vec![1; 1024]);
        let mut first = budget.output(store.clone(), path).await.unwrap();
        first.write(original.clone()).await.unwrap();
        first.close().await.unwrap();

        let written = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let fail = Arc::new(AtomicBool::new(true));
        let attempted = Arc::new(Notify::new());
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new({
                let written = written.clone();
                let resume = resume.clone();
                move |_, _, inner| {
                    Box::new(PausedWriter {
                        inner,
                        written: written.clone(),
                        resume: resume.clone(),
                    })
                }
            }))
            .deleter_factory(Arc::new({
                let fail = fail.clone();
                let attempted = attempted.clone();
                move |inner| {
                    Box::new(FailingDeleter {
                        inner,
                        fail: fail.clone(),
                        attempted: attempted.clone(),
                    })
                }
            }))
            .build()
            .unwrap();
        let fault_store = store.clone().layer(layer);
        let mut replacement = budget.output(fault_store.clone(), path).await.unwrap();
        let task = tokio::spawn(async move { replacement.write(Bytes::from(vec![2; 3072])).await });
        written.notified().await;
        assert_eq!(0, budget.available_bytes());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        attempted.notified().await;
        // Synchronize with the detached cleanup task's bookkeeping by trying cleanup
        // again; failure keeps the charge regardless of whether it has been enqueued yet.
        assert_eq!(0, budget.available_bytes());
        assert_eq!(original, store.read(path).await.unwrap().to_bytes());
        let mut other = budget
            .output(store.clone(), "1/series/other.parquet")
            .await
            .unwrap();
        assert!(is_capacity_error(
            &other.write(Bytes::from_static(b"x")).await.unwrap_err()
        ));
        other.abort().await;
        fail.store(false, Ordering::Relaxed);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while budget.available_bytes() != 3072 {
                budget.retry_cleanup(&fault_store).await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(original, store.read(path).await.unwrap().to_bytes());
        assert!(
            store
                .list(object_store::ATOMIC_WRITE_DIR)
                .await
                .unwrap()
                .iter()
                .all(|entry| entry.metadata().is_dir())
        );
    }
}
