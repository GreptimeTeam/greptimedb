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

//! Worker-owned background maintenance for series indexes.

use common_meta::key::SchemaMetadataManagerRef;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_telemetry::{info, warn};
use itertools::Itertools;
use snafu::ensure;
use store_api::storage::{FileId, RegionId};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{InvalidRequestSnafu, RegionBusySnafu, Result, WorkerStoppedSnafu};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionMapRef, RegionRoleState};
use crate::request::{
    BackgroundNotify, RegionEditOutcome, RegionEditResult, Waiters, WorkerRequest,
    WorkerRequestWithTime,
};
use crate::series_index::is_sparse_metric_metadata;

use crate::sst::file::{FileHandle, WindowSequence};
use crate::time_provider::TimeProviderRef;

/// Commands serialized with periodic series-index maintenance.
#[derive(Debug)]
pub(crate) enum SeriesIndexCommand {
    Wake,
    Reconcile {
        region: MitoRegionRef,
        sender: oneshot::Sender<Result<()>>,
    },
    Stop,
}

/// Shared lifecycle state and command sender for a worker's series-index task.
#[derive(Debug)]
pub(crate) struct SeriesIndexTaskState {
    worker_id: u32,
    running: AtomicBool,
    sender: Sender<SeriesIndexCommand>,
    pub(crate) worker: Option<Sender<WorkerRequestWithTime>>,
}

impl SeriesIndexTaskState {
    pub(crate) fn new(worker_id: u32, channel_size: usize) -> (Self, Receiver<SeriesIndexCommand>) {
        let (sender, receiver) = mpsc::channel(channel_size);
        (
            Self {
                worker_id,
                running: AtomicBool::new(true),
                sender,
                worker: None,
            },
            receiver,
        )
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) {
        // Queued work already wakes the task. Periodic maintenance still runs even
        // if the queued command only reconciles one region.
        if self.is_running() && self.sender.capacity() == self.sender.max_capacity() {
            let _ = self.sender.try_send(SeriesIndexCommand::Wake);
        }
    }

    /// Returns a receiver for build completion, or RegionBusy if the queue is full.
    pub(crate) fn try_reconcile(
        &self,
        region: MitoRegionRef,
    ) -> Result<oneshot::Receiver<Result<()>>> {
        ensure!(self.is_running(), WorkerStoppedSnafu { id: self.worker_id });
        let region_id = region.region_id;
        let (sender, receiver) = oneshot::channel();
        self.sender
            .try_send(SeriesIndexCommand::Reconcile { region, sender })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RegionBusySnafu { region_id }.build(),
                mpsc::error::TrySendError::Closed(_) => {
                    WorkerStoppedSnafu { id: self.worker_id }.build()
                }
            })?;
        Ok(receiver)
    }

    pub(crate) fn stop(&self) {
        if self.running.swap(false, Ordering::AcqRel) {
            // A full queue already wakes the task, which checks the running flag.
            let _ = self.sender.try_send(SeriesIndexCommand::Stop);
        }
    }
}

/// Starts Narthex series maintenance on the compaction runtime.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_series_index_tasks(
    worker_id: u32,
    schema_manager: SchemaMetadataManagerRef,
    regions: RegionMapRef,
    state: Arc<SeriesIndexTaskState>,
    receiver: Receiver<SeriesIndexCommand>,
    bucket_width: Duration,
    min_files: usize,
    interval: Duration,
    time_provider: TimeProviderRef,
) -> JoinHandle<()> {
    common_runtime::spawn_compact(async move {
        SeriesIndexTask {
            schema_manager,
            worker_id,
            regions,
            bucket_width,
            min_files: min_files.max(1),
            failed_merge: HashMap::new(),
            state,
            receiver,
            interval,
            time_provider,
        }
        .run()
        .await;
    })
}

/// Periodic series-index maintenance for one region worker.
struct SeriesIndexTask {
    schema_manager: SchemaMetadataManagerRef,
    regions: RegionMapRef,
    bucket_width: Duration,
    min_files: usize,
    /// Resume after a failed merge write on the next pass. Holds no reader pins.
    failed_merge: HashMap<RegionId, MergeRank>,
    worker_id: u32,
    state: Arc<SeriesIndexTaskState>,
    receiver: Receiver<SeriesIndexCommand>,
    interval: Duration,
    time_provider: TimeProviderRef,
}

const MERGE_FAN_IN: usize = 4;

/// Default selection order across sequence-adjacent groups.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MergeRank {
    total_bytes: u128,
    window_start: i64,
    file_ids: [FileId; MERGE_FAN_IN],
}

struct IndexMerge {
    rank: MergeRank,
    inputs: [FileHandle; MERGE_FAN_IN],
    window: WindowSequence,
    expected_watermark: u64,
}

/// Select the cheapest balanced run across all persisted windows.
fn pick_index_merge<'a>(
    files: impl Iterator<Item = &'a FileHandle>,
    after: Option<MergeRank>,
) -> Option<IndexMerge> {
    // A permanently unreadable group must not monopolize every pass. Visit
    // later eligible groups first, then wrap around so repaired inputs retry.
    let priority = |rank: MergeRank| (after.is_some_and(|last| rank <= last), rank);
    let mut windows = HashMap::new();
    for file in files {
        if let Some(window) = file.meta_ref().series_window() {
            windows
                .entry((window.start, window.end))
                .or_insert_with(Vec::new)
                .push((file, window.max_sequence));
        }
    }
    let mut best: Option<IndexMerge> = None;
    for ((start, end), mut files) in windows {
        let expected_watermark = files.iter().map(|(_, seq)| *seq).max().unwrap_or_default();
        files.sort_unstable_by_key(|(f, seq)| (*seq, f.meta_ref().file_id));
        for group in files.windows(MERGE_FAN_IN) {
            let total = group
                .iter()
                .map(|(f, _)| u128::from(f.meta_ref().file_size))
                .sum();
            let largest = group
                .iter()
                .map(|(f, _)| f.meta_ref().file_size)
                .max()
                .unwrap_or_default();
            if !crate::compaction::bytes_balanced(largest.into(), total) {
                continue;
            }
            let rank = MergeRank {
                total_bytes: total,
                window_start: start,
                file_ids: std::array::from_fn(|i| group[i].0.meta_ref().file_id),
            };
            if best
                .as_ref()
                .is_none_or(|previous| priority(rank) < priority(previous.rank))
            {
                best = Some(IndexMerge {
                    rank,
                    inputs: std::array::from_fn(|i| group[i].0.clone()),
                    window: WindowSequence {
                        start,
                        end,
                        max_sequence: group[MERGE_FAN_IN - 1].1,
                    },
                    expected_watermark,
                });
            }
        }
    }
    best
}

impl SeriesIndexTask {
    /// Runs periodic maintenance until the worker stops.
    async fn run(mut self) {
        let worker_id = self.worker_id;
        info!("Start series-index background task, worker: {worker_id}");
        let interval = self.time_provider.wait_duration(self.interval);
        let mut deadline = Instant::now() + interval;
        while self.state.is_running() {
            // Check before receiving so a busy channel cannot starve periodic maintenance.
            if Instant::now() >= deadline {
                self.maintain().await;
                deadline = Instant::now() + interval;
                continue;
            }
            match tokio::time::timeout_at(deadline, self.receiver.recv()).await {
                Err(_) => continue,
                Ok(None | Some(SeriesIndexCommand::Stop)) => break,
                Ok(Some(SeriesIndexCommand::Wake)) => {
                    if self.state.is_running() {
                        self.maintain().await;
                        deadline = Instant::now() + interval;
                    }
                }
                Ok(Some(SeriesIndexCommand::Reconcile { region, sender })) => {
                    let result = self.reconcile_manual(region).await;
                    let _ = sender.send(result);
                }
            }
        }
        self.state.stop();
        self.receiver.close();
        while let Some(command) = self.receiver.recv().await {
            if let SeriesIndexCommand::Reconcile { sender, .. } = command {
                let _ = sender.send(WorkerStoppedSnafu { id: worker_id }.fail());
            }
        }
        info!("Stop series-index background task, worker: {worker_id}");
    }

    /// A manual request must still refer to the same writable region instance.
    fn validate_manual_region(&self, region: &MitoRegionRef) -> Result<()> {
        ensure!(
            self.state.is_running(),
            WorkerStoppedSnafu { id: self.worker_id }
        );
        let current = self.regions.writable_non_staging_region(region.region_id)?;
        ensure!(
            Arc::ptr_eq(&current, region),
            InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "region was replaced during series-index maintenance",
            }
        );
        ensure!(
            is_sparse_metric_metadata(&region.version().metadata),
            InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "series indexes require sparse metric metadata",
            }
        );
        Ok(())
    }

    async fn reconcile_manual(&mut self, region: MitoRegionRef) -> Result<()> {
        self.validate_manual_region(&region)?;
        let result = self.reconcile_region(region.clone(), true).await;
        self.validate_manual_region(&region)?;
        result
    }

    async fn reconcile_region(&mut self, region: MitoRegionRef, manual: bool) -> Result<()> {
        if region.version().options.sst_format != Some(crate::sst::FormatType::Narthex)
            || region.state() != RegionRoleState::Leader(RegionLeaderState::Writable)
        {
            return Ok(());
        }
        self.build_and_expire_native(&region, manual).await?;
        let version = region.version();
        if !version.pending_tails.is_empty() || !self.state.is_running() {
            return Ok(());
        }
        let after = self.failed_merge.remove(&region.region_id);
        if let Some(IndexMerge {
            rank,
            inputs,
            window,
            expected_watermark,
        }) = pick_index_merge(version.ssts.series_indexes().values(), after)
        {
            let truncated = version.truncated_entry_id;
            let metadata = version.metadata.clone();
            drop(version);
            let output =
                crate::sst::narthex::write_series(&region.access_layer, metadata, &inputs, window)
                    .await
                    .inspect_err(|_| {
                        self.failed_merge.insert(region.region_id, rank);
                    })?;
            let handle = FileHandle::new(output.clone(), region.file_purger.clone());
            let edit = crate::manifest::action::RegionEdit {
                series_indexes_to_add: vec![output],
                series_indexes_to_remove: inputs.iter().map(FileHandle::file_id).collect(),
                ..Default::default()
            };
            self.publish_native(
                &region,
                edit,
                &[handle],
                truncated,
                &HashMap::from([(window.start, expected_watermark)]),
            )
            .await?;
        }
        Ok(())
    }

    async fn build_and_expire_native(&self, region: &MitoRegionRef, manual: bool) -> Result<()> {
        let (ssts, metadata, frontier, truncated_entry_id, options, series_window_seconds) = {
            let current = region.version_control.current();
            let version = current.version;
            if !version.pending_tails.is_empty() {
                return Ok(());
            }
            let frontier = if version.memtables.is_empty()
                && current.last_sequence == current.committed_sequence
            {
                current.committed_sequence
            } else {
                version.flushed_sequence
            };
            (
                version.ssts.clone(),
                version.metadata.clone(),
                frontier,
                version.truncated_entry_id,
                version.options.clone(),
                version.series_window_seconds,
            )
        };
        let ttl = match options.ttl {
            Some(ttl) => ttl,
            None => crate::compaction::find_dynamic_options(
                region.region_id,
                &options,
                &self.schema_manager,
            )
            .await
            .map(|(_, ttl)| ttl)
            .unwrap_or_else(|error| {
                warn!(error; "Failed to resolve database TTL for region {}", region.region_id);
                common_time::TimeToLive::Forever
            }),
        };
        let now = common_time::Timestamp::new_millisecond(self.time_provider.current_time_millis());
        let expired = |f: &FileHandle| ttl.is_expired(&f.time_range().1, &now).unwrap_or(false);
        let width = series_window_seconds.unwrap_or_else(|| {
            i64::try_from(self.bucket_width.as_secs())
                .unwrap_or(i64::MAX)
                .max(1)
        });
        ensure!(
            width > 0,
            InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "invalid series window width"
            }
        );
        let files = || ssts.levels().iter().flat_map(|l| l.files.values());
        let live_ranges: Vec<_> = files()
            .filter(|f| !expired(f))
            .map(FileHandle::time_range)
            .collect();
        let cutoff = match ttl {
            common_time::TimeToLive::Duration(duration) => now.sub_duration(duration).ok(),
            _ => None,
        };
        // Include expired samples in a partly live window: a later watermark covers
        // the whole window, including samples still present after a TTL change.
        let first_live_start = cutoff
            .and_then(|cut| WindowSequence::starts((cut, cut), width).next())
            .map(common_time::Timestamp::new_second);
        let mut edit = crate::manifest::action::RegionEdit::default();
        let mut watermarks = HashMap::<i64, u64>::new();
        for file in ssts.series_indexes().values() {
            if let Some(w) = file.meta_ref().series_window()
                && !ttl
                    .is_expired(&common_time::Timestamp::new_second(w.end), &now)
                    .unwrap_or(false)
                && live_ranges.iter().any(|range| w.intersects(*range))
            {
                let seq = watermarks.entry(w.start).or_default();
                *seq = (*seq).max(w.max_sequence);
            } else {
                edit.series_indexes_to_remove.push(file.file_id());
            }
        }
        let mut handles: Vec<FileHandle> = Vec::new();
        // Merge the ordered window starts lazily. One wide SST can span many windows,
        // and a periodic pass only needs the first buildable one.
        let can_build = !ttl.is_instant() && live_ranges.len() >= self.min_files;
        let mut windows = files()
            .filter(|f| can_build && f.meta_ref().sequence.is_some_and(|s| s.get() <= frontier))
            .map(|f| {
                let (mut start, end) = f.time_range();
                if let Some(first) = first_live_start {
                    start = start.max(first);
                }
                let file = f.clone();
                WindowSequence::starts((start, end), width).map(move |start| (start, file.clone()))
            })
            .kmerge_by(|a, b| a.0 < b.0)
            .peekable();
        while let Some(&(start, _)) = windows.peek() {
            let previous = watermarks.get(&start).copied().unwrap_or_default();
            let mut sources = Vec::new();
            let mut live_sources = 0;
            while let Some((_, file)) = windows.next_if(|(next, _)| *next == start) {
                if file
                    .meta_ref()
                    .sequence
                    .is_some_and(|seq| seq.get() > previous)
                {
                    live_sources += usize::from(!expired(&file));
                    sources.push(file);
                }
            }
            if live_sources < self.min_files {
                continue;
            }
            let window = WindowSequence {
                start,
                end: start + width,
                max_sequence: frontier,
            };
            let output = match crate::sst::narthex::write_series(
                &region.access_layer,
                metadata.clone(),
                &sources,
                window,
            )
            .await
            {
                Ok(output) => output,
                Err(error) => {
                    for handle in handles {
                        handle.mark_deleted();
                    }
                    return Err(error);
                }
            };
            handles.push(FileHandle::new(output.clone(), region.file_purger.clone()));
            edit.series_indexes_to_add.push(output);
            if !manual {
                break;
            }
        }
        if !handles.is_empty() || !edit.series_indexes_to_remove.is_empty() {
            self.publish_native(region, edit, &handles, truncated_entry_id, &watermarks)
                .await?;
        }
        Ok(())
    }

    async fn publish_native(
        &self,
        region: &MitoRegionRef,
        edit: crate::manifest::action::RegionEdit,
        handles: &[FileHandle],
        truncated_entry_id: Option<crate::wal::EntryId>,
        watermarks: &HashMap<i64, u64>,
    ) -> Result<()> {
        let Some(worker) = self
            .state
            .worker
            .as_ref()
            .filter(|_| self.state.is_running())
        else {
            for handle in handles {
                handle.mark_deleted();
            }
            return WorkerStoppedSnafu { id: self.worker_id }.fail();
        };
        let (tx, rx) = oneshot::channel();
        let result = region
            .manifest_ctx
            .publish_series_indexes(&edit, truncated_entry_id, watermarks)
            .await;
        if let Err(error) = &result
            && (matches!(error, crate::error::Error::RegionBusy { .. })
                || !error.may_have_persisted_manifest_update())
        {
            for handle in handles {
                handle.mark_deleted();
            }
        }
        let notification = WorkerRequest::Background {
            region_id: region.region_id,
            notify: BackgroundNotify::RegionEdit(RegionEditResult {
                region_id: region.region_id,
                origin: region.clone(),
                edit,
                result: result
                    .map(|()| RegionEditOutcome::Written)
                    .map_err(Arc::new),
                waiters: Waiters::one(tx),
                is_staging: false,
                update_region_state: false,
            }),
        };
        worker
            .send(WorkerRequestWithTime::new(notification))
            .await
            .map_err(|_| WorkerStoppedSnafu { id: self.worker_id }.build())?;
        rx.await
            .map_err(|_| WorkerStoppedSnafu { id: self.worker_id }.build())??;
        Ok(())
    }

    /// Each worker contributes only its open regions, refreshed at maintenance boundaries.
    /// Runs periodic maintenance independently of incoming deletion requests.
    async fn maintain(&mut self) {
        self.failed_merge
            .retain(|id, _| self.regions.get_region(*id).is_some());
        for region in self.regions.list_regions() {
            if !self.state.is_running() {
                break;
            }
            if let Err(error) = self.reconcile_region(region.clone(), false).await {
                warn!(error; "Failed to reconcile series indexes, worker: {}, region: {}", self.worker_id, region.region_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::layers::mock::{self, MockLayerBuilder, oio};
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;
    use tokio::sync::Notify;

    use super::*;
    use crate::config::MitoConfig;
    use crate::test_util::sst_util::new_sparse_primary_key;
    use crate::test_util::{
        TestEnv, build_series_index_request, flush_region, sparse_metric_create_request,
        sparse_metric_put,
    };

    struct BlockingIndexWriter {
        inner: oio::Writer,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl mock::Write for BlockingIndexWriter {
        async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
            self.inner.write(buffer).await
        }

        async fn close(&mut self) -> mock::Result<mock::Metadata> {
            self.entered.notify_one();
            self.release.notified().await;
            self.inner.close().await
        }

        async fn abort(&mut self) -> mock::Result<()> {
            self.inner.abort().await
        }
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn test_native_index_build_releases_flushed_memtables(#[case] truncate: bool) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let writer_entered = entered.clone();
        let writer_release = release.clone();
        let pause_first_write = AtomicBool::new(true);
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| {
                if path.ends_with(".series") && pause_first_write.swap(false, Ordering::Relaxed) {
                    Box::new(BlockingIndexWriter {
                        inner,
                        entered: writer_entered.clone(),
                        release: writer_release.clone(),
                    })
                } else {
                    inner
                }
            }))
            .build()
            .unwrap();
        let mut env = TestEnv::new().await.with_mock_layer(layer);
        let engine = env
            .create_engine(MitoConfig {
                experimental_enable_series_index: true,
                experimental_series_index_min_files: 1,
                ..Default::default()
            })
            .await;
        let id = RegionId::new(1, 0);
        engine
            .handle_request(
                id,
                RegionRequest::Create(sparse_metric_create_request("narthex")),
            )
            .await
            .unwrap();
        let region = engine.get_region(id).unwrap();
        let key = new_sparse_primary_key(&["api"], &region.metadata(), 42, 7);
        engine
            .handle_request(id, sparse_metric_put(key.clone(), 1000, Some(1.0)))
            .await
            .unwrap();
        flush_region(&engine, id, None).await;
        let memtables = Arc::downgrade(&region.version().memtables);

        let build = engine.handle_request(id, build_series_index_request());
        let flush_during_build = async {
            tokio::time::timeout(Duration::from_secs(10), entered.notified())
                .await
                .unwrap();
            engine
                .handle_request(id, sparse_metric_put(key, 2000, Some(2.0)))
                .await
                .unwrap();
            flush_region(&engine, id, None).await;
            assert!(
                memtables.upgrade().is_none(),
                "the build retained the old memtable version"
            );
            if truncate {
                engine
                    .handle_request(
                        id,
                        RegionRequest::Truncate(
                            store_api::region_request::RegionTruncateRequest::All,
                        ),
                    )
                    .await
                    .unwrap();
            }
            release.notify_one();
        };
        let (result, ()) = tokio::join!(build, flush_during_build);
        if truncate {
            use common_error::ext::ErrorExt;
            assert_eq!(
                result.unwrap_err().status_code(),
                common_error::status_code::StatusCode::RegionBusy
            );
            assert!(
                region
                    .manifest_ctx
                    .manifest()
                    .await
                    .series_indexes
                    .is_empty()
            );
        } else {
            result.unwrap();
        }
        assert_eq!(region.version().ssts.series_indexes().is_empty(), truncate);
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_native_import_frontier_and_index_ttl() {
        let mut env = TestEnv::new().await;
        let clock = Arc::new(crate::time_provider::mock::MockTimeProvider::new(2000));
        let engine = env
            .create_engine_with_time(
                MitoConfig {
                    experimental_enable_series_index: true,
                    experimental_series_index_min_files: 1,
                    ..Default::default()
                },
                None,
                None,
                clock.clone(),
            )
            .await;
        let source = RegionId::new(1, 0);
        let target = RegionId::new(1, 1);
        for id in [source, target] {
            engine
                .handle_request(
                    id,
                    RegionRequest::Create(sparse_metric_create_request("narthex")),
                )
                .await
                .unwrap();
        }
        let owner = engine.get_region(source).unwrap();
        let key = new_sparse_primary_key(&["api"], &owner.metadata(), 42, 7);
        engine
            .handle_request(source, sparse_metric_put(key.clone(), 1000, Some(1.0)))
            .await
            .unwrap();
        flush_region(&engine, source, None).await;
        // A dirty memtable must not prevent indexing the previous flush.
        engine
            .handle_request(source, sparse_metric_put(key, 2000, Some(2.0)))
            .await
            .unwrap();
        engine
            .handle_request(source, build_series_index_request())
            .await
            .unwrap();
        assert_eq!(owner.version().ssts.series_indexes().len(), 1);

        let region = engine.get_region(target).unwrap();
        let sample = owner.version().ssts.levels()[0]
            .files
            .values()
            .next()
            .unwrap()
            .meta_ref()
            .clone();
        engine
            .edit_region(
                target,
                crate::manifest::action::RegionEdit {
                    files_to_add: vec![sample],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(region.version().flushed_sequence, 0);
        let mut options = region.version().options.clone();
        options.ttl = Some(common_time::TimeToLive::Duration(Duration::from_secs(1)));
        region.version_control.alter_options(options);
        engine
            .handle_request(target, build_series_index_request())
            .await
            .unwrap();
        let pinned = region.version();
        assert_eq!(pinned.ssts.series_indexes().len(), 1);
        clock.set_now(2001);
        engine
            .handle_request(target, build_series_index_request())
            .await
            .unwrap();
        assert!(region.version().ssts.series_indexes().is_empty());
        let index = pinned.ssts.series_indexes().values().next().unwrap();
        for object in index
            .meta_ref()
            .narthex_objects(&region.access_layer.build_region_dir(target))
            .unwrap()
        {
            assert!(
                region
                    .access_layer
                    .object_store()
                    .exists(&object.path)
                    .await
                    .unwrap()
            );
        }
        engine.stop().await.unwrap();
    }

    #[derive(Clone, Copy)]
    enum BrokenIndex {
        MissingPart,
        TruncatedPart,
        IncompatibleSchema,
    }

    #[rstest::rstest]
    #[case::missing_part(BrokenIndex::MissingPart)]
    #[case::truncated_part(BrokenIndex::TruncatedPart)]
    #[case::incompatible_schema(BrokenIndex::IncompatibleSchema)]
    #[tokio::test]
    async fn test_unreadable_merge_does_not_starve_another_window(#[case] broken: BrokenIndex) {
        let mut env = TestEnv::new().await;
        let engine = env
            .create_engine(MitoConfig {
                experimental_enable_series_index: true,
                experimental_series_index_min_files: 1,
                experimental_series_index_bucket_width: Duration::from_secs(10),
                ..Default::default()
            })
            .await;
        let id = RegionId::new(1, 0);
        engine
            .handle_request(
                id,
                RegionRequest::Create(sparse_metric_create_request("narthex")),
            )
            .await
            .unwrap();
        let region = engine.get_region(id).unwrap();
        let _pause = engine.pause_compaction(id).await.unwrap();
        let metadata = region.metadata();
        let short = new_sparse_primary_key(&["a"], &metadata, 42, 1);
        let long = new_sparse_primary_key(&[&"b".repeat(256)], &metadata, 42, 2);
        let flush_windows = async |offset: i64| {
            for (ts, key) in [
                (1_000 + offset, short.clone()),
                (11_000 + offset, long.clone()),
            ] {
                engine
                    .handle_request(id, sparse_metric_put(key, ts, Some(1.0)))
                    .await
                    .unwrap();
                flush_region(&engine, id, None).await;
            }
        };
        let reconcile = async || {
            engine
                .handle_request(id, build_series_index_request())
                .await
        };
        for offset in 0..3 {
            flush_windows(offset).await;
            reconcile().await.unwrap();
        }
        let version = region.version();
        let cheapest = version
            .ssts
            .series_indexes()
            .values()
            .min_by_key(|f| {
                (
                    f.meta_ref().file_size,
                    f.meta_ref().series_window().unwrap().start,
                )
            })
            .unwrap();
        let broken_window = cheapest.meta_ref().series_window().unwrap().start;
        let suffix = match broken {
            BrokenIndex::IncompatibleSchema => "manifest.json",
            BrokenIndex::MissingPart | BrokenIndex::TruncatedPart => ".series",
        };
        let path = cheapest
            .meta_ref()
            .narthex_objects(&region.access_layer.build_region_dir(id))
            .unwrap()
            .into_iter()
            .find(|o| o.path.ends_with(suffix))
            .unwrap()
            .path;
        let store = region.access_layer.object_store();
        let saved = store.read(&path).await.unwrap().to_bytes();
        match broken {
            BrokenIndex::MissingPart => store.delete(&path).await.unwrap(),
            BrokenIndex::TruncatedPart => {
                store.write(&path, saved.slice(..1)).await.unwrap();
            }
            BrokenIndex::IncompatibleSchema => {
                let mut manifest: crate::series_index::columnar::Manifest =
                    serde_json::from_slice(&saved).unwrap();
                manifest.tags[0].column_id = 9;
                assert!(metadata.column_by_id(9).is_none());
                let incompatible = serde_json::to_vec(&manifest).unwrap();
                assert_eq!(saved.len(), incompatible.len());
                store.write(&path, incompatible).await.unwrap();
            }
        }
        drop(version);
        flush_windows(3).await;
        let error = reconcile().await.unwrap_err();
        if matches!(broken, BrokenIndex::IncompatibleSchema) {
            assert!(
                format!("{error:?}").contains("series tag absent from scan schema"),
                "{error:?}"
            );
        }
        assert_eq!(region.version().ssts.series_indexes().len(), 8);
        reconcile().await.unwrap();
        let version = region.version();
        assert_eq!(version.ssts.series_indexes().len(), 5);
        assert_eq!(
            version
                .ssts
                .series_indexes()
                .values()
                .filter(|f| f.meta_ref().series_window().unwrap().start == broken_window)
                .count(),
            4
        );
        store.write(&path, saved).await.unwrap();
        reconcile().await.unwrap();
        assert_eq!(region.version().ssts.series_indexes().len(), 2);
        engine.stop().await.unwrap();
    }

    #[test]
    fn test_pick_index_merge() {
        use crate::sst::file::{FileFormat, FileMeta, NarthexSeriesIndexMeta};
        use store_api::storage::FileId;

        let file = |id, mib, start| {
            FileHandle::new(
                FileMeta {
                    file_id: FileId::from_bytes([id; 16]),
                    file_size: mib * (1u64 << 20),
                    format: FileFormat::NarthexSeriesIndex(Box::new(NarthexSeriesIndexMeta {
                        manifest_size: 0,
                        parts: Default::default(),
                        window: WindowSequence {
                            start,
                            end: start + 10,
                            max_sequence: u64::from(id) * 10,
                        },
                    })),
                    ..Default::default()
                },
                crate::test_util::new_noop_file_purger(),
            )
        };
        // The 100 MiB file lies between the small files in watermark order.
        let separated = [
            file(1, 1, 0),
            file(2, 100, 0),
            file(3, 1, 0),
            file(4, 1, 0),
            file(5, 1, 0),
        ];
        assert!(pick_index_merge(separated.iter(), None).is_none());
        let mut files = vec![
            file(0, 100, 0),
            file(1, 1, 0),
            file(2, 1, 0),
            file(3, 1, 0),
            file(5, 1, 10),
        ];
        assert!(pick_index_merge(files.iter(), None).is_none());
        files.push(file(4, 1, 0));
        let picked = |files: &[FileHandle]| {
            pick_index_merge(files.iter(), None)
                .unwrap()
                .inputs
                .map(|f| f.meta_ref().file_id.as_bytes()[0])
        };
        assert_eq!(picked(&files), [1, 2, 3, 4]);
        files.extend([
            file(6, 1, 10),
            file(7, 1, 10),
            file(8, 1, 10),
            file(9, 1, 0),
        ]);
        files.reverse();
        assert_eq!(picked(&files), [1, 2, 3, 4]);
        files.extend([
            file(10, 0, 20),
            file(11, 0, 20),
            file(12, 0, 20),
            file(13, 0, 20),
        ]);
        assert_eq!(picked(&files), [10, 11, 12, 13]);
    }

    #[test]
    fn test_coalesce_wakeups_and_stop() {
        let (state, mut receiver) = SeriesIndexTaskState::new(0, 2);
        state.wake();
        state.wake();
        assert!(matches!(
            receiver.try_recv().unwrap(),
            SeriesIndexCommand::Wake
        ));
        assert!(receiver.try_recv().is_err());
        state.wake();
        assert!(matches!(
            receiver.try_recv().unwrap(),
            SeriesIndexCommand::Wake
        ));
        state.stop();
        state.stop();
        state.wake();
        assert!(matches!(
            receiver.try_recv().unwrap(),
            SeriesIndexCommand::Stop
        ));
        assert!(receiver.try_recv().is_err());
    }
}
