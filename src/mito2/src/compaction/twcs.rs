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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_telemetry::{debug, error, info};
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use tokio::sync::mpsc;

use crate::access_layer::{AccessLayerRef, SstWriteRequest};
use crate::cache::CacheManagerRef;
use crate::compaction::picker::{CompactionTask, Picker};
use crate::compaction::CompactionRequest;
use crate::config::MitoConfig;
use crate::error::{self, CompactRegionSnafu};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::metrics::{COMPACTION_FAILURE_COUNT, COMPACTION_STAGE_ELAPSED};
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::ScanInput;
use crate::read::seq_scan::SeqScan;
use crate::read::{BoxedBatchReader, Source};
use crate::region::options::IndexOptions;
use crate::region::version::VersionControlRef;
use crate::region::{ManifestContextRef, RegionState};
use crate::request::{
    BackgroundNotify, CompactionFailed, CompactionFinished, OutputTx, WorkerRequest,
};
use crate::sst::file::{FileHandle, FileId, FileMeta, IndexType, Level};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::parquet::WriteOptions;
use crate::sst::version::LevelMeta;
use crate::worker::WorkerListener;

const MAX_PARALLEL_COMPACTION: usize = 8;

/// `TwcsPicker` picks files of which the max timestamp are in the same time window as compaction
/// candidates.
pub struct TwcsPicker {
    max_active_window_files: usize,
    max_inactive_window_files: usize,
    time_window_seconds: Option<i64>,
}

impl Debug for TwcsPicker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwcsPicker")
            .field("max_active_window_files", &self.max_active_window_files)
            .field("max_inactive_window_files", &self.max_inactive_window_files)
            .finish()
    }
}

impl TwcsPicker {
    pub fn new(
        max_active_window_files: usize,
        max_inactive_window_files: usize,
        time_window_seconds: Option<i64>,
    ) -> Self {
        Self {
            max_inactive_window_files,
            max_active_window_files,
            time_window_seconds,
        }
    }

    /// Builds compaction output from files.
    /// For active writing window, we allow for at most `max_active_window_files` files to alleviate
    /// fragmentation. For other windows, we allow at most 1 file at each window.
    fn build_output(
        &self,
        time_windows: &BTreeMap<i64, Window>,
        active_window: Option<i64>,
    ) -> Vec<CompactionOutput> {
        let mut output = vec![];
        for (window, files) in time_windows {
            let files_in_window = &files.files;
            // we only remove deletion markers once no file in current window overlaps with any other window.
            let filter_deleted = !files.overlapping;

            if let Some(active_window) = active_window
                && *window == active_window
            {
                if files_in_window.len() > self.max_active_window_files {
                    output.push(CompactionOutput {
                        output_file_id: FileId::random(),
                        output_level: 1, // we only have two levels and always compact to l1
                        inputs: files_in_window.clone(),
                        filter_deleted,
                    });
                } else {
                    debug!("Active window not present or no enough files in active window {:?}, window: {}", active_window, *window);
                }
            } else {
                // not active writing window
                if files_in_window.len() > self.max_inactive_window_files {
                    output.push(CompactionOutput {
                        output_file_id: FileId::random(),
                        output_level: 1,
                        inputs: files_in_window.clone(),
                        filter_deleted,
                    });
                } else {
                    debug!(
                        "No enough files, current: {}, max_inactive_window_files: {}",
                        files_in_window.len(),
                        self.max_inactive_window_files
                    )
                }
            }
        }
        output
    }
}

impl Picker for TwcsPicker {
    fn pick(&self, req: CompactionRequest) -> Option<Box<dyn CompactionTask>> {
        let CompactionRequest {
            engine_config,
            current_version,
            access_layer,
            compact_type,
            request_sender,
            waiters,
            file_purger,
            start_time,
            cache_manager,
            manifest_ctx,
            version_control,
            listener,
        } = req;

        let region_metadata = current_version.metadata.clone();
        let region_id = region_metadata.region_id;

        let levels = current_version.ssts.levels();
        let ttl = current_version.options.ttl;
        let expired_ssts = get_expired_ssts(levels, ttl, Timestamp::current_millis());
        if !expired_ssts.is_empty() {
            info!("Expired SSTs in region {}: {:?}", region_id, expired_ssts);
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.set_compacting(true));
        }

        let compaction_time_window = current_version
            .compaction_time_window
            .map(|window| window.as_secs() as i64);
        let time_window_size = compaction_time_window
            .or(self.time_window_seconds)
            .unwrap_or_else(|| {
                let inferred = infer_time_bucket(levels[0].files());
                info!(
                    "Compaction window for region {} is not present, inferring from files: {:?}",
                    region_id, inferred
                );
                inferred
            });

        // Find active window from files in level 0.
        let active_window = find_latest_window_in_seconds(levels[0].files(), time_window_size);
        // Assign files to windows
        let windows = assign_to_windows(levels.iter().flat_map(LevelMeta::files), time_window_size);
        let outputs = self.build_output(&windows, active_window);

        if outputs.is_empty() && expired_ssts.is_empty() {
            // Nothing to compact, we are done. Notifies all waiters as we consume the compaction request.
            for waiter in waiters {
                waiter.send(Ok(0));
            }
            return None;
        }
        let task = TwcsCompactionTask {
            engine_config,
            region_id,
            metadata: region_metadata,
            sst_layer: access_layer,
            outputs,
            expired_ssts,
            compaction_time_window: Some(time_window_size),
            request_sender,
            waiters,
            file_purger,
            start_time,
            cache_manager,
            storage: current_version.options.storage.clone(),
            index_options: current_version.options.index_options.clone(),
            append_mode: current_version.options.append_mode,
            manifest_ctx,
            version_control,
            listener,
        };
        Some(Box::new(task))
    }
}

struct Window {
    start: Timestamp,
    end: Timestamp,
    files: Vec<FileHandle>,
    time_window: i64,
    overlapping: bool,
}

impl Window {
    /// Creates a new [Window] with given file.
    fn new_with_file(file: FileHandle) -> Self {
        let (start, end) = file.time_range();
        Self {
            start,
            end,
            files: vec![file],
            time_window: 0,
            overlapping: false,
        }
    }

    /// Returns the time range of all files in current window (inclusive).
    fn range(&self) -> (Timestamp, Timestamp) {
        (self.start, self.end)
    }

    /// Adds a new file to window and updates time range.
    fn add_file(&mut self, file: FileHandle) {
        let (start, end) = file.time_range();
        self.start = self.start.min(start);
        self.end = self.end.max(end);
        self.files.push(file);
    }
}

/// Assigns files to windows with predefined window size (in seconds) by their max timestamps.
fn assign_to_windows<'a>(
    files: impl Iterator<Item = &'a FileHandle>,
    time_window_size: i64,
) -> BTreeMap<i64, Window> {
    let mut windows: HashMap<i64, Window> = HashMap::new();
    // Iterates all files and assign to time windows according to max timestamp
    for f in files {
        let (_, end) = f.time_range();
        let time_window = end
            .convert_to(TimeUnit::Second)
            .unwrap()
            .value()
            .align_to_ceil_by_bucket(time_window_size)
            .unwrap_or(i64::MIN);

        match windows.entry(time_window) {
            Entry::Occupied(mut e) => {
                e.get_mut().add_file(f.clone());
            }
            Entry::Vacant(e) => {
                let mut window = Window::new_with_file(f.clone());
                window.time_window = time_window;
                e.insert(window);
            }
        }
    }
    if windows.is_empty() {
        return BTreeMap::new();
    }

    let mut windows = windows.into_values().collect::<Vec<_>>();
    windows.sort_unstable_by(|l, r| l.start.cmp(&r.start).then(l.end.cmp(&r.end).reverse()));

    let mut current_range: (Timestamp, Timestamp) = windows[0].range(); // windows cannot be empty.

    for idx in 1..windows.len() {
        let next_range = windows[idx].range();
        if overlaps(&current_range, &next_range) {
            windows[idx - 1].overlapping = true;
            windows[idx].overlapping = true;
        }
        current_range = (
            current_range.0.min(next_range.0),
            current_range.1.max(next_range.1),
        );
    }

    windows.into_iter().map(|w| (w.time_window, w)).collect()
}

/// Checks if two inclusive timestamp ranges overlap with each other.
fn overlaps(l: &(Timestamp, Timestamp), r: &(Timestamp, Timestamp)) -> bool {
    let (l, r) = if l.0 <= r.0 { (l, r) } else { (r, l) };
    let (_, l_end) = l;
    let (r_start, _) = r;

    r_start <= l_end
}

/// Finds the latest active writing window among all files.
/// Returns `None` when there are no files or all files are corrupted.
fn find_latest_window_in_seconds<'a>(
    files: impl Iterator<Item = &'a FileHandle>,
    time_window_size: i64,
) -> Option<i64> {
    let mut latest_timestamp = None;
    for f in files {
        let (_, end) = f.time_range();
        if let Some(latest) = latest_timestamp
            && end > latest
        {
            latest_timestamp = Some(end);
        } else {
            latest_timestamp = Some(end);
        }
    }
    latest_timestamp
        .and_then(|ts| ts.convert_to_ceil(TimeUnit::Second))
        .and_then(|ts| ts.value().align_to_ceil_by_bucket(time_window_size))
}

pub(crate) struct TwcsCompactionTask {
    pub engine_config: Arc<MitoConfig>,
    pub region_id: RegionId,
    pub metadata: RegionMetadataRef,
    pub sst_layer: AccessLayerRef,
    pub outputs: Vec<CompactionOutput>,
    pub expired_ssts: Vec<FileHandle>,
    pub compaction_time_window: Option<i64>,
    pub file_purger: FilePurgerRef,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    /// Senders that are used to notify waiters waiting for pending compaction tasks.
    pub waiters: Vec<OutputTx>,
    /// Start time of compaction task
    pub start_time: Instant,
    pub(crate) cache_manager: CacheManagerRef,
    /// Target storage of the region.
    pub(crate) storage: Option<String>,
    /// Index options of the region.
    pub(crate) index_options: IndexOptions,
    /// The region is using append mode.
    pub(crate) append_mode: bool,
    /// Manifest context.
    pub(crate) manifest_ctx: ManifestContextRef,
    /// Version control to update.
    pub(crate) version_control: VersionControlRef,
    /// Event listener.
    pub(crate) listener: WorkerListener,
}

impl Debug for TwcsCompactionTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwcsCompactionTask")
            .field("region_id", &self.region_id)
            .field("outputs", &self.outputs)
            .field("expired_ssts", &self.expired_ssts)
            .field("compaction_time_window", &self.compaction_time_window)
            .field("append_mode", &self.append_mode)
            .finish()
    }
}

impl Drop for TwcsCompactionTask {
    fn drop(&mut self) {
        self.mark_files_compacting(false)
    }
}

impl TwcsCompactionTask {
    fn mark_files_compacting(&self, compacting: bool) {
        self.outputs
            .iter()
            .flat_map(|o| o.inputs.iter())
            .for_each(|f| f.set_compacting(compacting))
    }

    /// Merges all SST files.
    /// Returns `(output files, input files)`.
    async fn merge_ssts(&mut self) -> error::Result<(Vec<FileMeta>, Vec<FileMeta>)> {
        let mut futs = Vec::with_capacity(self.outputs.len());
        let mut compacted_inputs =
            Vec::with_capacity(self.outputs.iter().map(|o| o.inputs.len()).sum());

        for output in self.outputs.drain(..) {
            compacted_inputs.extend(output.inputs.iter().map(FileHandle::meta));

            info!(
                "Compaction region {}. Input [{}] -> output {}",
                self.region_id,
                output
                    .inputs
                    .iter()
                    .map(|f| f.file_id().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                output.output_file_id
            );

            let write_opts = WriteOptions {
                write_buffer_size: self.engine_config.sst_write_buffer_size,
                ..Default::default()
            };
            let create_inverted_index = self
                .engine_config
                .inverted_index
                .create_on_compaction
                .auto();
            let mem_threshold_index_create = self
                .engine_config
                .inverted_index
                .mem_threshold_on_create
                .map(|m| m.as_bytes() as _);
            let index_write_buffer_size = Some(
                self.engine_config
                    .inverted_index
                    .write_buffer_size
                    .as_bytes() as usize,
            );

            let metadata = self.metadata.clone();
            let sst_layer = self.sst_layer.clone();
            let region_id = self.region_id;
            let file_id = output.output_file_id;
            let cache_manager = self.cache_manager.clone();
            let storage = self.storage.clone();
            let index_options = self.index_options.clone();
            let append_mode = self.append_mode;
            futs.push(async move {
                let reader = build_sst_reader(
                    metadata.clone(),
                    sst_layer.clone(),
                    Some(cache_manager.clone()),
                    &output.inputs,
                    append_mode,
                    output.filter_deleted,
                )
                .await?;
                let file_meta_opt = sst_layer
                    .write_sst(
                        SstWriteRequest {
                            file_id,
                            metadata,
                            source: Source::Reader(reader),
                            cache_manager,
                            storage,
                            create_inverted_index,
                            mem_threshold_index_create,
                            index_write_buffer_size,
                            index_options,
                        },
                        &write_opts,
                    )
                    .await?
                    .map(|sst_info| FileMeta {
                        region_id,
                        file_id,
                        time_range: sst_info.time_range,
                        level: output.output_level,
                        file_size: sst_info.file_size,
                        available_indexes: sst_info
                            .inverted_index_available
                            .then(|| SmallVec::from_iter([IndexType::InvertedIndex]))
                            .unwrap_or_default(),
                        index_file_size: sst_info.index_file_size,
                    });
                Ok(file_meta_opt)
            });
        }

        let mut output_files = Vec::with_capacity(futs.len());
        while !futs.is_empty() {
            let mut task_chunk = Vec::with_capacity(MAX_PARALLEL_COMPACTION);
            for _ in 0..MAX_PARALLEL_COMPACTION {
                if let Some(task) = futs.pop() {
                    task_chunk.push(common_runtime::spawn_bg(task));
                }
            }
            let metas = futures::future::try_join_all(task_chunk)
                .await
                .context(error::JoinSnafu)?
                .into_iter()
                .collect::<error::Result<Vec<_>>>()?;
            output_files.extend(metas.into_iter().flatten());
        }

        let inputs = compacted_inputs.into_iter().collect();
        Ok((output_files, inputs))
    }

    async fn handle_compaction(&mut self) -> error::Result<()> {
        self.mark_files_compacting(true);
        let merge_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .start_timer();
        let (added, mut deleted) = match self.merge_ssts().await {
            Ok(v) => v,
            Err(e) => {
                error!(e; "Failed to compact region: {}", self.region_id);
                merge_timer.stop_and_discard();
                return Err(e);
            }
        };
        deleted.extend(self.expired_ssts.iter().map(FileHandle::meta));
        let merge_time = merge_timer.stop_and_record();
        info!(
            "Compacted SST files, region_id: {}, input: {:?}, output: {:?}, window: {:?}, waiter_num: {}, merge_time: {}s",
            self.region_id,
            deleted,
            added,
            self.compaction_time_window,
            self.waiters.len(),
            merge_time,
        );

        self.listener.on_merge_ssts_finished(self.region_id).await;

        let _manifest_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["write_manifest"])
            .start_timer();
        // Write region edit to manifest.
        let edit = RegionEdit {
            files_to_add: added,
            files_to_remove: deleted,
            compaction_time_window: self
                .compaction_time_window
                .map(|seconds| Duration::from_secs(seconds as u64)),
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        // We might leak files if we fail to update manifest. We can add a cleanup task to
        // remove them later.
        self.manifest_ctx
            .update_manifest(RegionState::Writable, action_list, || {
                self.version_control
                    .apply_edit(edit, &[], self.file_purger.clone());
            })
            .await
    }

    /// Handles compaction failure, notifies all waiters.
    fn on_failure(&mut self, err: Arc<error::Error>) {
        COMPACTION_FAILURE_COUNT.inc();
        for waiter in self.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu {
                region_id: self.region_id,
            }));
        }
    }

    /// Notifies region worker to handle post-compaction tasks.
    async fn send_to_worker(&self, request: WorkerRequest) {
        if let Err(e) = self.request_sender.send(request).await {
            error!(
                "Failed to notify compaction job status for region {}, request: {:?}",
                self.region_id, e.0
            );
        }
    }
}

#[async_trait::async_trait]
impl CompactionTask for TwcsCompactionTask {
    async fn run(&mut self) {
        let notify = match self.handle_compaction().await {
            Ok(()) => BackgroundNotify::CompactionFinished(CompactionFinished {
                region_id: self.region_id,
                senders: std::mem::take(&mut self.waiters),
                start_time: self.start_time,
            }),
            Err(e) => {
                error!(e; "Failed to compact region, region id: {}", self.region_id);
                let err = Arc::new(e);
                // notify compaction waiters
                self.on_failure(err.clone());
                BackgroundNotify::CompactionFailed(CompactionFailed {
                    region_id: self.region_id,
                    err,
                })
            }
        };

        self.send_to_worker(WorkerRequest::Background {
            region_id: self.region_id,
            notify,
        })
        .await;
    }
}

/// Infers the suitable time bucket duration.
/// Now it simply find the max and min timestamp across all SSTs in level and fit the time span
/// into time bucket.
pub(crate) fn infer_time_bucket<'a>(files: impl Iterator<Item = &'a FileHandle>) -> i64 {
    let mut max_ts = Timestamp::new(i64::MIN, TimeUnit::Second);
    let mut min_ts = Timestamp::new(i64::MAX, TimeUnit::Second);

    for f in files {
        let (start, end) = f.time_range();
        min_ts = min_ts.min(start);
        max_ts = max_ts.max(end);
    }

    // safety: Convert whatever timestamp into seconds will not cause overflow.
    let min_sec = min_ts.convert_to(TimeUnit::Second).unwrap().value();
    let max_sec = max_ts.convert_to(TimeUnit::Second).unwrap().value();

    max_sec
        .checked_sub(min_sec)
        .map(|span| TIME_BUCKETS.fit_time_bucket(span)) // return the max bucket on subtraction overflow.
        .unwrap_or_else(|| TIME_BUCKETS.max()) // safety: TIME_BUCKETS cannot be empty.
}

pub(crate) struct TimeBuckets([i64; 7]);

impl TimeBuckets {
    /// Fits a given time span into time bucket by find the minimum bucket that can cover the span.
    /// Returns the max bucket if no such bucket can be found.
    fn fit_time_bucket(&self, span_sec: i64) -> i64 {
        assert!(span_sec >= 0);
        match self.0.binary_search(&span_sec) {
            Ok(idx) => self.0[idx],
            Err(idx) => {
                if idx < self.0.len() {
                    self.0[idx]
                } else {
                    self.0.last().copied().unwrap()
                }
            }
        }
    }

    #[cfg(test)]
    fn get(&self, idx: usize) -> i64 {
        self.0[idx]
    }

    fn max(&self) -> i64 {
        self.0.last().copied().unwrap()
    }
}

/// A set of predefined time buckets.
pub(crate) const TIME_BUCKETS: TimeBuckets = TimeBuckets([
    60 * 60,                 // one hour
    2 * 60 * 60,             // two hours
    12 * 60 * 60,            // twelve hours
    24 * 60 * 60,            // one day
    7 * 24 * 60 * 60,        // one week
    365 * 24 * 60 * 60,      // one year
    10 * 365 * 24 * 60 * 60, // ten years
]);

/// Finds all expired SSTs across levels.
fn get_expired_ssts(
    levels: &[LevelMeta],
    ttl: Option<Duration>,
    now: Timestamp,
) -> Vec<FileHandle> {
    let Some(ttl) = ttl else {
        return vec![];
    };

    let expire_time = match now.sub_duration(ttl) {
        Ok(expire_time) => expire_time,
        Err(e) => {
            error!(e; "Failed to calculate region TTL expire time");
            return vec![];
        }
    };

    levels
        .iter()
        .flat_map(|l| l.get_expired_files(&expire_time).into_iter())
        .collect()
}

#[derive(Debug)]
pub(crate) struct CompactionOutput {
    pub output_file_id: FileId,
    /// Compaction output file level.
    pub output_level: Level,
    /// Compaction input files.
    pub inputs: Vec<FileHandle>,
    /// Whether to remove deletion markers.
    pub filter_deleted: bool,
}

/// Builds [BoxedBatchReader] that reads all SST files and yields batches in primary key order.
async fn build_sst_reader(
    metadata: RegionMetadataRef,
    sst_layer: AccessLayerRef,
    cache: Option<CacheManagerRef>,
    inputs: &[FileHandle],
    append_mode: bool,
    filter_deleted: bool,
) -> error::Result<BoxedBatchReader> {
    let scan_input = ScanInput::new(sst_layer, ProjectionMapper::all(&metadata)?)
        .with_files(inputs.to_vec())
        .with_cache(cache)
        .with_append_mode(append_mode)
        .with_filter_deleted(filter_deleted)
        // We ignore file not found error during compaction.
        .with_ignore_file_not_found(true);
    SeqScan::new(scan_input).build_reader().await
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::compaction::test_util::new_file_handle;
    use crate::sst::file::Level;

    #[test]
    fn test_get_latest_window_in_seconds() {
        assert_eq!(
            Some(1),
            find_latest_window_in_seconds([new_file_handle(FileId::random(), 0, 999, 0)].iter(), 1)
        );
        assert_eq!(
            Some(1),
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), 0, 1000, 0)].iter(),
                1
            )
        );

        assert_eq!(
            Some(-9223372036854000),
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), i64::MIN, i64::MIN + 1, 0)].iter(),
                3600,
            )
        );

        assert_eq!(
            (i64::MAX / 10000000 + 1) * 10000,
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), i64::MIN, i64::MAX, 0)].iter(),
                10000,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_assign_to_windows() {
        let windows = assign_to_windows(
            [
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
            ]
            .iter(),
            3,
        );
        assert_eq!(5, windows.get(&0).unwrap().files.len());

        let files = [FileId::random(); 3];
        let windows = assign_to_windows(
            [
                new_file_handle(files[0], -2000, -3, 0),
                new_file_handle(files[1], 0, 2999, 0),
                new_file_handle(files[2], 50, 10001, 0),
            ]
            .iter(),
            3,
        );
        assert_eq!(
            files[0],
            windows.get(&0).unwrap().files.first().unwrap().file_id()
        );
        assert_eq!(
            files[1],
            windows.get(&3).unwrap().files.first().unwrap().file_id()
        );
        assert_eq!(
            files[2],
            windows.get(&12).unwrap().files.first().unwrap().file_id()
        );
    }

    /// (Window value, overlapping, files' time ranges in window)
    type ExpectedWindowSpec = (i64, bool, Vec<(i64, i64)>);

    fn check_assign_to_windows_with_overlapping(
        file_time_ranges: &[(i64, i64)],
        time_window: i64,
        expected_files: &[ExpectedWindowSpec],
    ) {
        let files: Vec<_> = (0..file_time_ranges.len())
            .map(|_| FileId::random())
            .collect();

        let file_handles = files
            .iter()
            .zip(file_time_ranges.iter())
            .map(|(file_id, range)| new_file_handle(*file_id, range.0, range.1, 0))
            .collect::<Vec<_>>();

        let windows = assign_to_windows(file_handles.iter(), time_window);

        for (expected_window, overlapping, window_files) in expected_files {
            let actual_window = windows.get(expected_window).unwrap();
            assert_eq!(*overlapping, actual_window.overlapping);
            let mut file_ranges = actual_window
                .files
                .iter()
                .map(|f| {
                    let (s, e) = f.time_range();
                    (s.value(), e.value())
                })
                .collect::<Vec<_>>();
            file_ranges.sort_unstable_by(|l, r| l.0.cmp(&r.0).then(l.1.cmp(&r.1)));
            assert_eq!(window_files, &file_ranges);
        }
    }

    #[test]
    fn test_assign_to_windows_with_overlapping() {
        check_assign_to_windows_with_overlapping(
            &[(0, 999), (1000, 1999), (2000, 2999)],
            2,
            &[
                (0, false, vec![(0, 999)]),
                (2, false, vec![(1000, 1999), (2000, 2999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[(0, 1), (0, 999), (100, 2999)],
            2,
            &[
                (0, true, vec![(0, 1), (0, 999)]),
                (2, true, vec![(100, 2999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[(0, 999), (1000, 1999), (2000, 2999), (3000, 3999)],
            2,
            &[
                (0, false, vec![(0, 999)]),
                (2, false, vec![(1000, 1999), (2000, 2999)]),
                (4, false, vec![(3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),
                (1000, 1999),
                (2000, 2999),
                (3000, 3999),
                (0, 3999),
            ],
            2,
            &[
                (0, true, vec![(0, 999)]),
                (2, true, vec![(1000, 1999), (2000, 2999)]),
                (4, true, vec![(0, 3999), (3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),
                (1000, 1999),
                (2000, 2999),
                (3000, 3999),
                (1999, 3999),
            ],
            2,
            &[
                (0, false, vec![(0, 999)]),
                (2, true, vec![(1000, 1999), (2000, 2999)]),
                (4, true, vec![(1999, 3999), (3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),     // window 0
                (1000, 1999), // window 2
                (2000, 2999), // window 2
                (3000, 3999), // window 4
                (2999, 3999), // window 4
            ],
            2,
            &[
                // window 2 overlaps with window 4
                (0, false, vec![(0, 999)]),
                (2, true, vec![(1000, 1999), (2000, 2999)]),
                (4, true, vec![(2999, 3999), (3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),     // window 0
                (1000, 1999), // window 2
                (2000, 2999), // window 2
                (3000, 3999), // window 4
                (0, 1000),    // // window 2
            ],
            2,
            &[
                // only window 0 overlaps with window 2.
                (0, true, vec![(0, 999)]),
                (2, true, vec![(0, 1000), (1000, 1999), (2000, 2999)]),
                (4, false, vec![(3000, 3999)]),
            ],
        );
    }

    struct CompactionPickerTestCase {
        window_size: i64,
        input_files: Vec<FileHandle>,
        expected_outputs: Vec<ExpectedOutput>,
    }

    impl CompactionPickerTestCase {
        fn check(&self) {
            let windows = assign_to_windows(self.input_files.iter(), self.window_size);
            let active_window =
                find_latest_window_in_seconds(self.input_files.iter(), self.window_size);
            let output = TwcsPicker::new(4, 1, None).build_output(&windows, active_window);

            let output = output
                .iter()
                .map(|o| {
                    let input_file_ids =
                        o.inputs.iter().map(|f| f.file_id()).collect::<HashSet<_>>();
                    (input_file_ids, o.output_level)
                })
                .collect::<Vec<_>>();

            let expected = self
                .expected_outputs
                .iter()
                .map(|o| {
                    let input_file_ids = o
                        .input_files
                        .iter()
                        .map(|idx| self.input_files[*idx].file_id())
                        .collect::<HashSet<_>>();
                    (input_file_ids, o.output_level)
                })
                .collect::<Vec<_>>();
            assert_eq!(expected, output);
        }
    }

    struct ExpectedOutput {
        input_files: Vec<usize>,
        output_level: Level,
    }

    #[test]
    fn test_build_twcs_output() {
        let file_ids = (0..4).map(|_| FileId::random()).collect::<Vec<_>>();

        CompactionPickerTestCase {
            window_size: 3,
            input_files: [
                new_file_handle(file_ids[0], -2000, -3, 0),
                new_file_handle(file_ids[1], -3000, -100, 0),
                new_file_handle(file_ids[2], 0, 2999, 0), //active windows
                new_file_handle(file_ids[3], 50, 2998, 0), //active windows
            ]
            .to_vec(),
            expected_outputs: vec![ExpectedOutput {
                input_files: vec![0, 1],
                output_level: 1,
            }],
        }
        .check();

        let file_ids = (0..6).map(|_| FileId::random()).collect::<Vec<_>>();
        CompactionPickerTestCase {
            window_size: 3,
            input_files: [
                new_file_handle(file_ids[0], -2000, -3, 0),
                new_file_handle(file_ids[1], -3000, -100, 0),
                new_file_handle(file_ids[2], 0, 2999, 0),
                new_file_handle(file_ids[3], 50, 2998, 0),
                new_file_handle(file_ids[4], 11, 2990, 0),
                new_file_handle(file_ids[5], 50, 4998, 0),
            ]
            .to_vec(),
            expected_outputs: vec![
                ExpectedOutput {
                    input_files: vec![0, 1],
                    output_level: 1,
                },
                ExpectedOutput {
                    input_files: vec![2, 3, 4],
                    output_level: 1,
                },
            ],
        }
        .check();
    }

    #[test]
    fn test_time_bucket() {
        assert_eq!(TIME_BUCKETS.get(0), TIME_BUCKETS.fit_time_bucket(1));
        assert_eq!(TIME_BUCKETS.get(0), TIME_BUCKETS.fit_time_bucket(60 * 60));
        assert_eq!(
            TIME_BUCKETS.get(1),
            TIME_BUCKETS.fit_time_bucket(60 * 60 + 1)
        );

        assert_eq!(
            TIME_BUCKETS.get(2),
            TIME_BUCKETS.fit_time_bucket(TIME_BUCKETS.get(2) - 1)
        );
        assert_eq!(
            TIME_BUCKETS.get(2),
            TIME_BUCKETS.fit_time_bucket(TIME_BUCKETS.get(2))
        );
        assert_eq!(
            TIME_BUCKETS.get(3),
            TIME_BUCKETS.fit_time_bucket(TIME_BUCKETS.get(3) - 1)
        );
        assert_eq!(TIME_BUCKETS.get(6), TIME_BUCKETS.fit_time_bucket(i64::MAX));
    }

    #[test]
    fn test_infer_time_buckets() {
        assert_eq!(
            TIME_BUCKETS.get(0),
            infer_time_bucket(
                [
                    new_file_handle(FileId::random(), 0, TIME_BUCKETS.get(0) * 1000 - 1, 0),
                    new_file_handle(FileId::random(), 1, 10_000, 0)
                ]
                .iter()
            )
        );
    }

    // TODO(hl): TTL tester that checks if get_expired_ssts function works as expected.
}
