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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_telemetry::{error, info};
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use tokio::sync::mpsc;

use crate::access_layer::{AccessLayerRef, SstWriteRequest};
use crate::cache::CacheManagerRef;
use crate::compaction::picker::CompactionTask;
use crate::compaction::{build_sst_reader, CompactionOutput};
use crate::config::MitoConfig;
use crate::error;
use crate::error::CompactRegionSnafu;
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::metrics::{COMPACTION_FAILURE_COUNT, COMPACTION_STAGE_ELAPSED};
use crate::read::Source;
use crate::region::options::IndexOptions;
use crate::region::version::VersionControlRef;
use crate::region::{ManifestContextRef, RegionState};
use crate::request::{
    BackgroundNotify, CompactionFailed, CompactionFinished, OutputTx, WorkerRequest,
};
use crate::sst::file::{FileHandle, FileMeta, IndexType};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::parquet::WriteOptions;
use crate::worker::WorkerListener;

const MAX_PARALLEL_COMPACTION: usize = 8;

pub(crate) struct CompactionTaskImpl {
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

impl Debug for CompactionTaskImpl {
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

impl Drop for CompactionTaskImpl {
    fn drop(&mut self) {
        self.mark_files_compacting(false)
    }
}

impl CompactionTaskImpl {
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
                "Compaction region {} output [{}]-> {}",
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
                    output.output_time_range,
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
impl CompactionTask for CompactionTaskImpl {
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
