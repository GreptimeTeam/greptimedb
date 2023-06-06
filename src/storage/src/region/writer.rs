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

use std::sync::Arc;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::logging;
use futures::TryStreamExt;
use metrics::increment_counter;
use snafu::{ensure, ResultExt};
use store_api::logstore::LogStore;
use store_api::manifest::{Manifest, ManifestVersion, MetaAction};
use store_api::storage::{
    AlterRequest, FlushContext, FlushReason, SequenceNumber, WriteContext, WriteResponse,
};
use tokio::sync::{oneshot, Mutex};

use crate::compaction::{CompactionRequestImpl, CompactionSchedulerRef};
use crate::config::EngineConfig;
use crate::error::{self, Result};
use crate::flush::{
    FlushHandle, FlushRegionRequest, FlushSchedulerRef, FlushStrategyRef, FlushType, RegionStatus,
};
use crate::manifest::action::{
    RawRegionMetadata, RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList,
    RegionRemove,
};
use crate::memtable::{Inserter, MemtableBuilderRef, MemtableId, MemtableRef};
use crate::metadata::RegionMetadataRef;
use crate::metrics::{FLUSH_REASON, FLUSH_REQUESTS_TOTAL, PREPROCESS_ELAPSED};
use crate::proto::wal::WalHeader;
use crate::region::{
    CompactContext, RecoverdMetadata, RecoveredMetadataMap, RegionManifest, SharedDataRef,
};
use crate::schema::compat::CompatWrite;
use crate::sst::AccessLayerRef;
use crate::version::{VersionControl, VersionControlRef, VersionEdit};
use crate::wal::Wal;
use crate::write_batch::WriteBatch;

pub type RegionWriterRef = Arc<RegionWriter>;

// TODO(yingwen): Add benches for write and support group commit to improve write throughput.

/// Region writer manages all write operations to the region.
#[derive(Debug)]
pub struct RegionWriter {
    // To avoid dead lock, we need to ensure the lock order is: inner -> version_mutex.
    /// Inner writer guarded by write lock, the write lock is used to ensure
    /// all write operations are serialized.
    inner: Mutex<WriterInner>,
    /// Version lock, protects read-write-update to region `Version`.
    ///
    /// Increasing committed sequence should be guarded by this lock.
    version_mutex: Mutex<()>,
}

impl RegionWriter {
    pub fn new(
        memtable_builder: MemtableBuilderRef,
        config: Arc<EngineConfig>,
        ttl: Option<Duration>,
        compaction_time_window: Option<i64>,
        write_buffer_size: usize,
    ) -> RegionWriter {
        RegionWriter {
            inner: Mutex::new(WriterInner::new(
                memtable_builder,
                config,
                ttl,
                compaction_time_window,
                write_buffer_size,
            )),
            version_mutex: Mutex::new(()),
        }
    }

    /// Write to region in the write lock.
    pub async fn write<S: LogStore>(
        &self,
        ctx: &WriteContext,
        request: WriteBatch,
        writer_ctx: WriterContext<'_, S>,
    ) -> Result<WriteResponse> {
        let mut inner = self.inner.lock().await;

        ensure!(!inner.is_closed(), error::ClosedRegionSnafu);

        inner
            .write(&self.version_mutex, ctx, request, writer_ctx)
            .await
    }

    /// Replay data to memtables.
    pub async fn replay<S: LogStore>(
        &self,
        recovered_metadata: RecoveredMetadataMap,
        writer_ctx: WriterContext<'_, S>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner
            .replay(&self.version_mutex, recovered_metadata, writer_ctx)
            .await
    }

    /// Write and apply the region edit.
    pub(crate) async fn write_edit_and_apply<S: LogStore>(
        &self,
        wal: &Wal<S>,
        shared: &SharedDataRef,
        manifest: &RegionManifest,
        edit: RegionEdit,
        max_memtable_id: Option<MemtableId>,
    ) -> Result<()> {
        let _lock = self.version_mutex.lock().await;
        // HACK: We won't acquire the write lock here because write stall would hold
        // write lock thus we have no chance to get the lock and apply the version edit.
        // So we add a version lock to ensure modification to `VersionControl` is
        // serialized.
        let version_control = &shared.version_control;
        let prev_version = version_control.current_manifest_version();

        logging::debug!(
            "Write region edit: {:?} to manifest, prev_version: {}.",
            edit,
            prev_version,
        );

        let files_to_add = edit.files_to_add.clone();
        let files_to_remove = edit.files_to_remove.clone();
        let flushed_sequence = edit.flushed_sequence;

        // Persist the meta action.
        let mut action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
        action_list.set_prev_version(prev_version);
        let manifest_version = manifest.update(action_list).await?;

        // Notify checkpointer the flushed manifest version after flushing memtable
        if flushed_sequence.is_some() {
            manifest.set_flushed_manifest_version(manifest_version);
        }

        let version_edit = VersionEdit {
            files_to_add,
            files_to_remove,
            flushed_sequence,
            manifest_version,
            max_memtable_id,
        };

        // We could tolerate failure during persisting manifest version to the WAL, since it won't
        // affect how we applying the edit to the version.
        version_control.apply_edit(version_edit);
        // TODO(yingwen): We should set the flush handle to `None`, but we can't acquire
        // write lock here.

        // Persist the manifest version to notify subscriber of the wal that the manifest has been
        // updated. This should be done at the end of the method.
        self.persist_manifest_version(wal, version_control, manifest_version)
            .await
    }

    /// Alter schema of the region.
    pub async fn alter<S: LogStore>(
        &self,
        alter_ctx: AlterContext<'_, S>,
        request: AlterRequest,
    ) -> Result<()> {
        // To alter the schema, we need to acquire the write lock first, so we could
        // avoid other writers write to the region and switch the memtable safely.
        // Another potential benefit is that the write lock also protect against concurrent
        // alter request to the region.
        let inner = self.inner.lock().await;

        ensure!(!inner.is_closed(), error::ClosedRegionSnafu);

        let version_control = alter_ctx.version_control();

        let old_metadata = version_control.metadata();
        old_metadata
            .validate_alter(&request)
            .context(error::InvalidAlterRequestSnafu)?;

        // The write lock protects us against other alter request, so we could build the new
        // metadata struct outside of the version mutex.
        let new_metadata = old_metadata
            .alter(&request)
            .context(error::AlterMetadataSnafu)?;

        let raw = RawRegionMetadata::from(&new_metadata);

        // Acquire the version lock before altering the metadata.
        let _lock = self.version_mutex.lock().await;

        let committed_sequence = version_control.committed_sequence();
        let mut action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Change(RegionChange {
                metadata: raw,
                committed_sequence,
            }));
        let new_metadata = Arc::new(new_metadata);

        // Persist the meta action.
        let prev_version = version_control.current_manifest_version();
        action_list.set_prev_version(prev_version);

        logging::debug!(
            "Try to alter schema of region {}, region_id: {}, action_list: {:?}",
            new_metadata.name(),
            new_metadata.id(),
            action_list
        );

        let manifest_version = alter_ctx.manifest.update(action_list).await?;

        // Now we could switch memtables and apply the new metadata to the version.
        let new_mutable = inner.memtable_builder.build(new_metadata.schema().clone());
        version_control.freeze_mutable_and_apply_metadata(
            new_metadata,
            manifest_version,
            new_mutable,
        );

        self.persist_manifest_version(alter_ctx.wal, version_control, manifest_version)
            .await
    }

    /// Allocate a sequence and persist the manifest version using that sequence to the wal.
    ///
    /// This method should be protected by the `version_mutex`.
    async fn persist_manifest_version<S: LogStore>(
        &self,
        wal: &Wal<S>,
        version_control: &VersionControlRef,
        manifest_version: ManifestVersion,
    ) -> Result<()> {
        // We always bump the committed sequence regardless whether persisting the manifest version
        // to wal is success, to avoid RegionMetaAction use same committed sequence in accident.
        let next_sequence = version_control.committed_sequence() + 1;
        version_control.set_committed_sequence(next_sequence);

        let header = WalHeader::with_last_manifest_version(manifest_version);
        wal.write_to_wal(next_sequence, header, None).await?;

        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        // In order to close a writer
        // 1. Acquires the write lock.
        // 2. Sets a memory flag to reject any potential writing.
        // 3. Waits for the pending flush task.
        {
            let mut inner = self.inner.lock().await;

            if inner.is_closed() {
                return Ok(());
            }

            inner.mark_closed();
        }
        // we release the writer lock once for rejecting any following potential writing requests immediately.

        self.wait_flush().await?;

        // TODO: cancel the compaction task

        Ok(())
    }

    pub async fn on_drop<S: LogStore>(&self, drop_ctx: DropContext<'_, S>) -> Result<()> {
        // 1. Acquires the write lock.
        // 2. Close writer reject any potential writing.
        // 3. Waits or cancels the flush job.
        // 4. Add `RegionMetaAction::Remove` to recover from manifest in case of failure.
        //    The main task is to restore the cleaning of sst files. If there is a failure
        //    in the previous stops, it can be restored through the `Procedure` framework.
        // 5. Mark all data obsolete in the WAL.
        // 6. Delete the namespace of the region from the WAL.
        // 7. Mark all SSTs deleted.
        let mut inner = self.inner.lock().await;
        inner.mark_closed();

        if let Some(handle) = inner.flush_handle.take() {
            handle.wait().await?;
        }

        let version_control = drop_ctx.version_control();

        let _lock = self.version_mutex.lock().await;
        let committed_sequence = version_control.committed_sequence();
        let current_version = version_control.current();

        let mut action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Remove(RegionRemove {
                region_id: drop_ctx.shared.id,
            }));

        // Persist the meta action.
        let prev_version = version_control.current_manifest_version();
        action_list.set_prev_version(prev_version);

        logging::info!(
            "Try to remove region {}, action_list: {:?}",
            drop_ctx.shared.id(),
            action_list
        );

        drop_ctx.manifest.update(action_list).await?;

        // Mark all data obsolete and delete the namespace in the WAL
        drop_ctx.wal.obsolete(committed_sequence).await?;
        drop_ctx.wal.delete_namespace().await?;
        logging::info!(
            "Remove WAL entries in region: {}, committed sequence: {}",
            drop_ctx.shared.id(),
            committed_sequence
        );

        // Mark all SSTs deleted
        let files = current_version.ssts().mark_all_files_deleted();
        logging::info!(
            "Try to remove all SSTs, region: {}, files: {:?}",
            drop_ctx.shared.id(),
            files
        );

        Ok(())
    }

    /// Flush task manually
    pub async fn flush<S: LogStore>(
        &self,
        writer_ctx: WriterContext<'_, S>,
        ctx: &FlushContext,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;

        if !ctx.force {
            ensure!(!inner.is_closed(), error::ClosedRegionSnafu);
        }

        inner.manual_flush(writer_ctx, ctx.reason).await?;

        if ctx.wait {
            if let Some(handle) = inner.flush_handle.take() {
                handle.wait().await?;
            }
        }

        Ok(())
    }

    /// Compact manually.
    pub async fn compact<S: LogStore>(
        &self,
        writer_ctx: WriterContext<'_, S>,
        ctx: CompactContext,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;

        ensure!(!inner.is_closed(), error::ClosedRegionSnafu);
        let sst_write_buffer_size = inner.engine_config.sst_write_buffer_size;
        inner
            .manual_compact(writer_ctx, ctx, sst_write_buffer_size)
            .await
    }

    /// Wait flush task if any
    async fn wait_flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;

        if let Some(handle) = inner.flush_handle.take() {
            handle.wait().await?;
        }

        Ok(())
    }
}

pub struct WriterContext<'a, S: LogStore> {
    pub shared: &'a SharedDataRef,
    pub flush_strategy: &'a FlushStrategyRef,
    pub flush_scheduler: &'a FlushSchedulerRef<S>,
    pub compaction_scheduler: &'a CompactionSchedulerRef<S>,
    pub sst_layer: &'a AccessLayerRef,
    pub wal: &'a Wal<S>,
    pub writer: &'a RegionWriterRef,
    pub manifest: &'a RegionManifest,
}

impl<'a, S: LogStore> WriterContext<'a, S> {
    #[inline]
    fn version_control(&self) -> &VersionControlRef {
        &self.shared.version_control
    }
}

pub struct AlterContext<'a, S: LogStore> {
    pub shared: &'a SharedDataRef,
    pub wal: &'a Wal<S>,
    pub manifest: &'a RegionManifest,
}

impl<'a, S: LogStore> AlterContext<'a, S> {
    #[inline]
    fn version_control(&self) -> &VersionControlRef {
        &self.shared.version_control
    }
}

pub struct DropContext<'a, S: LogStore> {
    pub shared: &'a SharedDataRef,
    pub wal: &'a Wal<S>,
    pub manifest: &'a RegionManifest,
    pub flush_scheduler: &'a FlushSchedulerRef<S>,
    pub compaction_scheduler: &'a CompactionSchedulerRef<S>,
    pub sst_layer: &'a AccessLayerRef,
}

impl<'a, S: LogStore> DropContext<'a, S> {
    #[inline]
    fn version_control(&self) -> &VersionControlRef {
        &self.shared.version_control
    }
}

#[derive(Debug)]
struct WriterInner {
    memtable_builder: MemtableBuilderRef,
    flush_handle: Option<FlushHandle>,

    /// `WriterInner` will reject any future writing, if the closed flag is set.
    ///
    /// It should protected by upper mutex
    closed: bool,
    engine_config: Arc<EngineConfig>,
    ttl: Option<Duration>,
    compaction_time_window: Option<i64>,
    /// Size in bytes to freeze the mutable memtable.
    write_buffer_size: usize,
}

impl WriterInner {
    fn new(
        memtable_builder: MemtableBuilderRef,
        engine_config: Arc<EngineConfig>,
        ttl: Option<Duration>,
        compaction_time_window: Option<i64>,
        write_buffer_size: usize,
    ) -> WriterInner {
        WriterInner {
            memtable_builder,
            flush_handle: None,
            engine_config,
            closed: false,
            ttl,
            compaction_time_window,
            write_buffer_size,
        }
    }

    /// Write `WriteBatch` to region, now the schema of batch needs to be validated outside.
    ///
    /// Mutable reference of writer ensure no other reference of this writer can modify the
    /// version control (write is exclusive).
    async fn write<S: LogStore>(
        &mut self,
        version_mutex: &Mutex<()>,
        _ctx: &WriteContext,
        mut request: WriteBatch,
        writer_ctx: WriterContext<'_, S>,
    ) -> Result<WriteResponse> {
        self.preprocess_write(&writer_ctx).await?;
        let version_control = writer_ctx.version_control();

        let _lock = version_mutex.lock().await;

        let metadata = version_control.metadata();
        // We need to check the schema again since it might has been altered. We need
        // to compat request's schema before writing it into the WAL otherwise some
        // default constraint like `current_timestamp()` would yield different value
        // during replay.
        request.compat_write(metadata.schema().user_schema())?;

        let committed_sequence = version_control.committed_sequence();
        // Sequence for current write batch.
        let next_sequence = committed_sequence + 1;

        let version = version_control.current();
        let wal_header = WalHeader::with_last_manifest_version(version.manifest_version());
        writer_ctx
            .wal
            .write_to_wal(next_sequence, wal_header, Some(request.payload()))
            .await?;

        // Insert batch into memtable.
        let mut inserter = Inserter::new(next_sequence);
        inserter.insert_memtable(request.payload(), version.mutable_memtable())?;

        // Update committed_sequence to make current batch visible. The `&mut self` of WriterInner
        // guarantees the writer is exclusive.
        version_control.set_committed_sequence(next_sequence);

        Ok(WriteResponse {})
    }

    async fn replay<S: LogStore>(
        &mut self,
        version_mutex: &Mutex<()>,
        mut recovered_metadata: RecoveredMetadataMap,
        writer_ctx: WriterContext<'_, S>,
    ) -> Result<()> {
        let version_control = writer_ctx.version_control();

        let (flushed_sequence, mut last_sequence);
        let mut num_requests = 0;
        let mut num_recovered_metadata = 0;
        let mut next_apply_metadata = recovered_metadata.pop_first();
        {
            let _lock = version_mutex.lock().await;

            // Data after flushed sequence need to be recovered.
            flushed_sequence = version_control.current().flushed_sequence();
            last_sequence = flushed_sequence;
            // Read starts from the first entry after last flushed entry, so the start sequence
            // should be flushed_sequence + 1.
            let mut stream = writer_ctx.wal.read_from_wal(flushed_sequence + 1).await?;
            while let Some((req_sequence, _header, payload)) = stream.try_next().await? {
                while let Some((sequence_before_alter, _)) = next_apply_metadata {
                    // There might be multiple metadata changes to be applied, so a loop is necessary.
                    if req_sequence > sequence_before_alter {
                        // This is the first request that use the new metadata.
                        self.apply_metadata(
                            &writer_ctx,
                            sequence_before_alter,
                            next_apply_metadata,
                            version_control,
                        )?;

                        num_recovered_metadata += 1;
                        next_apply_metadata = recovered_metadata.pop_first();
                    } else {
                        // Keep the next_apply_metadata until req_sequence > sequence_before_alter
                        break;
                    }
                }

                if req_sequence > last_sequence {
                    last_sequence = req_sequence;
                } else {
                    logging::error!(
                            "Sequence should not decrease during replay, found {} <= {}, \
                             region_id: {}, region_name: {}, flushed_sequence: {}, num_requests: {}",
                            req_sequence,
                            last_sequence,
                            writer_ctx.shared.id,
                            writer_ctx.shared.name,
                            flushed_sequence,
                            num_requests,
                        );

                    error::SequenceNotMonotonicSnafu {
                        prev: last_sequence,
                        given: req_sequence,
                    }
                    .fail()?;
                }

                if let Some(payload) = payload {
                    num_requests += 1;
                    // Note that memtables of `Version` may be updated during replay.
                    let version = version_control.current();
                    // TODO(yingwen): Trigger flush if the size of memtables reach the flush threshold to avoid
                    // out of memory during replay, but we need to do it carefully to avoid dead lock.
                    let mut inserter = Inserter::new(last_sequence);
                    inserter.insert_memtable(&payload, version.mutable_memtable())?;
                }
            }

            // Apply metadata after last WAL entry
            while let Some((sequence_before_alter, _)) = next_apply_metadata {
                assert!(
                    sequence_before_alter >= last_sequence,
                    "The sequence in metadata after last WAL entry is less than last sequence, \
                         metadata sequence: {}, last_sequence: {}, region_id: {}, region_name: {}",
                    sequence_before_alter,
                    last_sequence,
                    writer_ctx.shared.id,
                    writer_ctx.shared.name
                );

                self.apply_metadata(
                    &writer_ctx,
                    sequence_before_alter,
                    next_apply_metadata,
                    version_control,
                )?;

                num_recovered_metadata += 1;
                next_apply_metadata = recovered_metadata.pop_first();
            }

            version_control.set_committed_sequence(last_sequence);
        }

        logging::info!(
            "Region replay finished, region_id: {}, region_name: {}, flushed_sequence: {}, last_sequence: {}, num_requests: {}, num_recovered_metadata: {}",
            writer_ctx.shared.id,
            writer_ctx.shared.name,
            flushed_sequence,
            last_sequence,
            num_requests,
            num_recovered_metadata,
        );

        Ok(())
    }

    fn apply_metadata<S: LogStore>(
        &self,
        writer_ctx: &WriterContext<'_, S>,
        sequence: SequenceNumber,
        mut metadata: Option<RecoverdMetadata>,
        version_control: &VersionControl,
    ) -> Result<()> {
        // It's safe to unwrap here, it's checked outside.
        // Move out metadata to avoid cloning it.

        let (_, (manifest_version, metadata)) = metadata.take().unwrap();
        let region_metadata: RegionMetadataRef =
            Arc::new(metadata.try_into().context(error::InvalidRawRegionSnafu {
                region: &writer_ctx.shared.name,
            })?);
        let new_mutable = self
            .memtable_builder
            .build(region_metadata.schema().clone());
        version_control.freeze_mutable_and_apply_metadata(
            region_metadata,
            manifest_version,
            new_mutable,
        );
        logging::debug!(
            "Applied metadata to region: {} when replaying WAL: sequence={} manifest={} ",
            writer_ctx.shared.name,
            sequence,
            manifest_version
        );

        Ok(())
    }

    /// Preprocess before write.
    ///
    /// Creates needed mutable memtables, ensures there is enough capacity in memtable and trigger
    /// flush if necessary. Returns time ranges of the input write batch.
    async fn preprocess_write<S: LogStore>(
        &mut self,
        writer_ctx: &WriterContext<'_, S>,
    ) -> Result<()> {
        let _timer = common_telemetry::timer!(PREPROCESS_ELAPSED);

        let version_control = writer_ctx.version_control();
        // Check whether memtable is full or flush should be triggered. We need to do this first since
        // switching memtables will clear all mutable memtables.
        if let Some(flush_type) = self.should_flush(
            writer_ctx.shared,
            version_control,
            writer_ctx.flush_strategy,
        ) {
            // Trigger flush according to the flush type.
            match flush_type {
                FlushType::Region => {
                    // Trigger flush for current region.
                    self.trigger_flush(writer_ctx, FlushReason::MemtableFull)
                        .await?;
                }
                FlushType::Engine => {
                    // Trigger engine level flush. This wakeup the flush handler
                    // to pick region to flush.
                    writer_ctx.flush_scheduler.schedule_engine_flush()?;
                }
            }
        }

        Ok(())
    }

    /// Create a new mutable memtable.
    fn alloc_memtable(&self, version_control: &VersionControlRef) -> MemtableRef {
        let memtable_schema = version_control.current().schema().clone();
        self.memtable_builder.build(memtable_schema)
    }

    fn should_flush(
        &self,
        shared: &SharedDataRef,
        version_control: &VersionControlRef,
        flush_strategy: &FlushStrategyRef,
    ) -> Option<FlushType> {
        let current = version_control.current();
        let memtables = current.memtables();
        let status = RegionStatus {
            region_id: shared.id(),
            bytes_mutable: memtables.mutable_bytes_allocated(),
            write_buffer_size: self.write_buffer_size,
        };
        flush_strategy.should_flush(status)
    }

    async fn trigger_flush<S: LogStore>(
        &mut self,
        ctx: &WriterContext<'_, S>,
        reason: FlushReason,
    ) -> Result<()> {
        let version_control = &ctx.shared.version_control;
        let new_mutable = self.alloc_memtable(version_control);
        // Freeze all mutable memtables so we can flush them later.
        version_control.freeze_mutable(new_mutable);

        increment_counter!(FLUSH_REQUESTS_TOTAL, FLUSH_REASON => reason.as_str());

        if let Some(flush_handle) = self.flush_handle.take() {
            // Previous flush job is incomplete, wait util it is finished.
            // However the last flush job may fail, in which case, we just return error
            // and abort current write request. The flush handle is left empty, so the next
            // time we still have chance to trigger a new flush.
            // TODO(yingwen): We should release the write lock during waiting flush done, which
            // needs something like async condvar.
            flush_handle.wait().await.map_err(|e| {
                logging::error!(e; "Previous flush job failed, region: {}", ctx.shared.name);
                e
            })?;
        }

        let current_version = version_control.current();
        let (max_memtable_id, mem_to_flush) = current_version.memtables().memtables_to_flush();

        if max_memtable_id.is_none() {
            // We still update the flush time to avoid the picker picks this region again.
            ctx.shared.update_flush_millis();

            logging::info!("No memtables to flush in region: {}", ctx.shared.name);
            return Ok(());
        }

        let flush_req = FlushRegionRequest {
            max_memtable_id: max_memtable_id.unwrap(),
            memtables: mem_to_flush,
            // In write thread, safe to use current committed sequence.
            flush_sequence: version_control.committed_sequence(),
            shared: ctx.shared.clone(),
            sst_layer: ctx.sst_layer.clone(),
            writer: ctx.writer.clone(),
            wal: ctx.wal.clone(),
            manifest: ctx.manifest.clone(),
            engine_config: self.engine_config.clone(),
            ttl: self.ttl,
            compaction_time_window: self.compaction_time_window,
        };

        let flush_handle = ctx
            .flush_scheduler
            .schedule_region_flush(flush_req)
            .map_err(|e| {
                logging::error!(e; "Failed to schedule flush request");
                e
            })?;
        self.flush_handle = Some(flush_handle);

        Ok(())
    }

    async fn manual_compact<S: LogStore>(
        &mut self,
        writer_ctx: WriterContext<'_, S>,
        compact_ctx: CompactContext,
        sst_write_buffer_size: ReadableSize,
    ) -> Result<()> {
        let region_id = writer_ctx.shared.id();
        let mut compaction_request = CompactionRequestImpl {
            region_id,
            sst_layer: writer_ctx.sst_layer.clone(),
            writer: writer_ctx.writer.clone(),
            shared: writer_ctx.shared.clone(),
            manifest: writer_ctx.manifest.clone(),
            wal: writer_ctx.wal.clone(),
            ttl: self.ttl,
            compaction_time_window: self.compaction_time_window,
            sender: None,
            sst_write_buffer_size,
        };

        let compaction_scheduler = writer_ctx.compaction_scheduler.clone();
        let shared_data = writer_ctx.shared.clone();

        logging::info!(
            "Manual compact, region_id: {}, compact_ctx: {:?}",
            region_id,
            compact_ctx
        );

        if compact_ctx.wait {
            let (sender, receiver) = oneshot::channel();
            compaction_request.sender = Some(sender);

            if schedule_compaction(
                shared_data,
                compaction_scheduler,
                compaction_request,
                compact_ctx.max_files_in_l0,
            ) {
                receiver
                    .await
                    .context(error::CompactTaskCancelSnafu { region_id })??;
            }
        } else {
            schedule_compaction(
                shared_data,
                compaction_scheduler,
                compaction_request,
                compact_ctx.max_files_in_l0,
            );
        }

        Ok(())
    }

    async fn manual_flush<S: LogStore>(
        &mut self,
        writer_ctx: WriterContext<'_, S>,
        reason: FlushReason,
    ) -> Result<()> {
        self.trigger_flush(&writer_ctx, reason).await?;
        Ok(())
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.closed
    }

    #[inline]
    fn mark_closed(&mut self) {
        self.closed = true;
    }
}

/// Schedule compaction task, returns whether the task is scheduled.
pub(crate) fn schedule_compaction<S: LogStore>(
    shared_data: SharedDataRef,
    compaction_scheduler: CompactionSchedulerRef<S>,
    compaction_request: CompactionRequestImpl<S>,
    max_files_in_l0: usize,
) -> bool {
    let region_id = shared_data.id();
    let level0_file_num = shared_data
        .version_control
        .current()
        .ssts()
        .level(0)
        .file_num();

    if level0_file_num <= max_files_in_l0 {
        logging::debug!(
            "No enough SST files in level 0 (threshold: {}), skip compaction",
            max_files_in_l0
        );
        return false;
    }
    match compaction_scheduler.schedule(compaction_request) {
        Ok(scheduled) => {
            logging::info!(
                "Schedule region {} compaction request result: {}",
                region_id,
                scheduled
            );

            scheduled
        }
        Err(e) => {
            logging::error!(e;"Failed to schedule region compaction request {}", region_id);

            false
        }
    }
}
