use std::sync::Arc;

use common_telemetry::logging;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::manifest::{Manifest, ManifestVersion, MetaAction};
use store_api::storage::SequenceNumber;
use store_api::storage::{AlterRequest, WriteContext, WriteResponse};
use tokio::sync::Mutex;

use crate::background::JobHandle;
use crate::error::{self, Result};
use crate::flush::{FlushJob, FlushSchedulerRef, FlushStrategyRef};
use crate::manifest::action::{
    RawRegionMetadata, RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList,
};
use crate::memtable::{Inserter, MemtableBuilderRef, MemtableId, MemtableRef};
use crate::metadata::RegionMetadataRef;
use crate::proto::wal::WalHeader;
use crate::region::{RecoverdMetadata, RecoveredMetadataMap, RegionManifest, SharedDataRef};
use crate::schema::compat::CompatWrite;
use crate::sst::AccessLayerRef;
use crate::version::VersionControl;
use crate::version::{VersionControlRef, VersionEdit};
use crate::wal::{Payload, Wal};
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
    pub fn new(memtable_builder: MemtableBuilderRef) -> RegionWriter {
        RegionWriter {
            inner: Mutex::new(WriterInner::new(memtable_builder)),
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
        max_memtable_id: MemtableId,
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
        let flushed_sequence = edit.flushed_sequence;

        // Persist the meta action.
        let mut action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
        action_list.set_prev_version(prev_version);
        let manifest_version = manifest.update(action_list).await?;

        let version_edit = VersionEdit {
            files_to_add,
            flushed_sequence: Some(flushed_sequence),
            manifest_version,
            max_memtable_id: Some(max_memtable_id),
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
        wal.write_to_wal(next_sequence, header, Payload::None)
            .await?;

        Ok(())
    }
}

// Private methods for tests.
#[cfg(test)]
impl RegionWriter {
    pub async fn wait_flush_done(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if let Some(handle) = inner.flush_handle.take() {
            handle.join().await?;
        }

        Ok(())
    }
}

pub struct WriterContext<'a, S: LogStore> {
    pub shared: &'a SharedDataRef,
    pub flush_strategy: &'a FlushStrategyRef,
    pub flush_scheduler: &'a FlushSchedulerRef,
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

#[derive(Debug)]
struct WriterInner {
    memtable_builder: MemtableBuilderRef,
    flush_handle: Option<JobHandle>,
}

impl WriterInner {
    fn new(memtable_builder: MemtableBuilderRef) -> WriterInner {
        WriterInner {
            memtable_builder,
            flush_handle: None,
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
            .write_to_wal(
                next_sequence,
                wal_header,
                Payload::WriteBatchArrow(&request),
            )
            .await?;

        // Insert batch into memtable.
        let mut inserter = Inserter::new(next_sequence);
        inserter.insert_memtable(&request, version.mutable_memtable())?;

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
            while let Some((req_sequence, _header, request)) = stream.try_next().await? {
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

                if let Some(request) = request {
                    num_requests += 1;
                    // Note that memtables of `Version` may be updated during replay.
                    let version = version_control.current();

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
                    // TODO(yingwen): Trigger flush if the size of memtables reach the flush threshold to avoid
                    // out of memory during replay, but we need to do it carefully to avoid dead lock.
                    let mut inserter = Inserter::new(last_sequence);
                    inserter.insert_memtable(&request, version.mutable_memtable())?;
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
        let version_control = writer_ctx.version_control();
        // Check whether memtable is full or flush should be triggered. We need to do this first since
        // switching memtables will clear all mutable memtables.
        if self.should_flush(
            writer_ctx.shared,
            version_control,
            writer_ctx.flush_strategy,
        ) {
            self.trigger_flush(writer_ctx).await?;
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
    ) -> bool {
        let current = version_control.current();
        let memtables = current.memtables();
        let mutable_bytes_allocated = memtables.mutable_bytes_allocated();
        let total_bytes_allocated = memtables.total_bytes_allocated();
        flush_strategy.should_flush(shared, mutable_bytes_allocated, total_bytes_allocated)
    }

    async fn trigger_flush<S: LogStore>(&mut self, ctx: &WriterContext<'_, S>) -> Result<()> {
        let version_control = &ctx.shared.version_control;
        let new_mutable = self.alloc_memtable(version_control);
        // Freeze all mutable memtables so we can flush them later.
        version_control.freeze_mutable(new_mutable);

        if let Some(flush_handle) = self.flush_handle.take() {
            // Previous flush job is incomplete, wait util it is finished (write stall).
            // However the last flush job may fail, in which case, we just return error
            // and abort current write request. The flush handle is left empty, so the next
            // time we still have chance to trigger a new flush.
            logging::info!("Write stall, region: {}", ctx.shared.name);

            // TODO(yingwen): We should release the write lock during waiting flush done, which
            // needs something like async condvar.
            flush_handle.join().await.map_err(|e| {
                logging::error!(e; "Previous flush job failed, region: {}", ctx.shared.name);
                e
            })?;
        }

        let current_version = version_control.current();
        let (max_memtable_id, mem_to_flush) = current_version.memtables().memtables_to_flush();

        if max_memtable_id.is_none() {
            logging::info!("No memtables to flush in region: {}", ctx.shared.name);
            return Ok(());
        }

        let flush_req = FlushJob {
            max_memtable_id: max_memtable_id.unwrap(),
            memtables: mem_to_flush,
            // In write thread, safe to use current commited sequence.
            flush_sequence: version_control.committed_sequence(),
            shared: ctx.shared.clone(),
            sst_layer: ctx.sst_layer.clone(),
            writer: ctx.writer.clone(),
            wal: ctx.wal.clone(),
            manifest: ctx.manifest.clone(),
        };

        let flush_handle = ctx
            .flush_scheduler
            .schedule_flush(Box::new(flush_req))
            .await?;
        self.flush_handle = Some(flush_handle);

        Ok(())
    }
}
