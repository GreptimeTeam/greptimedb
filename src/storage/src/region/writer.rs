use std::sync::Arc;

use common_telemetry::logging;
use common_time::RangeMillis;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::manifest::{Manifest, ManifestVersion, MetaAction};
use store_api::storage::{AlterRequest, WriteContext, WriteRequest, WriteResponse};
use tokio::sync::Mutex;

use crate::background::JobHandle;
use crate::error::{self, Result};
use crate::flush::{FlushJob, FlushSchedulerRef, FlushStrategyRef};
use crate::manifest::action::{
    RawRegionMetadata, RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList,
};
use crate::memtable::{Inserter, MemtableBuilderRef, MemtableId, MemtableSet};
use crate::proto::wal::WalHeader;
use crate::region::{RecoveredMetadataMap, RegionManifest, SharedDataRef};
use crate::schema::compat::CompatWrite;
use crate::sst::AccessLayerRef;
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
        let _inner = self.inner.lock().await;

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

        let mut action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Change(RegionChange {
                metadata: raw,
                committed_sequence: version_control.committed_sequence(),
            }));
        let new_metadata = Arc::new(new_metadata);

        // Persist the meta action.
        let prev_version = version_control.current_manifest_version();
        action_list.set_prev_version(prev_version);
        let manifest_version = alter_ctx.manifest.update(action_list).await?;

        // Now we could switch memtables and apply the new metadata to the version.
        version_control.freeze_mutable_and_apply_metadata(new_metadata, manifest_version);

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
        let next_sequence = version_control.committed_sequence() + 1;
        version_control.set_committed_sequence(next_sequence);

        let header = WalHeader::with_last_manifest_version(manifest_version);
        wal.write_to_wal(next_sequence, header, Payload::None)
            .await?;

        version_control.set_committed_sequence(next_sequence);

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
    last_memtable_id: MemtableId,
    flush_handle: Option<JobHandle>,
}

impl WriterInner {
    fn new(memtable_builder: MemtableBuilderRef) -> WriterInner {
        WriterInner {
            memtable_builder,
            last_memtable_id: 0,
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
        let time_ranges = self.preprocess_write(&request, &writer_ctx).await?;
        let version_control = writer_ctx.version_control();

        let _lock = version_mutex.lock().await;

        let metadata = version_control.metadata();
        // We need to check the schema again since it might has been altered.
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
        let mut inserter = Inserter::new(next_sequence, time_ranges, version.bucket_duration());
        inserter.insert_memtables(&request, version.mutable_memtables())?;

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
                while let Some((next_apply_sequence, _)) = next_apply_metadata {
                    if req_sequence >= next_apply_sequence {
                        // It's safe to unwrap here. It's checked above.
                        // Move out metadata to avoid cloning it.
                        let (_, (manifest_version, metadata)) = next_apply_metadata.take().unwrap();
                        version_control.freeze_mutable_and_apply_metadata(
                            Arc::new(metadata.try_into().context(
                                error::InvalidRawRegionSnafu {
                                    region: &writer_ctx.shared.name,
                                },
                            )?),
                            manifest_version,
                        );
                        num_recovered_metadata += 1;
                        logging::debug!("Applied metadata to region: {} when replaying WAL: sequence={} manifest={} ",
                                        writer_ctx.shared.name,
                                        next_apply_sequence,
                                        manifest_version);
                        next_apply_metadata = recovered_metadata.pop_first();
                    } else {
                        // Keep the next_apply_metadata until req_sequence >= next_apply_sequence
                        break;
                    }
                }

                if let Some(request) = request {
                    num_requests += 1;
                    let time_ranges = self.prepare_memtables(&request, version_control)?;
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
                    let mut inserter =
                        Inserter::new(last_sequence, time_ranges, version.bucket_duration());
                    inserter.insert_memtables(&request, version.mutable_memtables())?;
                }
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

    /// Preprocess before write.
    ///
    /// Creates needed mutable memtables, ensures there is enough capacity in memtable and trigger
    /// flush if necessary. Returns time ranges of the input write batch.
    async fn preprocess_write<S: LogStore>(
        &mut self,
        request: &WriteBatch,
        writer_ctx: &WriterContext<'_, S>,
    ) -> Result<Vec<RangeMillis>> {
        let version_control = writer_ctx.version_control();
        // Check whether memtable is full or flush should be triggered. We need to do this first since
        // switching memtables will clear all mutable memtables.
        if self.should_flush(
            writer_ctx.shared,
            version_control,
            writer_ctx.flush_strategy,
        ) {
            self.trigger_flush(
                writer_ctx.shared,
                writer_ctx.flush_scheduler,
                writer_ctx.sst_layer,
                writer_ctx.writer,
                writer_ctx.wal,
                writer_ctx.manifest,
            )
            .await?;
        }

        self.prepare_memtables(request, version_control)
    }

    /// Create all needed mutable memtables, returns time ranges that overlapped with `request`.
    fn prepare_memtables(
        &mut self,
        request: &WriteBatch,
        version_control: &VersionControlRef,
    ) -> Result<Vec<RangeMillis>> {
        let current_version = version_control.current();
        let bucket_duration = current_version.bucket_duration();
        let time_ranges = request
            .time_ranges(bucket_duration)
            .context(error::InvalidTimestampSnafu)?;
        let mutable = current_version.mutable_memtables();
        let mut memtables_to_add = MemtableSet::default();

        // Pre-create all needed mutable memtables.
        for range in &time_ranges {
            if mutable.get_by_range(range).is_none()
                && memtables_to_add.get_by_range(range).is_none()
            {
                // Memtable for this range is missing, need to create a new memtable.
                let memtable_schema = current_version.schema().clone();
                let id = self.alloc_memtable_id();
                let memtable = self.memtable_builder.build(id, memtable_schema);
                memtables_to_add.insert(*range, memtable);
            }
        }

        if !memtables_to_add.is_empty() {
            version_control.add_mutable(memtables_to_add);
        }

        Ok(time_ranges)
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

    async fn trigger_flush<S: LogStore>(
        &mut self,
        shared: &SharedDataRef,
        flush_scheduler: &FlushSchedulerRef,
        sst_layer: &AccessLayerRef,
        writer: &RegionWriterRef,
        wal: &Wal<S>,
        manifest: &RegionManifest,
    ) -> Result<()> {
        let version_control = &shared.version_control;
        // Freeze all mutable memtables so we can flush them later.
        version_control.freeze_mutable();

        if let Some(flush_handle) = self.flush_handle.take() {
            // Previous flush job is incomplete, wait util it is finished (write stall).
            // However the last flush job may fail, in which case, we just return error
            // and abort current write request. The flush handle is left empty, so the next
            // time we still have chance to trigger a new flush.
            logging::info!("Write stall, region: {}", shared.name);

            // TODO(yingwen): We should release the write lock during waiting flush done, which
            // needs something like async condvar.
            flush_handle.join().await.map_err(|e| {
                logging::error!(e; "Previous flush job failed, region: {}", shared.name);
                e
            })?;
        }

        let current_version = version_control.current();
        let (max_memtable_id, mem_to_flush) = current_version.memtables().memtables_to_flush();

        if max_memtable_id.is_none() {
            logging::info!("No memtables to flush in region: {}", shared.name);
            return Ok(());
        }

        let flush_req = FlushJob {
            max_memtable_id: max_memtable_id.unwrap(),
            memtables: mem_to_flush,
            // In write thread, safe to use current commited sequence.
            flush_sequence: version_control.committed_sequence(),
            shared: shared.clone(),
            sst_layer: sst_layer.clone(),
            writer: writer.clone(),
            wal: wal.clone(),
            manifest: manifest.clone(),
        };

        let flush_handle = flush_scheduler.schedule_flush(Box::new(flush_req)).await?;
        self.flush_handle = Some(flush_handle);

        Ok(())
    }

    #[inline]
    fn alloc_memtable_id(&mut self) -> MemtableId {
        self.last_memtable_id += 1;
        self.last_memtable_id
    }
}
