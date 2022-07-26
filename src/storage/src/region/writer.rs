use std::sync::Arc;

use common_telemetry::logging;
use common_time::RangeMillis;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::storage::{SequenceNumber, WriteContext, WriteRequest, WriteResponse};
use tokio::sync::Mutex;

use crate::background::JobHandle;
use crate::error::{InvalidTimestampSnafu, Result};
use crate::flush::{FlushJob, FlushSchedulerRef, FlushStrategyRef};
use crate::memtable::{Inserter, MemtableBuilderRef, MemtableId, MemtableSet};
use crate::proto::WalHeader;
use crate::region::RegionManifest;
use crate::region::SharedDataRef;
use crate::sst::AccessLayerRef;
use crate::version::{VersionControlRef, VersionEdit};
use crate::wal::{Payload, Wal};
use crate::write_batch::WriteBatch;

pub type RegionWriterRef = Arc<RegionWriter>;

// TODO(yingwen): Add benches for write and support group commit to improve write throughput.

/// Region writer manages all write operations to the region.
pub struct RegionWriter {
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

    /// Apply version edit.
    pub async fn apply_version_edit<S: LogStore>(
        &self,
        wal: &Wal<S>,
        edit: VersionEdit,
        shared: &SharedDataRef,
    ) -> Result<()> {
        // HACK: We won't acquire the write lock here because write stall would hold
        // write lock thus we have no chance to get the lock and apply the version edit.
        // So we add a version lock to ensure modification to `VersionControl` is
        // serialized.
        let version_control = &shared.version_control;

        let _lock = self.version_mutex.lock().await;
        let next_sequence = version_control.committed_sequence() + 1;

        self.persist_manifest_version(wal, next_sequence, &edit)
            .await?;

        version_control.apply_edit(edit);

        version_control.set_committed_sequence(next_sequence);

        Ok(())
    }

    async fn persist_manifest_version<S: LogStore>(
        &self,
        wal: &Wal<S>,
        seq: SequenceNumber,
        edit: &VersionEdit,
    ) -> Result<()> {
        let header = WalHeader::with_last_manifest_version(edit.manifest_version);

        wal.write_to_wal(seq, header, Payload::None).await?;

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
        request: WriteBatch,
        writer_ctx: WriterContext<'_, S>,
    ) -> Result<WriteResponse> {
        let time_ranges = self.preprocess_write(&request, &writer_ctx).await?;

        // TODO(yingwen): Write wal and get sequence.
        let version_control = writer_ctx.version_control();
        let version = version_control.current();

        let _lock = version_mutex.lock().await;
        let committed_sequence = version_control.committed_sequence();
        // Sequence for current write batch.
        let next_sequence = committed_sequence + 1;

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

        let current_version = version_control.current();
        let duration = current_version.bucket_duration();
        let time_ranges = request
            .time_ranges(duration)
            .context(InvalidTimestampSnafu)?;
        let mutable = current_version.mutable_memtables();
        let mut memtables_to_add = MemtableSet::default();

        // Pre-create all needed mutable memtables.
        for range in &time_ranges {
            if mutable.get_by_range(range).is_none()
                && memtables_to_add.get_by_range(range).is_none()
            {
                // Memtable for this range is missing, need to create a new memtable.
                let memtable_schema = current_version.memtable_schema();
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
                logging::error!(
                    "Previous flush job failed, region: {}, err: {}",
                    shared.name,
                    e
                );
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
