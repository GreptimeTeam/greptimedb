use std::sync::Arc;

use store_api::storage::{WriteContext, WriteRequest, WriteResponse};
use tokio::sync::Mutex;

use crate::background::JobHandle;
use crate::error::Result;
use crate::flush::{FlushJob, FlushSchedulerRef, FlushStrategyRef};
use crate::memtable::{Inserter, MemtableBuilderRef, MemtableId, MemtableSet};
use crate::region::SharedDataRef;
use crate::sst::AccessLayerRef;
use crate::version::{VersionControlRef, VersionEdit};
use crate::wal::Wal;
use crate::write_batch::WriteBatch;

pub type RegionWriterRef = Arc<RegionWriter>;

pub struct RegionWriter {
    inner: Mutex<WriterInner>,
}

impl RegionWriter {
    pub fn new(memtable_builder: MemtableBuilderRef) -> RegionWriter {
        RegionWriter {
            inner: Mutex::new(WriterInner::new(memtable_builder)),
        }
    }

    pub async fn write<S>(
        &self,
        ctx: &WriteContext,
        request: WriteBatch,
        writer_ctx: WriterContext<'_, S>,
    ) -> Result<WriteResponse> {
        let mut inner = self.inner.lock().await;
        inner.write(ctx, request, writer_ctx).await
    }

    pub async fn apply_version_edit(
        &self,
        edit: VersionEdit,
        shared: &SharedDataRef,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.apply_version_edit(edit, shared).await
    }
}

pub struct WriterContext<'a, S> {
    pub shared: &'a SharedDataRef,
    pub flush_strategy: &'a FlushStrategyRef,
    pub flush_scheduler: &'a FlushSchedulerRef,
    pub sst_layer: &'a AccessLayerRef,
    pub wal: &'a Wal<S>,
    pub writer: &'a RegionWriterRef,
}

impl<'a, S> WriterContext<'a, S> {
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

    // TODO(yingwen): Support group commit so we can avoid taking mutable reference.
    /// Write `WriteBatch` to region, now the schema of batch needs to be validated outside.
    ///
    /// Mutable reference of writer ensure no other reference of this writer can modify the
    /// version control (write is exclusive).
    async fn write<S>(
        &mut self,
        _ctx: &WriteContext,
        request: WriteBatch,
        writer_ctx: WriterContext<'_, S>,
    ) -> Result<WriteResponse> {
        self.preprocess_write(&request, &writer_ctx).await?;

        // TODO(yingwen): Write wal and get sequence.
        let version_control = writer_ctx.version_control();
        let version = version_control.current();

        let committed_sequence = version_control.committed_sequence();
        // Sequence for current write batch.
        let next_sequence = committed_sequence + 1;

        // TODO(jiachun): [flush] write data to wal

        // Insert batch into memtable.
        let mut inserter = Inserter::new(next_sequence);
        inserter.insert_memtables(&request, version.mutable_memtables())?;

        // Update committed_sequence to make current batch visible. The `&mut self` of WriterInner
        // guarantees the writer is exclusive.
        version_control.set_committed_sequence(next_sequence);

        Ok(WriteResponse {})
    }

    // TODO(yingwen): [flush] Provide a method in CowCell that takes a closure which returns `Result`
    // and commit on `Ok`, rollback on `Err`.
    async fn preprocess_write<S>(
        &mut self,
        request: &WriteBatch,
        writer_ctx: &WriterContext<'_, S>,
    ) -> Result<()> {
        let version_control = writer_ctx.version_control();
        // Check whether memtable is full or flush should be triggered. We need to do this first since
        // switching memtables will clear all mutable memtables.
        if self.should_flush(version_control, writer_ctx.flush_strategy) {
            self.trigger_flush(
                writer_ctx.shared,
                writer_ctx.flush_scheduler,
                writer_ctx.sst_layer,
                writer_ctx.writer,
            )
            .await?;
        }

        let current_version = version_control.current();
        let duration = current_version.bucket_duration();
        let time_ranges = request.time_ranges(duration);
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

        Ok(())
    }

    fn should_flush(
        &self,
        version_control: &VersionControlRef,
        flush_strategy: &FlushStrategyRef,
    ) -> bool {
        let current_version = version_control.current();
        let memtable_bytes_allocated = current_version.mutable_memtables().bytes_allocated();
        flush_strategy.should_flush(memtable_bytes_allocated)
    }

    async fn trigger_flush(
        &mut self,
        shared: &SharedDataRef,
        flush_scheduler: &FlushSchedulerRef,
        sst_layer: &AccessLayerRef,
        writer: &RegionWriterRef,
    ) -> Result<()> {
        let version_control = &shared.version_control;
        if version_control.try_freeze_mutable().is_err() {
            // TODO(yingwen): [flush] Write stall, wait for last flush.
            unimplemented!()
        }

        // TODO(yingwen): [flush] Flush may fail, so we need to flush both old and new immutable memtables.
        assert!(self.flush_handle.is_none());

        let current_version = version_control.current();
        let duration = current_version.bucket_duration();
        let mem_to_flush = current_version.memtables().memtables_to_flush(duration);

        let flush_req = FlushJob {
            memtables: mem_to_flush,
            // In write thread, safe to use current commited sequence.
            flush_sequence: version_control.committed_sequence(),
            shared: shared.clone(),
            sst_layer: sst_layer.clone(),
            writer: writer.clone(),
        };

        let flush_handle = flush_scheduler.schedule_flush(flush_req).await?;
        self.flush_handle = Some(flush_handle);

        Ok(())
    }

    pub async fn apply_version_edit(
        &mut self,
        edit: VersionEdit,
        shared: &SharedDataRef,
    ) -> Result<()> {
        self.persist_version_edit_log(&edit).await?;

        shared.version_control.apply_edit(edit);

        Ok(())
    }

    pub async fn persist_version_edit_log(&self, _edit: &VersionEdit) -> Result<()> {
        // TODO(yingwen): [flush] Write meta log that points to the manifest file to log store.
        unimplemented!()
    }

    #[inline]
    fn alloc_memtable_id(&mut self) -> MemtableId {
        self.last_memtable_id += 1;
        self.last_memtable_id
    }
}
