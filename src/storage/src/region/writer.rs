use store_api::storage::{WriteContext, WriteResponse};

use crate::error::Result;
use crate::memtable::{Inserter, MemtableBuilderRef};
use crate::version::VersionControlRef;
use crate::write_batch::WriteBatch;

pub struct RegionWriter {
    _memtable_builder: MemtableBuilderRef,
}

impl RegionWriter {
    pub fn new(_memtable_builder: MemtableBuilderRef) -> RegionWriter {
        RegionWriter { _memtable_builder }
    }

    // TODO(yingwen): Support group commit so we can avoid taking mutable reference.
    /// Write `WriteBatch` to region, now the schema of batch needs to be validated outside.
    pub async fn write(
        &mut self,
        _ctx: &WriteContext,
        version_control: &VersionControlRef,
        request: WriteBatch,
    ) -> Result<WriteResponse> {
        // Mutable reference of writer ensure no other reference of this writer can modify
        // the version control (write is exclusive).

        // TODO(yingwen): Write wal and get sequence.
        let version = version_control.current();
        let mem = version.mutable_memtable();

        let committed_sequence = version_control.committed_sequence();
        // Sequence for current write batch.
        let next_sequence = committed_sequence + 1;

        // Insert batch into memtable.
        let mut inserter = Inserter::new(next_sequence);
        inserter.insert_memtable(&request, &**mem)?;

        // Update committed_sequence to make current batch visible. The `&mut self` of RegionWriter
        // guarantees the writer is exclusive.
        version_control.set_committed_sequence(next_sequence);

        Ok(WriteResponse {})
    }
}
