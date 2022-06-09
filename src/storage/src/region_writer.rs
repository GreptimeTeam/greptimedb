use store_api::storage::{SequenceNumber, WriteContext, WriteResponse};

use crate::error::Result;
use crate::memtable::{Inserter, MemtableBuilderRef};
use crate::version::VersionControlRef;
use crate::write_batch::WriteBatch;

pub struct RegionWriter {
    _memtable_builder: MemtableBuilderRef,
    last_sequence: SequenceNumber,
}

impl RegionWriter {
    pub fn new(_memtable_builder: MemtableBuilderRef) -> RegionWriter {
        RegionWriter {
            _memtable_builder,
            last_sequence: 0,
        }
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
        let memtables = &version.memtables;

        let mem = memtables.mutable_memtable();
        self.last_sequence += 1;
        let mut inserter = Inserter::new(self.last_sequence);
        inserter.insert_memtable(&request, &**mem)?;

        Ok(WriteResponse {})
    }
}
