use store_api::storage::{WriteContext, WriteResponse};

use crate::error::Result;
use crate::memtable::{Inserter, MemTableBuilderRef};
use crate::version::VersionControlRef;
use crate::write_batch::WriteBatch;

pub struct RegionWriter {
    _memtable_builder: MemTableBuilderRef,
}

impl RegionWriter {
    pub fn new(_memtable_builder: MemTableBuilderRef) -> RegionWriter {
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

        let version = version_control.current();
        let memtables = &version.memtables;

        let mem = memtables.mutable_memtable();
        // FIXME(yingwen): Provide sequence number.
        let mut inserter = Inserter::new(1);
        inserter.insert_memtable(&request, &**mem)?;

        Ok(WriteResponse {})
    }
}
