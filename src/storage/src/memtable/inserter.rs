
use store_api::storage::SequenceNumber;
use crate::write_batch::{WriteBatch, PutData, Mutation};
use crate::error::Result;

/// MemTable inserter.
pub struct Inserter {
    /// Sequence of the batch to be inserted.
    sequence: SequenceNumber,
}

impl Inserter {
    /// Insert write batch into memtable.
    ///
    /// Won't do schema validation.
    fn insert_memtable(&self, batch: &WriteBatch, memtable: &dyn MemTable) -> Result<()> {
        if batch.is_empty() {
            return Ok(())
        }

        let mut index_in_batch = 0;
        for mutation in batch {
            match mutation {
                Mutation::Put(put_data) => self.put_impl(put_data, memtable)?,
            }
        }

        unimplemented!()
    }

    fn put_impl(&self, put_data: &PutData, memtable: &dyn MemTable) -> Result<()> {
        let schema = memtable.schema();

        let mut row_key_columns = Vec::with_capacity(schema.num_row_key_columns());
        let mut value_columns = Vec::with_capacity(schema.num_value_columns());

        //

        unimplemented!()
    }
}

// TODO(yingwen): Iter write batch and insert. Maybe implement iter for WriteBatch.
