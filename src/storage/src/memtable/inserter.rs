use std::sync::Arc;

use datatypes::vectors::{NullVector, VectorRef};
use snafu::ensure;
use store_api::storage::{ColumnDescriptor, SequenceNumber, ValueType};

use crate::error::{self, Result};
use crate::memtable::{KeyValues, Memtable, MemtableSet};
use crate::write_batch::{Mutation, PutData, WriteBatch};

/// Wraps logic of inserting key/values in [WriteBatch] to [Memtable].
pub struct Inserter {
    /// Sequence of the batch to be inserted.
    sequence: SequenceNumber,
    index_in_batch: usize,
}

impl Inserter {
    pub fn new(sequence: SequenceNumber) -> Inserter {
        Inserter {
            sequence,
            index_in_batch: 0,
        }
    }

    // TODO(yingwen): Can we take the WriteBatch?
    /// Insert write batch into memtables.
    ///
    /// Won't do schema validation.
    pub fn insert_memtables(&mut self, batch: &WriteBatch, memtables: &MemtableSet) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // Enough to hold all key or value columns.
        let total_column_num = batch.schema().num_columns();
        // Reusable KeyValues buffer.
        let mut kvs = KeyValues {
            sequence: self.sequence,
            value_type: ValueType::Put,
            start_index_in_batch: self.index_in_batch,
            keys: Vec::with_capacity(total_column_num),
            values: Vec::with_capacity(total_column_num),
        };

        for mutation in batch {
            match mutation {
                Mutation::Put(put_data) => {
                    self.put_memtables(put_data, memtables, &mut kvs)?;
                }
            }
        }

        Ok(())
    }

    fn put_memtables(
        &mut self,
        _put_data: &PutData,
        _memtables: &MemtableSet,
        _kvs: &mut KeyValues,
    ) -> Result<()> {
        // TODO(yingwen): [flush] Split data by time range and put into memtables.
        unimplemented!()
    }

    #[allow(unused)]
    fn put_impl(
        &mut self,
        put_data: &PutData,
        memtable: &dyn Memtable,
        kvs: &mut KeyValues,
    ) -> Result<()> {
        let schema = memtable.schema();
        let num_rows = put_data.num_rows();

        kvs.reset(ValueType::Put, self.index_in_batch);

        for key_col in schema.row_key_columns() {
            clone_put_data_column_to(put_data, &key_col.desc, &mut kvs.keys)?;
        }

        for value_col in schema.value_columns() {
            clone_put_data_column_to(put_data, &value_col.desc, &mut kvs.values)?;
        }

        memtable.write(kvs)?;

        self.index_in_batch += num_rows;

        Ok(())
    }
}

fn clone_put_data_column_to(
    put_data: &PutData,
    desc: &ColumnDescriptor,
    target: &mut Vec<VectorRef>,
) -> Result<()> {
    if let Some(vector) = put_data.column_by_name(&desc.name) {
        target.push(vector.clone());
    } else {
        // The write batch should have been validated before.
        ensure!(
            desc.is_nullable,
            error::BatchMissingColumnSnafu { column: &desc.name }
        );

        let num_rows = put_data.num_rows();
        target.push(Arc::new(NullVector::new(num_rows)));
    }

    Ok(())
}
