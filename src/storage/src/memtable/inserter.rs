use std::sync::Arc;

use datatypes::vectors::{NullVector, VectorRef};
use snafu::ensure;
use store_api::storage::{ColumnDescriptor, SequenceNumber, ValueType};

use crate::error::{self, Result};
use crate::memtable::{KeyValues, MemTable};
use crate::write_batch::{Mutation, PutData, WriteBatch};

/// Wraps logic of inserting key/values in [WriteBatch] to [MemTable].
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
    /// Insert write batch into memtable.
    ///
    /// Won't do schema validation.
    pub fn insert_memtable(&mut self, batch: &WriteBatch, memtable: &dyn MemTable) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let schema = memtable.schema();
        // Reusable KeyValues buffer.
        let mut key_values = KeyValues {
            sequence: self.sequence,
            value_type: ValueType::Put,
            start_index_in_batch: self.index_in_batch,
            keys: Vec::with_capacity(schema.num_row_key_columns()),
            values: Vec::with_capacity(schema.num_value_columns()),
        };

        for mutation in batch {
            match mutation {
                Mutation::Put(put_data) => {
                    self.put_impl(put_data, memtable, &mut key_values)?;
                }
            }
        }

        Ok(())
    }

    fn put_impl(
        &mut self,
        put_data: &PutData,
        memtable: &dyn MemTable,
        key_values: &mut KeyValues,
    ) -> Result<()> {
        let schema = memtable.schema();
        let num_rows = put_data.num_rows();

        reset_key_values(key_values, ValueType::Put, self.index_in_batch);

        for key_col in schema.row_key_columns() {
            clone_put_data_column_to(put_data, &key_col.desc, &mut key_values.keys)?;
        }

        for value_col in schema.value_columns() {
            clone_put_data_column_to(put_data, &value_col.desc, &mut key_values.values)?;
        }

        memtable.write(key_values)?;

        self.index_in_batch += num_rows;

        Ok(())
    }
}

fn reset_key_values(key_values: &mut KeyValues, value_type: ValueType, index_in_batch: usize) {
    key_values.value_type = value_type;
    key_values.start_index_in_batch = index_in_batch;
    key_values.keys.clear();
    key_values.values.clear();
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
