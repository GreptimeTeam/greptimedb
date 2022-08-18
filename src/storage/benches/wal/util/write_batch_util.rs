use storage::write_batch::WriteBatch;

use crate::memtable::util::schema_util::{self, ColumnDef};

pub fn new_write_batch(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> WriteBatch {
    let schema = schema_util::new_schema_ref(column_defs, timestamp_index);

    WriteBatch::new(schema)
}
