use store_api::storage::WriteRequest;

use crate::test_util::schema_util::{self, ColumnDef};
use crate::write_batch::WriteBatch;

pub fn new_write_batch(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> WriteBatch {
    let schema = schema_util::new_schema_ref(column_defs, timestamp_index);

    WriteBatch::new(schema)
}
