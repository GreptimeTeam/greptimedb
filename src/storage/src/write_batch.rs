use store_api::storage::WriteRequest;

use crate::column_family::ColumnFamilyHandle;

pub struct WriteBatch {}

impl WriteRequest for WriteBatch {
    type ColumnFamily = ColumnFamilyHandle;
}
