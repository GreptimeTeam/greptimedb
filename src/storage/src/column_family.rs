use std::sync::Arc;

use store_api::storage::{ColumnFamily, ColumnFamilyId};

/// Handle to column family.
#[derive(Clone)]
pub struct ColumnFamilyHandle {
    data: ColumnFamilyDataRef,
}

impl ColumnFamily for ColumnFamilyHandle {
    fn name(&self) -> &str {
        &self.data.name
    }
}

type ColumnFamilyDataRef = Arc<ColumnFamilyData>;

// TODO(yingwen): cf_id and region meta.
struct ColumnFamilyData {
    name: String,
    cf_id: ColumnFamilyId,
}
