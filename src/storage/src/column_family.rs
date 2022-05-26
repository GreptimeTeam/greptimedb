use std::sync::Arc;

use store_api::storage::{ColumnFamily, ColumnFamilyId};

use crate::metadata::RegionMetadataRef;

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

struct ColumnFamilyData {
    // TODO(yingwen): Maybe remove name from cf data.
    name: String,
    cf_id: ColumnFamilyId,
    // TODO(yingwen): metadata.
}
