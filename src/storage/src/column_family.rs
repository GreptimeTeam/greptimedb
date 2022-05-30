use std::fmt;

use store_api::storage::{ColumnFamily, ColumnFamilyId};

use crate::version::VersionControlRef;

/// Handle to column family.
#[derive(Clone)]
pub struct ColumnFamilyHandle {
    cf_id: ColumnFamilyId,
    version: VersionControlRef,
}

impl fmt::Debug for ColumnFamilyHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let metadata = self.version.metadata();
        if let Some(cf) = metadata.cf_by_id(self.cf_id) {
            write!(f, "ColumnFamily({}, {})", cf.name, self.cf_id)
        } else {
            write!(f, "ColumnFamily(unknown, {})", self.cf_id)
        }
    }
}

impl ColumnFamily for ColumnFamilyHandle {
    fn id(&self) -> ColumnFamilyId {
        self.cf_id
    }
}
