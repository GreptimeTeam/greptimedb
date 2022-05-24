use std::sync::Arc;

use store_api::storage::ColumnFamily;

// TODO(yingwen): Wrap an arc inner.
/// Handle to column family.
#[derive(Clone)]
pub struct ColumnFamilyHandle {
    inner: Arc<ColumnFamilyInner>,
}

impl ColumnFamily for ColumnFamilyHandle {
    fn name(&self) -> &str {
        &self.inner.name
    }
}

struct ColumnFamilyInner {
    name: String,
}
