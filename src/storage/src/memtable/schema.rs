use crate::metadata::ColumnsRowKeyMetadataRef;

pub struct MemTableSchema {
    _columns_row_key: ColumnsRowKeyMetadataRef,
}

impl MemTableSchema {
    pub fn new(_columns_row_key: ColumnsRowKeyMetadataRef) -> MemTableSchema {
        MemTableSchema { _columns_row_key }
    }
}
