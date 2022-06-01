use crate::metadata::{ColumnMetadata, ColumnsRowKeyMetadataRef};

pub struct MemTableSchema {
    columns_row_key: ColumnsRowKeyMetadataRef,
}

impl MemTableSchema {
    pub fn new(columns_row_key: ColumnsRowKeyMetadataRef) -> MemTableSchema {
        MemTableSchema { columns_row_key }
    }

    #[inline]
    pub fn row_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns_row_key.iter_row_key_columns()
    }

    #[inline]
    pub fn num_row_key_columns(&self) -> usize {
        self.columns_row_key.num_row_key_columns()
    }

    #[inline]
    pub fn num_value_columns(&self) -> usize {
        self.columns_row_key.num_value_columns()
    }
}
