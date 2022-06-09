use crate::metadata::{ColumnMetadata, ColumnsRowKeyMetadataRef};

pub struct MemtableSchema {
    columns_row_key: ColumnsRowKeyMetadataRef,
}

impl MemtableSchema {
    pub fn new(columns_row_key: ColumnsRowKeyMetadataRef) -> MemtableSchema {
        MemtableSchema { columns_row_key }
    }

    #[inline]
    pub fn row_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns_row_key.iter_row_key_columns()
    }

    #[inline]
    pub fn value_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns_row_key.iter_value_columns()
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
