//! Columns can be grouped into column families.
//!
//! Columns in different column families are not allowed to have same name now, so user
//! can still using `table_name.column_name` to represent a column uniquely.

/// A group of value columns.
pub trait ColumnFamily: Send + Sync + Clone {
    fn name(&self) -> &str;
}
