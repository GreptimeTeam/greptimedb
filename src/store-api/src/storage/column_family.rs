/// A group of value columns.
pub trait ColumnFamily: Send + Sync + Clone {
    fn name(&self) -> &str;
}
