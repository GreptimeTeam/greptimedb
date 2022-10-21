use crate::ast::{ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value as SqlValue};

/// Time index name, used in table constraints.
pub const TIME_INDEX: &str = "__time_index";

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateTable {
    /// Create if not exists
    pub if_not_exists: bool,
    pub table_id: u32,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: Vec<SqlOption>,
    pub partitions: Option<Partitions>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Partitions {
    pub column_list: Vec<Ident>,
    pub entries: Vec<PartitionEntry>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PartitionEntry {
    pub name: Ident,
    pub value_list: Vec<SqlValue>,
}
