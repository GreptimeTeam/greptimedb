use crate::ast::{ColumnDef, ObjectName, SqlOption, TableConstraint};

/// Time index name, used in table constraints.
pub const TIME_INDEX: &str = "__time_index";

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTable {
    /// Create if not exists
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: Vec<SqlOption>,
}
