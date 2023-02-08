use sqlparser::ast::ObjectName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTable {
    table_name: ObjectName,
    file_name: String,
}

impl CopyTable {
    pub(crate) fn new(table_name: ObjectName, file_name: String) -> Self {
        Self {
            table_name,
            file_name,
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }
}
