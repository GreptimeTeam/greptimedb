use crate::statements::show_kind::ShowKind;

/// SQL structure for `SHOW DATABASES`.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlShowDatabase {
    pub kind: ShowKind,
}

impl SqlShowDatabase {
    /// Creates a statement for `SHOW DATABASES`
    pub fn create(kind: ShowKind) -> Self {
        SqlShowDatabase { kind }
    }
}
