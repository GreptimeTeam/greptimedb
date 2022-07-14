use crate::statements::show_kind::ShowKind;

/// SQL structure for `SHOW DATABASES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlShowDatabase {
    pub kind: ShowKind,
}

impl SqlShowDatabase {
    /// Creates a statement for `SHOW DATABASES`
    pub fn new(kind: ShowKind) -> Self {
        SqlShowDatabase { kind }
    }
}
