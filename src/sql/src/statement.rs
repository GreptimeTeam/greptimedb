use crate::statements::statement_show_database::SqlShowDatabase;

/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum GtStatement {
    // Databases.
    ShowDatabases(SqlShowDatabase),
}

/// Comment hints from SQL.
/// It'll be enabled when using `--comment` in mysql client.
/// Eg: `SELECT * FROM system.number LIMIT 1; -- { ErrorCode 25 }`
#[derive(Debug, Clone, PartialEq)]
pub struct GtHint {
    pub error_code: Option<u16>,
    pub comment: String,
    pub prefix: String,
}
