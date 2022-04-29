/// Common status code for public API.
#[derive(Debug, Clone, Copy)]
pub enum StatusCode {
    // ====== Begin of common status code ===========
    /// Unknown error.
    Unknown,
    /// Unsupported operation.
    Unsupported,
    // ====== End of common status code =============

    // ====== Begin of SQL related status code ======
    /// SQL Syntax error.
    InvalidSyntax,
    // ====== End of SQL related status code ========
}
