/// Common status code for public API.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatusCode {
    // ====== Begin of common status code ==============
    /// Unknown error.
    Unknown,
    /// Unsupported operation.
    Unsupported,
    /// Internal server error.
    Internal,
    // ====== End of common status code ================

    // ====== Begin of SQL related status code =========
    /// SQL Syntax error.
    InvalidSyntax,
    // ====== End of SQL related status code ===========

    // ====== Begin of catalog related status code =====
    /// Table already exists.
    TableAlreadyExists,
    // ====== End of catalog related status code =======
}
