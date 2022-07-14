use std::fmt;

/// Common status code for public API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    // ====== Begin of common status code ==============
    /// Unknown error.
    Unknown,
    /// Unsupported operation.
    Unsupported,
    /// Unexpected error, maybe there is a BUG.
    Unexpected,
    /// Internal server error.
    Internal,
    /// Invalid arguments.
    InvalidArguments,
    // ====== End of common status code ================

    // ====== Begin of SQL related status code =========
    /// SQL Syntax error.
    InvalidSyntax,
    // ====== End of SQL related status code ===========

    // ====== Begin of query related status code =======
    /// Fail to create a plan for the query.
    PlanQuery,
    /// The query engine fail to execute query.
    EngineExecuteQuery,
    // ====== End of query related status code =========

    // ====== Begin of catalog related status code =====
    /// Table already exists.
    TableAlreadyExists,
    TableNotFound,
    TableColumnNotFound,
    // ====== End of catalog related status code =======
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The current debug format is suitable to display.
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_status_code_display(code: StatusCode, msg: &str) {
        let code_msg = format!("{}", code);
        assert_eq!(msg, code_msg);
    }

    #[test]
    fn test_display_status_code() {
        assert_status_code_display(StatusCode::Unknown, "Unknown");
        assert_status_code_display(StatusCode::TableAlreadyExists, "TableAlreadyExists");
    }
}
