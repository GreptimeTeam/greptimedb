//! Utils for mock.

use std::fmt;

use snafu::GenerateImplicitData;

use crate::prelude::*;

/// A mock error mainly for test.
#[derive(Debug)]
pub struct MockError {
    pub code: StatusCode,
    backtrace: Option<Backtrace>,
}

impl MockError {
    /// Create a new [MockError] without backtrace.
    pub fn new(code: StatusCode) -> MockError {
        MockError {
            code,
            backtrace: None,
        }
    }

    /// Create a new [MockError] with backtrace.
    pub fn with_backtrace(code: StatusCode) -> MockError {
        MockError {
            code,
            backtrace: Some(Backtrace::generate()),
        }
    }
}

impl fmt::Display for MockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)
    }
}

impl std::error::Error for MockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl ErrorExt for MockError {
    fn status_code(&self) -> StatusCode {
        self.code
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        self.backtrace.as_ref()
    }
}

impl ErrorCompat for MockError {
    fn backtrace(&self) -> Option<&Backtrace> {
        self.backtrace_opt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_error() {
        let err = MockError::new(StatusCode::Unknown);
        assert!(err.backtrace_opt().is_none());

        let err = MockError::with_backtrace(StatusCode::Unknown);
        assert!(err.backtrace_opt().is_some());
    }
}
