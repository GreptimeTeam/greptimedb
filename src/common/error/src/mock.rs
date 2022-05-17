//! Utils for mock.

use std::any::Any;
use std::fmt;

use snafu::GenerateImplicitData;

use crate::prelude::*;

/// A mock error mainly for test.
#[derive(Debug)]
pub struct MockError {
    pub code: StatusCode,
    backtrace: Option<Backtrace>,
    source: Option<Box<MockError>>,
}

impl MockError {
    /// Create a new [MockError] without backtrace.
    pub fn new(code: StatusCode) -> MockError {
        MockError {
            code,
            backtrace: None,
            source: None,
        }
    }

    /// Create a new [MockError] with backtrace.
    pub fn with_backtrace(code: StatusCode) -> MockError {
        MockError {
            code,
            backtrace: Some(Backtrace::generate()),
            source: None,
        }
    }

    /// Create a new [MockError] with source.
    pub fn with_source(source: MockError) -> MockError {
        MockError {
            code: source.code,
            backtrace: None,
            source: Some(Box::new(source)),
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
        self.source.as_ref().map(|e| e as _)
    }
}

impl ErrorExt for MockError {
    fn status_code(&self) -> StatusCode {
        self.code
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        self.backtrace
            .as_ref()
            .or_else(|| self.source.as_ref().and_then(|err| err.backtrace_opt()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ErrorCompat for MockError {
    fn backtrace(&self) -> Option<&Backtrace> {
        self.backtrace_opt()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn test_mock_error() {
        let err = MockError::new(StatusCode::Unknown);
        assert!(err.backtrace_opt().is_none());

        let err = MockError::with_backtrace(StatusCode::Unknown);
        assert!(err.backtrace_opt().is_some());

        let root_err = MockError::with_source(err);
        assert!(root_err.source().is_some());
        assert!(root_err.backtrace_opt().is_some());
    }
}
