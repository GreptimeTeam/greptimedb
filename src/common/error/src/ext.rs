// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use crate::status_code::StatusCode;

/// Extension to [`Error`](std::error::Error) in std.
pub trait ErrorExt: std::error::Error {
    /// Map this error to [StatusCode].
    fn status_code(&self) -> StatusCode {
        StatusCode::Unknown
    }

    /// Get the reference to the backtrace of this error, None if the backtrace is unavailable.
    // Add `_opt` suffix to avoid confusing with similar method in `std::error::Error`, once backtrace
    // in std is stable, we can deprecate this method.
    fn backtrace_opt(&self) -> Option<&crate::snafu::Backtrace>;

    /// Returns the error as [Any](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// An opaque boxed error based on errors that implement [ErrorExt] trait.
pub struct BoxedError {
    inner: Box<dyn crate::ext::ErrorExt + Send + Sync>,
}

impl BoxedError {
    pub fn new<E: crate::ext::ErrorExt + Send + Sync + 'static>(err: E) -> Self {
        Self {
            inner: Box::new(err),
        }
    }
}

impl std::fmt::Debug for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use the pretty debug format of inner error for opaque error.
        let debug_format = crate::format::DebugFormat::new(&*self.inner);
        debug_format.fmt(f)
    }
}

impl std::fmt::Display for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for BoxedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

impl crate::ext::ErrorExt for BoxedError {
    fn status_code(&self) -> crate::status_code::StatusCode {
        self.inner.status_code()
    }

    fn backtrace_opt(&self) -> Option<&crate::snafu::Backtrace> {
        self.inner.backtrace_opt()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }
}

// Implement ErrorCompat for this opaque error so the backtrace is also available
// via `ErrorCompat::backtrace()`.
impl crate::snafu::ErrorCompat for BoxedError {
    fn backtrace(&self) -> Option<&crate::snafu::Backtrace> {
        self.inner.backtrace_opt()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use snafu::ErrorCompat;

    use super::*;
    use crate::format::DebugFormat;
    use crate::mock::MockError;

    #[test]
    fn test_opaque_error_without_backtrace() {
        let err = BoxedError::new(MockError::new(StatusCode::Internal));
        assert!(err.backtrace_opt().is_none());
        assert_eq!(StatusCode::Internal, err.status_code());
        assert!(err.as_any().downcast_ref::<MockError>().is_some());
        assert!(err.source().is_none());

        assert!(ErrorCompat::backtrace(&err).is_none());
    }

    #[test]
    fn test_opaque_error_with_backtrace() {
        let err = BoxedError::new(MockError::with_backtrace(StatusCode::Internal));
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Internal, err.status_code());
        assert!(err.as_any().downcast_ref::<MockError>().is_some());
        assert!(err.source().is_none());

        assert!(ErrorCompat::backtrace(&err).is_some());

        let msg = format!("{err:?}");
        assert!(msg.contains("\nBacktrace:\n"));
        let fmt_msg = format!("{:?}", DebugFormat::new(&err));
        assert_eq!(msg, fmt_msg);

        let msg = err.to_string();
        msg.contains("Internal");
    }

    #[test]
    fn test_opaque_error_with_source() {
        let leaf_err = MockError::with_backtrace(StatusCode::Internal);
        let internal_err = MockError::with_source(leaf_err);
        let err = BoxedError::new(internal_err);

        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Internal, err.status_code());
        assert!(err.as_any().downcast_ref::<MockError>().is_some());
        assert!(err.source().is_some());

        let msg = format!("{err:?}");
        assert!(msg.contains("\nBacktrace:\n"));
        assert!(msg.contains("Caused by"));

        assert!(ErrorCompat::backtrace(&err).is_some());
    }
}
