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

/// A helper macro to define a opaque boxed error based on errors that implement [ErrorExt] trait.
#[macro_export]
macro_rules! define_opaque_error {
    ($Error:ident) => {
        /// An error behaves like `Box<dyn Error>`.
        ///
        /// Define this error as a new type instead of using `Box<dyn Error>` directly so we can implement
        /// more methods or traits for it.
        pub struct $Error {
            inner: Box<dyn $crate::ext::ErrorExt + Send + Sync>,
        }

        impl $Error {
            pub fn new<E: $crate::ext::ErrorExt + Send + Sync + 'static>(err: E) -> Self {
                Self {
                    inner: Box::new(err),
                }
            }
        }

        impl std::fmt::Debug for $Error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // Use the pretty debug format of inner error for opaque error.
                let debug_format = $crate::format::DebugFormat::new(&*self.inner);
                debug_format.fmt(f)
            }
        }

        impl std::fmt::Display for $Error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.inner)
            }
        }

        impl std::error::Error for $Error {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.inner.source()
            }
        }

        impl $crate::ext::ErrorExt for $Error {
            fn status_code(&self) -> $crate::status_code::StatusCode {
                self.inner.status_code()
            }

            fn backtrace_opt(&self) -> Option<&$crate::snafu::Backtrace> {
                self.inner.backtrace_opt()
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self.inner.as_any()
            }
        }

        // Implement ErrorCompat for this opaque error so the backtrace is also available
        // via `ErrorCompat::backtrace()`.
        impl $crate::snafu::ErrorCompat for $Error {
            fn backtrace(&self) -> Option<&$crate::snafu::Backtrace> {
                self.inner.backtrace_opt()
            }
        }
    };
}

// Define a general boxed error.
define_opaque_error!(BoxedError);

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

        let msg = format!("{:?}", err);
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

        let msg = format!("{:?}", err);
        assert!(msg.contains("\nBacktrace:\n"));
        assert!(msg.contains("Caused by"));

        assert!(ErrorCompat::backtrace(&err).is_some());
    }
}
