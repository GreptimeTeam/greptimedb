use crate::status_code::StatusCode;

/// Extension to [`Error`](std::error::Error) in std.
pub trait ErrorExt: std::error::Error {
    /// Returns the [StatusCode] of this holder.
    fn status_code(&self) -> StatusCode {
        StatusCode::Unknown
    }

    /// Get the reference to the backtrace of this error.
    // Add `_opt` suffix to avoid confusing with similar method in `std::error::Error`.
    fn backtrace_opt(&self) -> Option<&snafu::Backtrace>;
}

/// A helper macro to define a opaque boxed error based on errors that implement [ErrorExt] trait.
#[macro_export]
macro_rules! define_opaque_error {
    ($Error:ident) => {
        /// An error behaves like `Box<dyn Error>`.
        ///
        /// Define this error as a new type instead of using `Box<dyn Error>` directly so we can implement
        /// more method or trait for it.
        pub struct $Error {
            inner: Box<dyn $crate::ext::ErrorExt + Send + Sync>,
        }

        impl $Error {
            /// Create a new error.
            pub fn new<E: $crate::ext::ErrorExt + Send + Sync + 'static>(err: E) -> Self {
                Self {
                    inner: Box::new(err),
                }
            }
        }

        impl std::fmt::Debug for $Error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

            fn backtrace_opt(&self) -> Option<&snafu::Backtrace> {
                self.inner.backtrace_opt()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use snafu::{prelude::*, Backtrace};

    use super::*;

    define_opaque_error!(Error);

    #[derive(Debug, Snafu)]
    enum InnerError {
        #[snafu(display("This is a leaf error, val: {}", val))]
        Leaf { val: i32, backtrace: Backtrace },

        #[snafu(display("This is an internal error"))]
        Internal {
            source: std::io::Error,
            backtrace: Backtrace,
        },
    }

    impl ErrorExt for InnerError {
        fn backtrace_opt(&self) -> Option<&snafu::Backtrace> {
            snafu::ErrorCompat::backtrace(self)
        }
    }

    impl From<InnerError> for Error {
        fn from(err: InnerError) -> Self {
            Self::new(err)
        }
    }

    fn throw_leaf() -> std::result::Result<(), InnerError> {
        LeafSnafu { val: 10 }.fail()
    }

    fn throw_io() -> std::result::Result<(), std::io::Error> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "oh no!"))
    }

    fn throw_internal() -> std::result::Result<(), InnerError> {
        throw_io().context(InternalSnafu)
    }

    #[test]
    fn test_opaque_error() {
        let leaf = throw_leaf().err().unwrap();
        assert!(leaf.backtrace_opt().is_some());
        assert!(leaf.source().is_none());

        let internal = throw_internal().err().unwrap();
        assert!(internal.backtrace_opt().is_some());
        assert!(internal.source().is_some());
    }
}
