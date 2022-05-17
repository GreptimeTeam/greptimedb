use std::fmt;

use crate::ext::ErrorExt;

/// Pretty debug format for error, also prints source and backtrace.
pub struct DebugFormat<'a, E: ?Sized>(&'a E);

impl<'a, E: ?Sized> DebugFormat<'a, E> {
    /// Create a new format struct from `err`.
    pub fn new(err: &'a E) -> Self {
        Self(err)
    }
}

impl<'a, E: ErrorExt + ?Sized> fmt::Debug for DebugFormat<'a, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.", self.0)?;
        if let Some(source) = self.0.source() {
            // Source error use debug format for more verbose info.
            write!(f, " Caused by: {:?}", source)?;
        }
        if let Some(backtrace) = self.0.backtrace_opt() {
            // Add a newline to seperate causes and backtrace.
            write!(f, "\nBacktrace:\n{}", backtrace)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use snafu::{prelude::*, Backtrace, GenerateImplicitData};

    use super::*;

    #[derive(Debug, Snafu)]
    #[snafu(display("This is a leaf error"))]
    struct Leaf;

    impl ErrorExt for Leaf {
        fn backtrace_opt(&self) -> Option<&Backtrace> {
            None
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug, Snafu)]
    #[snafu(display("This is a leaf with backtrace"))]
    struct LeafWithBacktrace {
        backtrace: Backtrace,
    }

    impl ErrorExt for LeafWithBacktrace {
        fn backtrace_opt(&self) -> Option<&Backtrace> {
            Some(&self.backtrace)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug, Snafu)]
    #[snafu(display("Internal error"))]
    struct Internal {
        #[snafu(source)]
        source: Leaf,
        backtrace: Backtrace,
    }

    impl ErrorExt for Internal {
        fn backtrace_opt(&self) -> Option<&Backtrace> {
            Some(&self.backtrace)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_debug_format() {
        let err = Leaf;

        let msg = format!("{:?}", DebugFormat::new(&err));
        assert_eq!("This is a leaf error.", msg);

        let err = LeafWithBacktrace {
            backtrace: Backtrace::generate(),
        };

        let msg = format!("{:?}", DebugFormat::new(&err));
        assert!(msg.starts_with("This is a leaf with backtrace.\nBacktrace:\n"));

        let err = Internal {
            source: Leaf,
            backtrace: Backtrace::generate(),
        };

        let msg = format!("{:?}", DebugFormat::new(&err));
        assert!(msg.contains("Internal error. Caused by: Leaf\nBacktrace:\n"));
    }
}
