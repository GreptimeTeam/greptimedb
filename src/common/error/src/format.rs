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
            write!(f, " Caused by: {source:?}")?;
        }
        if let Some(location) = self.0.location_opt() {
            // Add a newline to separate causes and backtrace.
            write!(f, " at: {location}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use snafu::prelude::*;
    use snafu::{GenerateImplicitData, Location};

    use super::*;
    use crate::ext::StackError;

    #[derive(Debug, Snafu)]
    #[snafu(display("This is a leaf error"))]
    struct Leaf;

    impl ErrorExt for Leaf {
        fn location_opt(&self) -> Option<Location> {
            None
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl StackError for Leaf {
        fn debug_fmt(&self, _: usize, _: &mut Vec<String>) {}

        fn next(&self) -> Option<&dyn StackError> {
            None
        }
    }

    #[derive(Debug, Snafu)]
    #[snafu(display("This is a leaf with location"))]
    struct LeafWithLocation {
        location: Location,
    }

    impl ErrorExt for LeafWithLocation {
        fn location_opt(&self) -> Option<Location> {
            None
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl StackError for LeafWithLocation {
        fn debug_fmt(&self, _: usize, _: &mut Vec<String>) {}

        fn next(&self) -> Option<&dyn StackError> {
            None
        }
    }

    #[derive(Debug, Snafu)]
    #[snafu(display("Internal error"))]
    struct Internal {
        #[snafu(source)]
        source: Leaf,
        location: Location,
    }

    impl ErrorExt for Internal {
        fn location_opt(&self) -> Option<Location> {
            None
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl StackError for Internal {
        fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
            buf.push(format!("{}: Internal error, at {}", layer, self.location));
            self.source.debug_fmt(layer + 1, buf);
        }

        fn next(&self) -> Option<&dyn StackError> {
            Some(&self.source)
        }
    }

    #[test]
    fn test_debug_format() {
        let err = Leaf;

        let msg = format!("{:?}", DebugFormat::new(&err));
        assert_eq!("This is a leaf error.", msg);

        let err = LeafWithLocation {
            location: Location::generate(),
        };

        // TODO(ruihang): display location here
        let msg = format!("{:?}", DebugFormat::new(&err));
        assert!(msg.starts_with("This is a leaf with location."));

        let err = Internal {
            source: Leaf,
            location: Location::generate(),
        };

        // TODO(ruihang): display location here
        let msg = format!("{:?}", DebugFormat::new(&err));
        assert!(msg.contains("Internal error. Caused by: Leaf"));
    }
}
