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

    // TODO(ruihang): remove this default implementation
    /// Get the location of this error, None if the location is unavailable.
    /// Add `_opt` suffix to avoid confusing with similar method in `std::error::Error`
    fn location_opt(&self) -> Option<crate::snafu::Location> {
        None
    }

    /// Returns the error as [Any](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

pub trait StackError {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>);

    fn next(&self) -> Option<&dyn StackError>;
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

    fn location_opt(&self) -> Option<crate::snafu::Location> {
        self.inner.location_opt()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }
}

// Implement ErrorCompat for this opaque error so the backtrace is also available
// via `ErrorCompat::backtrace()`.
impl crate::snafu::ErrorCompat for BoxedError {
    fn backtrace(&self) -> Option<&crate::snafu::Backtrace> {
        None
    }
}

/// Error type with plain error message
#[derive(Debug)]
pub struct PlainError {
    msg: String,
    status_code: StatusCode,
}

impl PlainError {
    pub fn new(msg: String, status_code: StatusCode) -> Self {
        Self { msg, status_code }
    }
}

impl std::fmt::Display for PlainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for PlainError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl crate::ext::ErrorExt for PlainError {
    fn status_code(&self) -> crate::status_code::StatusCode {
        self.status_code
    }

    fn location_opt(&self) -> Option<crate::snafu::Location> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self as _
    }
}
