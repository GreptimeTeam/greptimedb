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
use std::sync::Arc;

use crate::status_code::StatusCode;

/// Extension to [`Error`](std::error::Error) in std.
pub trait ErrorExt: StackError {
    /// Map this error to [StatusCode].
    fn status_code(&self) -> StatusCode {
        StatusCode::Unknown
    }

    /// Returns the error as [Any](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    fn output_msg(&self) -> String
    where
        Self: Sized,
    {
        match self.status_code() {
            StatusCode::Unknown | StatusCode::Internal => {
                // masks internal error from end user
                format!("Internal error: {}", self.status_code() as u32)
            }
            _ => {
                let error = self.last();
                if let Some(external_error) = error.source() {
                    let external_root = external_error.sources().last().unwrap();

                    if error.to_string().is_empty() {
                        format!("{external_root}")
                    } else {
                        format!("{error}: {external_root}")
                    }
                } else {
                    format!("{error}")
                }
            }
        }
    }

    /// Find out root level error for nested error
    fn root_cause(&self) -> Option<&dyn std::error::Error>
    where
        Self: Sized,
    {
        let error = self.last();
        if let Some(external_error) = error.source() {
            let external_root = external_error.sources().last().unwrap();
            Some(external_root)
        } else {
            None
        }
    }
}

pub trait StackError: std::error::Error {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>);

    fn next(&self) -> Option<&dyn StackError>;

    fn last(&self) -> &dyn StackError
    where
        Self: Sized,
    {
        let Some(mut result) = self.next() else {
            return self;
        };
        while let Some(err) = result.next() {
            result = err;
        }
        result
    }
}

impl<T: ?Sized + StackError> StackError for Arc<T> {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        self.as_ref().debug_fmt(layer, buf)
    }

    fn next(&self) -> Option<&dyn StackError> {
        self.as_ref().next()
    }
}

impl<T: StackError> StackError for Box<T> {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        self.as_ref().debug_fmt(layer, buf)
    }

    fn next(&self) -> Option<&dyn StackError> {
        self.as_ref().next()
    }
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

    pub fn into_inner(self) -> Box<dyn crate::ext::ErrorExt + Send + Sync> {
        self.inner
    }
}

impl std::fmt::Debug for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = vec![];
        self.debug_fmt(0, &mut buf);
        write!(f, "{}", buf.join("\n"))
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

impl StackError for BoxedError {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        self.inner.debug_fmt(layer, buf)
    }

    fn next(&self) -> Option<&dyn StackError> {
        self.inner.next()
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

    fn as_any(&self) -> &dyn std::any::Any {
        self as _
    }
}

impl StackError for PlainError {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        buf.push(format!("{}: {}", layer, self.msg))
    }

    fn next(&self) -> Option<&dyn StackError> {
        None
    }
}
