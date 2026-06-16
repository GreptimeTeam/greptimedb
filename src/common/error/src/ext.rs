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
use std::fmt::{Debug, Formatter};
use std::io::ErrorKind;
use std::str::FromStr;
use std::sync::Arc;

use snafu::{FromString, Snafu};

use crate::status_code::StatusCode;

/// Describes whether an error instance is safe and useful to retry.
///
/// This is intentionally separate from [`StatusCode`]: status code describes the
/// error category exposed to users, while retry hint describes retry policy for
/// this specific error instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryHint {
    /// The operation may succeed if retried later.
    Retryable,
    /// Retrying the same operation is not expected to help.
    ///
    /// This is the default for errors that do not explicitly opt in to retry.
    NonRetryable,
}

const RETRY_HINT_RETRYABLE: &str = "retryable";
const RETRY_HINT_NON_RETRYABLE: &str = "non_retryable";

impl RetryHint {
    pub fn is_retryable(self) -> bool {
        matches!(self, RetryHint::Retryable)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RetryHint::Retryable => RETRY_HINT_RETRYABLE,
            RetryHint::NonRetryable => RETRY_HINT_NON_RETRYABLE,
        }
    }
}

impl FromStr for RetryHint {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            RETRY_HINT_RETRYABLE => Ok(RetryHint::Retryable),
            RETRY_HINT_NON_RETRYABLE => Ok(RetryHint::NonRetryable),
            _ => Err(()),
        }
    }
}

/// Converts a [`std::io::Error`] into a conservative [`RetryHint`].
///
/// This helper classifies known transient I/O conditions as retryable and treats
/// request, permission, filesystem-capacity, and data-shape errors as
/// non-retryable. `std::io::ErrorKind` is non-exhaustive, so future or
/// unclassified kinds are considered non-retryable until reviewed explicitly.
pub fn retry_hint_from_io_error(error: &std::io::Error) -> RetryHint {
    match error.kind() {
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::HostUnreachable
        | ErrorKind::NetworkUnreachable
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::NetworkDown
        | ErrorKind::BrokenPipe
        | ErrorKind::WouldBlock
        | ErrorKind::StaleNetworkFileHandle
        | ErrorKind::TimedOut
        | ErrorKind::ResourceBusy
        | ErrorKind::Interrupted => RetryHint::Retryable,

        _ => RetryHint::NonRetryable,
    }
}

/// Extension to [`Error`](std::error::Error) in std.
pub trait ErrorExt: StackError {
    /// Map this error to [StatusCode].
    fn status_code(&self) -> StatusCode {
        StatusCode::Unknown
    }

    /// Returns the retry hint for this error instance.
    ///
    /// Implementations should return [`RetryHint::Retryable`] only when retrying the
    /// same operation may succeed without changing the request. The default is
    /// [`RetryHint::NonRetryable`] to avoid accidental retry loops.
    fn retry_hint(&self) -> RetryHint {
        RetryHint::NonRetryable
    }

    /// Returns whether this error instance is marked retryable.
    ///
    /// This is derived from [`Self::retry_hint`]. Transport-level retries, such as a
    /// gRPC `Unavailable`, may still be handled separately by client code.
    fn is_retryable(&self) -> bool {
        self.retry_hint().is_retryable()
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

                    if error.transparent() {
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

    /// Indicates whether this error is "transparent", that it delegates its "display" and "source"
    /// to the underlying error. Could be useful when you are just wrapping some external error,
    /// **AND** can not or would not provide meaningful contextual info. For example, the
    /// `DataFusionError`.
    fn transparent(&self) -> bool {
        false
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

/// A simple [Result] of which the error is convertible from [ErrorExt] (which every GreptimeDB
/// error implements). Use this if you are tired of writing `unwrap`s in test codes, that you can
/// use the `?` on all GreptimeDB errors.
pub type WhateverResult<T> = Result<T, Whatever>;

#[derive(Snafu)]
#[snafu(display("{inner}"))]
pub struct Whatever {
    inner: snafu::Whatever,
}

impl Debug for Whatever {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<E: ErrorExt> From<E> for Whatever {
    fn from(e: E) -> Self {
        Self {
            inner: FromString::without_source(format!("{e:?}")),
        }
    }
}

impl From<String> for Whatever {
    fn from(s: String) -> Self {
        Self {
            inner: FromString::without_source(s),
        }
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

    fn retry_hint(&self) -> RetryHint {
        self.inner.retry_hint()
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
