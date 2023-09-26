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

//! Utils for mock.

use std::any::Any;
use std::fmt;

use snafu::Location;

use crate::ext::{ErrorExt, StackError};
use crate::status_code::StatusCode;

/// A mock error mainly for test.
#[derive(Debug)]
pub struct MockError {
    pub code: StatusCode,
    source: Option<Box<MockError>>,
}

impl MockError {
    /// Create a new [MockError] without backtrace.
    pub fn new(code: StatusCode) -> MockError {
        MockError { code, source: None }
    }

    /// Create a new [MockError] with source.
    pub fn with_source(source: MockError) -> MockError {
        MockError {
            code: source.code,
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

    fn location_opt(&self) -> Option<Location> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StackError for MockError {
    fn debug_fmt(&self, _: usize, _: &mut Vec<String>) {}

    fn next(&self) -> Option<&dyn StackError> {
        None
    }
}
