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

use common_error::ext::{ErrorExt, PlainError, StackError};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, ResultExt, Snafu};

#[derive(Snafu)]
#[stack_trace_debug]
enum MyError {
    #[snafu(display(r#"A normal error with "display" attribute, message "{}""#, message))]
    Normal {
        message: String,
        #[snafu(source)]
        error: PlainError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(transparent)]
    Transparent {
        #[snafu(source)]
        error: PlainError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for MyError {
    fn status_code(&self) -> StatusCode {
        StatusCode::Unexpected
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn normal_error() -> Result<(), MyError> {
    let plain_error = PlainError::new("<root cause>".to_string(), StatusCode::Unexpected);
    Err(plain_error).context(NormalSnafu { message: "blabla" })
}

fn transparent_error() -> Result<(), MyError> {
    let plain_error = PlainError::new("<root cause>".to_string(), StatusCode::Unexpected);
    Err(plain_error)?
}

#[test]
fn test_output_msg() {
    let result = normal_error();
    assert_eq!(
        result.unwrap_err().output_msg(),
        r#"A normal error with "display" attribute, message "blabla": <root cause>"#
    );

    let result = transparent_error();
    assert_eq!(result.unwrap_err().output_msg(), "<root cause>");
}

#[test]
fn test_to_string() {
    let result = normal_error();
    assert_eq!(
        result.unwrap_err().to_string(),
        r#"A normal error with "display" attribute, message "blabla""#
    );

    let result = transparent_error();
    assert_eq!(result.unwrap_err().to_string(), "<root cause>");
}

#[test]
fn test_debug_format() {
    let result = normal_error();
    assert_eq!(
        format!("{:?}", result.unwrap_err()),
        r#"0: A normal error with "display" attribute, message "blabla", at src/common/error/tests/ext.rs:55:22
1: PlainError { msg: "<root cause>", status_code: Unexpected }"#
    );

    let result = transparent_error();
    assert_eq!(
        format!("{:?}", result.unwrap_err()),
        r#"0: <transparent>, at src/common/error/tests/ext.rs:60:5
1: PlainError { msg: "<root cause>", status_code: Unexpected }"#
    );
}

#[test]
fn test_transparent_flag() {
    let result = normal_error();
    assert!(!result.unwrap_err().transparent());

    let result = transparent_error();
    assert!(result.unwrap_err().transparent());
}
