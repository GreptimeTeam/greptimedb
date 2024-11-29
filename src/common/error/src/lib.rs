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

#![feature(error_iter)]

pub mod ext;
pub mod mock;
pub mod status_code;

use http::{HeaderMap, HeaderValue};
pub use snafu;

// HACK - these headers are here for shared in gRPC services. For common HTTP headers,
// please define in `src/servers/src/http/header.rs`.
pub const GREPTIME_DB_HEADER_ERROR_CODE: &str = "x-greptime-err-code";
pub const GREPTIME_DB_HEADER_ERROR_MSG: &str = "x-greptime-err-msg";

/// Create a http header map from error code and message.
/// using `GREPTIME_DB_HEADER_ERROR_CODE` and `GREPTIME_DB_HEADER_ERROR_MSG` as keys.
pub fn from_err_code_msg_to_header(
    code: impl Into<HeaderValue>,
    msg: impl Into<HeaderValue>,
) -> HeaderMap {
    let mut header = HeaderMap::new();
    header.insert(GREPTIME_DB_HEADER_ERROR_CODE, code.into());
    header.insert(GREPTIME_DB_HEADER_ERROR_MSG, msg.into());
    header
}
