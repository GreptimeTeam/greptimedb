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

use std::fmt::Debug;

use serde::Serialize;
use snafu::ResultExt;
use tonic::codegen::http;

use crate::error::{self, Result};

/// Returns a 200 response with a text body.
pub fn to_text_response(text: &str) -> Result<http::Response<String>> {
    http::Response::builder()
        .header("Content-Type", "text/plain")
        .status(http::StatusCode::OK)
        .body(text.to_string())
        .context(error::InvalidHttpBodySnafu)
}

/// Returns a 200 response with a JSON body.
pub fn to_json_response<T>(response: T) -> Result<http::Response<String>>
where
    T: Serialize + Debug,
{
    let response = serde_json::to_string(&response).context(error::SerializeToJsonSnafu {
        input: format!("{response:?}"),
    })?;
    http::Response::builder()
        .header("Content-Type", "application/json")
        .status(http::StatusCode::OK)
        .body(response)
        .context(error::InvalidHttpBodySnafu)
}

/// Returns a 404 response with an empty body.
pub fn to_not_found_response() -> Result<http::Response<String>> {
    http::Response::builder()
        .status(http::StatusCode::NOT_FOUND)
        .body("".to_string())
        .context(error::InvalidHttpBodySnafu)
}
