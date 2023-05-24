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

use common_error::prelude::*;
use serde_json::error::Error as JsonError;
use snafu::Location;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to encode object into json, source: {}", source))]
    EncodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Failed to decode object into json, source: {}", source))]
    DecodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Payload not exist"))]
    PayloadNotExist { location: Location },

    #[snafu(display("Failed to send message: {err_msg}"))]
    SendMessage { err_msg: String, location: Location },

    #[snafu(display("Failed to serde json, source: {}", source))]
    SerdeJson {
        source: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Corrupted table route data, err: {}", err_msg))]
    RouteInfoCorrupted { err_msg: String, location: Location },

    #[snafu(display("Illegal state from server, code: {}, error: {}", code, err_msg))]
    IllegalServerState {
        code: i32,
        err_msg: String,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            IllegalServerState { .. } => StatusCode::Internal,
            SerdeJson { .. } | RouteInfoCorrupted { .. } => StatusCode::Unexpected,

            SendMessage { .. } => StatusCode::Internal,

            EncodeJson { .. } | DecodeJson { .. } | PayloadNotExist { .. } => {
                StatusCode::Unexpected
            }
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
