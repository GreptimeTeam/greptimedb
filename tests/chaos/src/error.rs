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
use procfs::ProcError;
use snafu::Location;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to get process info: {}", source))]
    GetProcInfo {
        location: Location,
        source: ProcError,
    },

    #[snafu(display("Failed to send request: {}", source))]
    SendRequest {
        location: Location,
        source: reqwest::Error,
    },

    #[snafu(display("Failed to decode object from json, source: {}", source))]
    DecodeResponse {
        location: Location,
        source: reqwest::Error,
    },

    #[snafu(display("Received known status: {}", msg))]
    UnknownStatus { location: Location, msg: String },

    #[snafu(display("Try to invoke a invoked nemesis: {}", msg))]
    InvokedNemesis { location: Location, msg: String },

    #[snafu(display("Unknown process"))]
    UnknownProcess { location: Location },

    #[snafu(display("Received error from server: {}", msg))]
    ErrorResponse { location: Location, msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;
