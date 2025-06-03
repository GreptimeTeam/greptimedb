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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to read cgroup mount point {}", path))]
    ReadCgroupMountpoint {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: nix::Error,
    },

    #[snafu(display("Failed to read file {}", path))]
    ReadFile {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to parse cgroup data from file {}", path))]
    ParseCgroupData {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::num::ParseIntError,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ReadCgroupMountpoint { .. }
            | Error::ReadFile { .. }
            | Error::ParseCgroupData { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
