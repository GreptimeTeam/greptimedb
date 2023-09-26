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
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use tokio::task::JoinError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to build runtime"))]
    BuildRuntime {
        #[snafu(source)]
        error: std::io::Error,
        location: Location,
    },

    #[snafu(display("Repeated task {} is already started", name))]
    IllegalState { name: String, location: Location },

    #[snafu(display("Failed to wait for repeated task {} to stop", name))]
    WaitGcTaskStop {
        name: String,
        #[snafu(source)]
        error: JoinError,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn location_opt(&self) -> Option<common_error::snafu::Location> {
        match self {
            Error::BuildRuntime { location, .. }
            | Error::IllegalState { location, .. }
            | Error::WaitGcTaskStop { location, .. } => Some(*location),
        }
    }
}
