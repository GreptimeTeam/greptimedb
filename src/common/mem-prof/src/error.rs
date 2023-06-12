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
use std::path::PathBuf;

use common_error::prelude::{ErrorExt, StatusCode};
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to read OPT_PROF, source: {}", source))]
    ReadOptProf { source: tikv_jemalloc_ctl::Error },

    #[snafu(display("Memory profiling is not enabled"))]
    ProfilingNotEnabled,

    #[snafu(display("Failed to build temp file from given path: {:?}", path))]
    BuildTempPath { path: PathBuf, location: Location },

    #[snafu(display("Failed to open temp file: {}, source: {}", path, source))]
    OpenTempFile {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Failed to dump profiling data to temp file: {:?}, source: {}",
        path,
        source
    ))]
    DumpProfileData {
        path: PathBuf,
        source: tikv_jemalloc_ctl::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ReadOptProf { .. } => StatusCode::Internal,
            Error::ProfilingNotEnabled => StatusCode::InvalidArguments,
            Error::BuildTempPath { .. } => StatusCode::Internal,
            Error::OpenTempFile { .. } => StatusCode::StorageUnavailable,
            Error::DumpProfileData { .. } => StatusCode::StorageUnavailable,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
