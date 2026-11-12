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
use std::sync::Arc;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("IO error"))]
    Io {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode protobuf"))]
    DecodeProto {
        #[snafu(source)]
        error: prost::DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Intermediate error"))]
    Intermediate {
        source: crate::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("File size too small for bloom filter"))]
    FileSizeTooSmall {
        size: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected bloom filter meta size"))]
    UnexpectedMetaSize {
        max_meta_size: u64,
        actual_meta_size: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid intermediate magic"))]
    InvalidIntermediateMagic {
        invalid: Vec<u8>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Shared index metadata read failed"))]
    SharedRead {
        source: Arc<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("External error"))]
    External {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            Io { .. }
            | FileSizeTooSmall { .. }
            | UnexpectedMetaSize { .. }
            | DecodeProto { .. }
            | InvalidIntermediateMagic { .. } => StatusCode::Unexpected,

            Intermediate { source, .. } => source.status_code(),
            External { source, .. } => source.status_code(),
            SharedRead { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use snafu::IntoError;

    use super::*;

    #[test]
    fn shared_metadata_error_preserves_status() {
        let source = FileSizeTooSmallSnafu { size: 0_u64 }.build();
        let status = source.status_code();
        let retry = source.retry_hint();
        let error = SharedReadSnafu.into_error(Arc::new(source));
        assert_eq!(error.status_code(), status);
        assert_eq!(error.retry_hint(), retry);
    }
}
