// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Error of record batch.
use std::any::Any;

use common_error::ext::BoxedError;
use common_error::prelude::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to create datafusion record batch, source: {}", source))]
    NewDfRecordBatch {
        source: datatypes::arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Data types error, source: {}", source))]
    DataTypes {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("External error, source: {}", source))]
    External {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to create RecordBatches, reason: {}", reason))]
    CreateRecordBatches {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert Arrow schema, source: {}", source))]
    SchemaConversion {
        source: datatypes::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to poll stream, source: {}", source))]
    PollStream {
        source: datatypes::arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to format record batch, source: {}", source))]
    Format {
        source: datatypes::arrow::error::ArrowError,
        backtrace: Backtrace,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::NewDfRecordBatch { .. } => StatusCode::InvalidArguments,

            Error::DataTypes { .. }
            | Error::CreateRecordBatches { .. }
            | Error::PollStream { .. }
            | Error::Format { .. } => StatusCode::Internal,

            Error::External { source } => source.status_code(),

            Error::SchemaConversion { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
