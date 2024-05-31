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
use datafusion::error::DataFusionError;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Internal error during building DataFusion plan"))]
    DataFusionPlanning {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Illegal range: offset {}, length {}, array len {}",
        offset,
        length,
        len,
    ))]
    IllegalRange {
        offset: u32,
        length: u32,
        len: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize"))]
    Deserialize {
        #[snafu(source)]
        error: prost::DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty range is not expected"))]
    EmptyRange {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find column {col}"))]
    ColumnNotFound {
        col: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            Deserialize { .. } => StatusCode::Unexpected,
            IllegalRange { .. } | ColumnNotFound { .. } | EmptyRange { .. } => {
                StatusCode::InvalidArguments
            }

            DataFusionPlanning { .. } => StatusCode::PlanQuery,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(err: Error) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

pub(crate) fn ensure(
    predicate: bool,
    error: DataFusionError,
) -> std::result::Result<(), DataFusionError> {
    if predicate {
        Ok(())
    } else {
        Err(error)
    }
}
