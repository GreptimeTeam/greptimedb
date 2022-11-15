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

use std::any::Any;

use common_error::prelude::*;
use datafusion::error::DataFusionError;
use snafu::{Backtrace, ErrorCompat, Snafu};

common_error::define_opaque_error!(Error);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, backtrace: Backtrace },

    #[snafu(display("General catalog error: {}", source))]
    Catalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Catalog not found: {}", catalog))]
    CatalogNotFound {
        catalog: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Schema not found: {}", schema))]
    SchemaNotFound {
        schema: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to do vector computation, source: {}", source))]
    VectorComputation {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to create RecordBatch, source: {}", source))]
    CreateRecordBatch {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        use InnerError::*;

        match self {
            UnsupportedExpr { .. } | CatalogNotFound { .. } | SchemaNotFound { .. } => {
                StatusCode::InvalidArguments
            }
            Catalog { source } => source.status_code(),
            VectorComputation { source } => source.status_code(),
            CreateRecordBatch { source } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<InnerError> for Error {
    fn from(e: InnerError) -> Error {
        Error::new(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}

impl From<catalog::error::Error> for Error {
    fn from(e: catalog::error::Error) -> Self {
        Error::new(e)
    }
}
