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

//! Error of record batch.
use std::any::Any;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datatypes::prelude::ConcreteDataType;
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Fail to create datafusion record batch"))]
    NewDfRecordBatch {
        #[snafu(source)]
        error: datatypes::arrow::error::ArrowError,
        location: Location,
    },

    #[snafu(display("Data types error"))]
    DataTypes {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("External error"))]
    External {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to create RecordBatches, reason: {}", reason))]
    CreateRecordBatches { reason: String, location: Location },

    #[snafu(display("Failed to convert Arrow schema"))]
    SchemaConversion {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display(""))]
    PollStream {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        location: Location,
    },

    #[snafu(display("Fail to format record batch"))]
    Format {
        #[snafu(source)]
        error: datatypes::arrow::error::ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to init Recordbatch stream"))]
    InitRecordbatchStream {
        #[snafu(source)]
        error: datafusion_common::DataFusionError,
        location: Location,
    },

    #[snafu(display(
        "Failed to project Arrow RecordBatch with schema {:?} and projection {:?}",
        schema,
        projection,
    ))]
    ProjectArrowRecordBatch {
        #[snafu(source)]
        error: datatypes::arrow::error::ArrowError,
        location: Location,
        schema: datatypes::schema::SchemaRef,
        projection: Vec<usize>,
    },

    #[snafu(display("Column {} not exists in table {}", column_name, table_name))]
    ColumnNotExists {
        column_name: String,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to cast vector of type '{:?}' to type '{:?}'",
        from_type,
        to_type,
    ))]
    CastVector {
        from_type: ConcreteDataType,
        to_type: ConcreteDataType,
        location: Location,
        source: datatypes::error::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::NewDfRecordBatch { .. } => StatusCode::InvalidArguments,

            Error::DataTypes { .. }
            | Error::CreateRecordBatches { .. }
            | Error::PollStream { .. }
            | Error::Format { .. }
            | Error::InitRecordbatchStream { .. }
            | Error::ColumnNotExists { .. }
            | Error::ProjectArrowRecordBatch { .. } => StatusCode::Internal,

            Error::External { source, .. } => source.status_code(),

            Error::SchemaConversion { source, .. } | Error::CastVector { source, .. } => {
                source.status_code()
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
