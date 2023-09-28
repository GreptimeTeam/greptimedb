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

use arrow::error::ArrowError;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_recordbatch::error::Error as RecordbatchError;
use datafusion_common::DataFusionError;
use datatypes::arrow;
use datatypes::arrow::datatypes::DataType as ArrowDatatype;
use datatypes::error::Error as DataTypeError;
use datatypes::prelude::ConcreteDataType;
use snafu::{Location, Snafu};
use statrs::StatsError;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to execute Python UDF: {}", msg))]
    PyUdf {
        // TODO(discord9): find a way that prevent circle depend(query<-script<-query) and can use script's error type
        msg: String,
        location: Location,
    },

    #[snafu(display("Failed to create temporary recordbatch when eval Python UDF"))]
    UdfTempRecordBatch {
        location: Location,
        source: RecordbatchError,
    },

    #[snafu(display("Failed to execute function"))]
    ExecuteFunction {
        #[snafu(source)]
        error: DataFusionError,
        location: Location,
    },

    #[snafu(display("Unsupported input datatypes {:?} in function {}", datatypes, function))]
    UnsupportedInputDataType {
        function: String,
        datatypes: Vec<ConcreteDataType>,
        location: Location,
    },

    #[snafu(display("Failed to generate function"))]
    GenerateFunction {
        #[snafu(source)]
        error: StatsError,
        location: Location,
    },

    #[snafu(display("Failed to cast scalar value into vector"))]
    FromScalarValue {
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Failed to cast arrow array into vector"))]
    FromArrowArray {
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Failed to cast arrow array into vector: {:?}", data_type))]
    IntoVector {
        location: Location,
        source: DataTypeError,
        data_type: ArrowDatatype,
    },

    #[snafu(display("Failed to create accumulator: {}", err_msg))]
    CreateAccumulator { err_msg: String },

    #[snafu(display("Failed to downcast vector: {}", err_msg))]
    DowncastVector { err_msg: String },

    #[snafu(display("Bad accumulator implementation: {}", err_msg))]
    BadAccumulatorImpl { err_msg: String, location: Location },

    #[snafu(display("Invalid input type: {}", err_msg))]
    InvalidInputType {
        location: Location,
        source: DataTypeError,
        err_msg: String,
    },

    #[snafu(display(
        "Illegal input_types status, check if DataFusion has changed its UDAF execution logic"
    ))]
    InvalidInputState { location: Location },

    #[snafu(display("unexpected: not constant column"))]
    InvalidInputCol { location: Location },

    #[snafu(display("Not expected to run ExecutionPlan more than once"))]
    ExecuteRepeatedly { location: Location },

    #[snafu(display("General DataFusion error"))]
    GeneralDataFusion {
        #[snafu(source)]
        error: DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to convert DataFusion's recordbatch stream"))]
    ConvertDfRecordBatchStream {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to convert arrow schema"))]
    ConvertArrowSchema {
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Failed to execute physical plan"))]
    ExecutePhysicalPlan {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to cast array to {:?}", typ))]
    TypeCast {
        #[snafu(source)]
        error: ArrowError,
        typ: arrow::datatypes::DataType,
        location: Location,
    },

    #[snafu(display("Failed to perform compute operation on arrow arrays"))]
    ArrowCompute {
        #[snafu(source)]
        error: ArrowError,
        location: Location,
    },

    #[snafu(display("Query engine fail to cast value"))]
    ToScalarValue {
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Failed to get scalar vector"))]
    GetScalarVector {
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Invalid function args: {}", err_msg))]
    InvalidFuncArgs { err_msg: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::UdfTempRecordBatch { .. }
            | Error::PyUdf { .. }
            | Error::ExecuteFunction { .. }
            | Error::GenerateFunction { .. }
            | Error::CreateAccumulator { .. }
            | Error::DowncastVector { .. }
            | Error::InvalidInputState { .. }
            | Error::InvalidInputCol { .. }
            | Error::BadAccumulatorImpl { .. }
            | Error::ToScalarValue { .. }
            | Error::GetScalarVector { .. }
            | Error::ArrowCompute { .. } => StatusCode::EngineExecuteQuery,

            Error::InvalidInputType { source, .. }
            | Error::IntoVector { source, .. }
            | Error::FromScalarValue { source, .. }
            | Error::ConvertArrowSchema { source, .. }
            | Error::FromArrowArray { source, .. } => source.status_code(),

            Error::ExecuteRepeatedly { .. } | Error::GeneralDataFusion { .. } => {
                StatusCode::Unexpected
            }

            Error::UnsupportedInputDataType { .. }
            | Error::TypeCast { .. }
            | Error::InvalidFuncArgs { .. } => StatusCode::InvalidArguments,

            Error::ConvertDfRecordBatchStream { source, .. } => source.status_code(),
            Error::ExecutePhysicalPlan { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}
