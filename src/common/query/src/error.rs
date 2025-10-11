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
use datafusion_common::DataFusionError;
use datatypes::arrow;
use datatypes::arrow::datatypes::DataType as ArrowDatatype;
use datatypes::error::Error as DataTypeError;
use datatypes::prelude::ConcreteDataType;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported input datatypes {:?} in function {}", datatypes, function))]
    UnsupportedInputDataType {
        function: String,
        datatypes: Vec<ConcreteDataType>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast scalar value into vector"))]
    FromScalarValue {
        #[snafu(implicit)]
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Failed to cast arrow array into vector: {:?}", data_type))]
    IntoVector {
        #[snafu(implicit)]
        location: Location,
        source: DataTypeError,
        data_type: ArrowDatatype,
    },

    #[snafu(display("Failed to downcast vector: {}", err_msg))]
    DowncastVector { err_msg: String },

    #[snafu(display("Invalid input type: {}", err_msg))]
    InvalidInputType {
        #[snafu(implicit)]
        location: Location,
        source: DataTypeError,
        err_msg: String,
    },

    #[snafu(display(
        "Illegal input_types status, check if DataFusion has changed its UDAF execution logic"
    ))]
    InvalidInputState {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(transparent)]
    GeneralDataFusion {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert DataFusion's recordbatch stream"))]
    ConvertDfRecordBatchStream {
        #[snafu(implicit)]
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to cast array to {:?}", typ))]
    TypeCast {
        #[snafu(source)]
        error: ArrowError,
        typ: arrow::datatypes::DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to perform compute operation on arrow arrays"))]
    ArrowCompute {
        #[snafu(source)]
        error: ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Query engine fail to cast value"))]
    ToScalarValue {
        #[snafu(implicit)]
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Failed to get scalar vector"))]
    GetScalarVector {
        #[snafu(implicit)]
        location: Location,
        source: DataTypeError,
    },

    #[snafu(display("Failed to execute function: {source}"))]
    Execute {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to decode logical plan: {source}"))]
    DecodePlan {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to do table mutation"))]
    TableMutation {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to do procedure task"))]
    ProcedureService {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing TableMutationHandler, not expected"))]
    MissingTableMutationHandler {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing ProcedureServiceHandler, not expected"))]
    MissingProcedureServiceHandler {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing FlowServiceHandler, not expected"))]
    MissingFlowServiceHandler {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid function args: {}", err_msg))]
    InvalidFuncArgs {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Permission denied: {}", err_msg))]
    PermissionDenied {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Can't found alive flownode"))]
    FlownodeNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid vector string: {}", vec_str))]
    InvalidVectorString {
        vec_str: String,
        source: DataTypeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to register UDF: {}", name))]
    RegisterUdf {
        name: String,
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::DowncastVector { .. }
            | Error::InvalidInputState { .. }
            | Error::ToScalarValue { .. }
            | Error::GetScalarVector { .. }
            | Error::ArrowCompute { .. }
            | Error::FlownodeNotFound { .. } => StatusCode::EngineExecuteQuery,

            Error::GeneralDataFusion { error, .. } => datafusion_status_code::<Self>(error, None),

            Error::InvalidInputType { source, .. }
            | Error::IntoVector { source, .. }
            | Error::FromScalarValue { source, .. }
            | Error::InvalidVectorString { source, .. } => source.status_code(),

            Error::MissingTableMutationHandler { .. }
            | Error::MissingProcedureServiceHandler { .. }
            | Error::MissingFlowServiceHandler { .. }
            | Error::RegisterUdf { .. } => StatusCode::Unexpected,

            Error::UnsupportedInputDataType { .. }
            | Error::TypeCast { .. }
            | Error::InvalidFuncArgs { .. } => StatusCode::InvalidArguments,

            Error::ConvertDfRecordBatchStream { source, .. } => source.status_code(),

            Error::DecodePlan { source, .. }
            | Error::Execute { source, .. }
            | Error::ProcedureService { source, .. }
            | Error::TableMutation { source, .. } => source.status_code(),

            Error::PermissionDenied { .. } => StatusCode::PermissionDenied,
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

/// Try to get the proper [`StatusCode`] of [`DataFusionError].
pub fn datafusion_status_code<T: ErrorExt + 'static>(
    e: &DataFusionError,
    default_status: Option<StatusCode>,
) -> StatusCode {
    match e {
        DataFusionError::Internal(_) => StatusCode::Internal,
        DataFusionError::NotImplemented(_) => StatusCode::Unsupported,
        DataFusionError::Plan(_) => StatusCode::PlanQuery,
        DataFusionError::External(e) => {
            if let Some(ext) = (*e).downcast_ref::<T>() {
                ext.status_code()
            } else {
                default_status.unwrap_or(StatusCode::EngineExecuteQuery)
            }
        }
        DataFusionError::Diagnostic(_, e) => datafusion_status_code::<T>(e, default_status),
        _ => default_status.unwrap_or(StatusCode::EngineExecuteQuery),
    }
}
