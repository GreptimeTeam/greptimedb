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

    #[snafu(display("Invalid character in prefix config: {}", prefix))]
    InvalidColumnPrefix { prefix: String },

    #[snafu(display(
        "DynFilterPayload::Datafusion is {} bytes, which exceeds the configured limit of {} bytes",
        payload_size_bytes,
        max_payload_bytes
    ))]
    DynFilterPayloadTooLarge {
        payload_size_bytes: usize,
        max_payload_bytes: usize,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidInputState { .. }
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
            | Error::InvalidFuncArgs { .. }
            | Error::InvalidColumnPrefix { .. } => StatusCode::InvalidArguments,

            Error::DynFilterPayloadTooLarge { .. } => StatusCode::PlanQuery,

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
    let mut error = e;
    loop {
        error = match error {
            DataFusionError::Shared(inner) => inner,
            DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => inner,
            _ => break,
        };
    }

    match error {
        DataFusionError::Internal(_) => StatusCode::Internal,
        DataFusionError::NotImplemented(_) => StatusCode::Unsupported,
        DataFusionError::Plan(_) => StatusCode::PlanQuery,
        DataFusionError::External(error) => {
            if let Some(ext) = (*error).downcast_ref::<T>() {
                ext.status_code()
            } else if let Some(ext) = (*error).downcast_ref::<BoxedError>() {
                ext.status_code()
            } else {
                default_status.unwrap_or(StatusCode::EngineExecuteQuery)
            }
        }
        _ => default_status.unwrap_or(StatusCode::EngineExecuteQuery),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::ext::PlainError;

    use super::*;

    #[test]
    fn test_datafusion_status_code_preserves_external_errors_through_wrappers() {
        let boxed_error = |status| {
            DataFusionError::External(Box::new(BoxedError::new(PlainError::new(
                "neutral error".to_string(),
                status,
            ))))
        };

        for status in [
            StatusCode::RequestOutdated,
            StatusCode::Unknown,
            StatusCode::Unsupported,
        ] {
            let errors = [
                boxed_error(status),
                DataFusionError::Shared(Arc::new(boxed_error(status))),
                DataFusionError::Context("context".to_string(), Box::new(boxed_error(status))),
                DataFusionError::Diagnostic(
                    Box::new(datafusion_common::Diagnostic::new_error("diagnostic", None)),
                    Box::new(boxed_error(status)),
                ),
                DataFusionError::Shared(Arc::new(DataFusionError::Context(
                    "context".to_string(),
                    Box::new(DataFusionError::Diagnostic(
                        Box::new(datafusion_common::Diagnostic::new_error("diagnostic", None)),
                        Box::new(boxed_error(status)),
                    )),
                ))),
            ];

            for error in errors {
                assert_eq!(datafusion_status_code::<Error>(&error, None), status);
                assert_eq!(
                    datafusion_status_code::<Error>(&error, Some(StatusCode::PlanQuery)),
                    status
                );
            }
        }

        let direct_error = DataFusionError::External(Box::new(Error::DynFilterPayloadTooLarge {
            payload_size_bytes: 2,
            max_payload_bytes: 1,
            location: Location::default(),
        }));
        assert_eq!(
            datafusion_status_code::<Error>(&direct_error, Some(StatusCode::Internal)),
            StatusCode::PlanQuery
        );

        let boundary_error =
            DataFusionError::Shared(Arc::new(DataFusionError::External(Box::new(
                BoxedError::new(common_recordbatch::error::Error::PhysicalExpr {
                    error: DataFusionError::NotImplemented("inner error".to_string()),
                    location: Location::default(),
                }),
            ))));
        assert_eq!(
            datafusion_status_code::<Error>(&boundary_error, Some(StatusCode::PlanQuery)),
            StatusCode::Internal
        );
    }

    #[test]
    fn test_datafusion_status_code_uses_default_for_untyped_errors() {
        let wrap = |error| {
            DataFusionError::Shared(Arc::new(DataFusionError::Context(
                "context".to_string(),
                Box::new(DataFusionError::Diagnostic(
                    Box::new(datafusion_common::Diagnostic::new_error("diagnostic", None)),
                    Box::new(error),
                )),
            )))
        };
        let errors = || {
            [
                (
                    DataFusionError::External(Box::new(std::io::Error::other("neutral io error"))),
                    StatusCode::EngineExecuteQuery,
                    StatusCode::Unknown,
                ),
                (
                    DataFusionError::Internal("neutral internal error".to_string()),
                    StatusCode::Internal,
                    StatusCode::Internal,
                ),
                (
                    DataFusionError::NotImplemented("neutral not implemented error".to_string()),
                    StatusCode::Unsupported,
                    StatusCode::Unsupported,
                ),
                (
                    DataFusionError::Plan("neutral plan error".to_string()),
                    StatusCode::PlanQuery,
                    StatusCode::PlanQuery,
                ),
                (
                    DataFusionError::External(Box::new(DataFusionError::Internal(
                        "inner error".to_string(),
                    ))),
                    StatusCode::EngineExecuteQuery,
                    StatusCode::Unknown,
                ),
            ]
        };

        for (error, none_expected, default_expected) in
            errors()
                .into_iter()
                .chain(errors().map(|(error, none_expected, default_expected)| {
                    (wrap(error), none_expected, default_expected)
                }))
        {
            assert_eq!(datafusion_status_code::<Error>(&error, None), none_expected);
            assert_eq!(
                datafusion_status_code::<Error>(&error, Some(StatusCode::Unknown)),
                default_expected
            );
        }
    }
}
