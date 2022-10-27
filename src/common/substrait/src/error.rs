use std::any::Any;

use common_error::prelude::{BoxedError, ErrorExt, StatusCode};
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use prost::{DecodeError, EncodeError};
use snafu::{Backtrace, ErrorCompat, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported physical expr: {}", name))]
    UnsupportedPlan { name: String, backtrace: Backtrace },

    #[snafu(display("Unsupported physical plan: {}", name))]
    UnsupportedExpr { name: String, backtrace: Backtrace },

    #[snafu(display("Unsupported concrete type: {:?}", ty))]
    UnsupportedConcreteType {
        ty: ConcreteDataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Unsupported substrait type: {}", ty))]
    UnsupportedSubstraitType { ty: String, backtrace: Backtrace },

    #[snafu(display("Failed to decode substrait relation, source: {}", source))]
    DecodeRel {
        source: DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode substrait relation, source: {}", source))]
    EncodeRel {
        source: EncodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Input plan is empty"))]
    EmptyPlan { backtrace: Backtrace },

    #[snafu(display("Input expression is empty"))]
    EmptyExpr { backtrace: Backtrace },

    #[snafu(display("Missing required field in protobuf, field: {}, plan: {}", field, plan))]
    MissingField {
        field: String,
        plan: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid parameters: {}", reason))]
    InvalidParameters {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal error from DataFusion: {}", source))]
    DFInternal {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal error: {}", source))]
    Internal {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Table quering not found: {}", name))]
    TableNotFound { name: String, backtrace: Backtrace },

    #[snafu(display("Cannot convert plan doesn't belong to GrepTimeDB"))]
    UnknownPlan { backtrace: Backtrace },

    #[snafu(display(
        "Schema from Substrait proto doesn't match with the schema in storage.
        Substrait schema: {:?}
        Storage schema: {:?}",
        substrait_schema,
        storage_schema
    ))]
    SchemaNotMatch {
        substrait_schema: datafusion::arrow::datatypes::SchemaRef,
        storage_schema: datafusion::arrow::datatypes::SchemaRef,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::UnsupportedConcreteType { .. }
            | Error::UnsupportedPlan { .. }
            | Error::UnsupportedExpr { .. }
            | Error::UnsupportedSubstraitType { .. } => StatusCode::Unsupported,
            Error::UnknownPlan { .. }
            | Error::EncodeRel { .. }
            | Error::DecodeRel { .. }
            | Error::EmptyPlan { .. }
            | Error::EmptyExpr { .. }
            | Error::MissingField { .. }
            | Error::InvalidParameters { .. }
            | Error::TableNotFound { .. }
            | Error::SchemaNotMatch { .. } => StatusCode::InvalidArguments,
            Error::DFInternal { .. } | Error::Internal { .. } => StatusCode::Internal,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
