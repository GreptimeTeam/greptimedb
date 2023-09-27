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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use prost::{DecodeError, EncodeError};
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported physical plan: {}", name))]
    UnsupportedPlan { name: String, location: Location },

    #[snafu(display("Unsupported expr: {}", name))]
    UnsupportedExpr { name: String, location: Location },

    #[snafu(display("Unsupported concrete type: {:?}", ty))]
    UnsupportedConcreteType {
        ty: ConcreteDataType,
        location: Location,
    },

    #[snafu(display("Unsupported substrait type: {}", ty))]
    UnsupportedSubstraitType { ty: String, location: Location },

    #[snafu(display("Failed to decode substrait relation"))]
    DecodeRel {
        #[snafu(source)]
        error: DecodeError,
        location: Location,
    },

    #[snafu(display("Failed to encode substrait relation"))]
    EncodeRel {
        #[snafu(source)]
        error: EncodeError,
        location: Location,
    },

    #[snafu(display("Input plan is empty"))]
    EmptyPlan { location: Location },

    #[snafu(display("Input expression is empty"))]
    EmptyExpr { location: Location },

    #[snafu(display("Missing required field in protobuf, field: {}, plan: {}", field, plan))]
    MissingField {
        field: String,
        plan: String,
        location: Location,
    },

    #[snafu(display("Invalid parameters: {}", reason))]
    InvalidParameters { reason: String, location: Location },

    #[snafu(display("Internal error from DataFusion"))]
    DFInternal {
        #[snafu(source)]
        error: DataFusionError,
        location: Location,
    },

    #[snafu(display("Internal error"))]
    Internal {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Cannot convert plan doesn't belong to GreptimeDB"))]
    UnknownPlan { location: Location },

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
        location: Location,
    },

    #[snafu(display("Failed to convert DataFusion schema"))]
    ConvertDfSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Unable to resolve table: {table_name}, error: "))]
    ResolveTable {
        table_name: String,
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to encode DataFusion plan"))]
    EncodeDfPlan {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to decode DataFusion plan"))]
    DecodeDfPlan {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        location: Location,
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
            | Error::SchemaNotMatch { .. } => StatusCode::InvalidArguments,
            Error::DFInternal { .. }
            | Error::Internal { .. }
            | Error::EncodeDfPlan { .. }
            | Error::DecodeDfPlan { .. } => StatusCode::Internal,
            Error::ConvertDfSchema { source, .. } => source.status_code(),
            Error::ResolveTable { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
