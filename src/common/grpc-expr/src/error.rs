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

use api::v1::ColumnDataType;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use store_api::metadata::MetadataError;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Column datatype error"))]
    ColumnDataType {
        #[snafu(implicit)]
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display(
        "Duplicated timestamp column in gRPC requests, exists {}, duplicated: {}",
        exists,
        duplicated
    ))]
    DuplicatedTimestampColumn {
        exists: String,
        duplicated: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Duplicated column name in gRPC requests, name: {}", name,))]
    DuplicatedColumnName {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing timestamp column, msg: {}", msg))]
    MissingTimestampColumn {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField {
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid column proto definition, column: {}", column))]
    InvalidColumnDef {
        column: String,
        #[snafu(implicit)]
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Unknown location type: {}", location_type))]
    UnknownLocationType {
        location_type: i32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unknown proto column datatype: {}", datatype))]
    UnknownColumnDataType {
        datatype: i32,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: prost::UnknownEnumValue,
    },

    #[snafu(display(
        "Fulltext or Skipping index only supports string type, column: {column_name}, unexpected type: {column_type:?}"
    ))]
    InvalidStringIndexColumnType {
        column_name: String,
        column_type: ColumnDataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid set table option request"))]
    InvalidSetTableOptionRequest {
        #[snafu(source)]
        error: MetadataError,
    },

    #[snafu(display("Invalid unset table option request"))]
    InvalidUnsetTableOptionRequest {
        #[snafu(source)]
        error: MetadataError,
    },

    #[snafu(display("Invalid set fulltext option request"))]
    InvalidSetFulltextOptionRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: prost::UnknownEnumValue,
    },

    #[snafu(display("Invalid set skipping index option request"))]
    InvalidSetSkippingIndexOptionRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: prost::UnknownEnumValue,
    },

    #[snafu(display("Missing alter index options"))]
    MissingAlterIndexOption {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid index option"))]
    InvalidIndexOption {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: datatypes::error::Error,
    },

    #[snafu(display("Sql common error"))]
    SqlCommon {
        source: common_sql::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required field in protobuf, column name: {}", column_name))]
    ColumnNotFound {
        column_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Need table metadata, but not found, table_id: {}", table_id))]
    MissingTableMeta {
        table_id: u32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected: {err_msg}"))]
    Unexpected {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ColumnDataType { .. } => StatusCode::Internal,
            Error::DuplicatedTimestampColumn { .. }
            | Error::DuplicatedColumnName { .. }
            | Error::MissingTimestampColumn { .. } => StatusCode::InvalidArguments,
            Error::MissingField { .. } => StatusCode::InvalidArguments,
            Error::InvalidColumnDef { source, .. } => source.status_code(),
            Error::UnknownLocationType { .. } => StatusCode::InvalidArguments,

            Error::UnknownColumnDataType { .. } | Error::InvalidStringIndexColumnType { .. } => {
                StatusCode::InvalidArguments
            }
            Error::InvalidSetTableOptionRequest { .. }
            | Error::InvalidUnsetTableOptionRequest { .. }
            | Error::InvalidSetFulltextOptionRequest { .. }
            | Error::InvalidSetSkippingIndexOptionRequest { .. }
            | Error::MissingAlterIndexOption { .. }
            | Error::InvalidIndexOption { .. } => StatusCode::InvalidArguments,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,
            Error::SqlCommon { source, .. } => source.status_code(),
            Error::MissingTableMeta { .. } => StatusCode::Unexpected,
            Error::Unexpected { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
