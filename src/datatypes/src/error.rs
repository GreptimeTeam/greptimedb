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
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to serialize data"))]
    Serialize {
        source: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize data, json: {}", json))]
    Deserialize {
        source: serde_json::Error,
        location: Location,
        json: String,
    },

    #[snafu(display("Failed to convert datafusion type: {}", from))]
    Conversion { from: String, location: Location },

    #[snafu(display("Bad array access, Index out of bounds: {}, size: {}", index, size))]
    BadArrayAccess {
        index: usize,
        size: usize,
        location: Location,
    },

    #[snafu(display("Unknown vector, {}", msg))]
    UnknownVector { msg: String, location: Location },

    #[snafu(display("Unsupported arrow data type, type: {:?}", arrow_type))]
    UnsupportedArrowType {
        arrow_type: arrow::datatypes::DataType,
        location: Location,
    },

    #[snafu(display("Unsupported operation: {} for vector: {}", op, vector_type))]
    UnsupportedOperation {
        op: String,
        vector_type: String,
        location: Location,
    },

    #[snafu(display("Failed to parse version in schema meta, value: {}", value))]
    ParseSchemaVersion {
        value: String,
        source: std::num::ParseIntError,
        location: Location,
    },

    #[snafu(display("Invalid timestamp index: {}", index))]
    InvalidTimestampIndex { index: usize, location: Location },

    #[snafu(display("{}", msg))]
    CastType { msg: String, location: Location },

    #[snafu(display("Failed to cast arrow time i32 type into i64"))]
    CastTimeType {
        source: std::num::TryFromIntError,
        location: Location,
    },

    #[snafu(display("Arrow failed to compute"))]
    ArrowCompute {
        source: arrow::error::ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to project arrow schema"))]
    ProjectArrowSchema {
        source: arrow::error::ArrowError,
        location: Location,
    },

    #[snafu(display("Unsupported column default constraint expression: {}", expr))]
    UnsupportedDefaultExpr { expr: String, location: Location },

    #[snafu(display("Default value should not be null for non null column"))]
    NullDefault { location: Location },

    #[snafu(display("Incompatible default value type, reason: {}", reason))]
    DefaultValueType { reason: String, location: Location },

    #[snafu(display("Duplicated metadata for {}", key))]
    DuplicateMeta { key: String, location: Location },

    #[snafu(display("Failed to convert value into scalar value, reason: {}", reason))]
    ToScalarValue { reason: String, location: Location },

    #[snafu(display("Invalid timestamp precision: {}", precision))]
    InvalidTimestampPrecision { precision: u64, location: Location },

    #[snafu(display("Column {} already exists", column))]
    DuplicateColumn { column: String, location: Location },

    #[snafu(display("Failed to unpack value to given type: {}", reason))]
    TryFromValue { reason: String, location: Location },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        // Inner encoding and decoding error should not be exposed to users.
        StatusCode::Internal
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
