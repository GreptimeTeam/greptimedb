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
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to serialize data"))]
    Serialize {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize data, json: {}", json))]
    Deserialize {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
        json: String,
    },

    #[snafu(display("Failed to convert datafusion type: {}", from))]
    Conversion {
        from: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Bad array access, Index out of bounds: {}, size: {}", index, size))]
    BadArrayAccess {
        index: usize,
        size: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unknown vector, {}", msg))]
    UnknownVector {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported arrow data type, type: {:?}", arrow_type))]
    UnsupportedArrowType {
        arrow_type: arrow::datatypes::DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported operation: {} for vector: {}", op, vector_type))]
    UnsupportedOperation {
        op: String,
        vector_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse version in schema meta, value: {}", value))]
    ParseSchemaVersion {
        value: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid timestamp index: {}", index))]
    InvalidTimestampIndex {
        index: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{}", msg))]
    CastType {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast arrow time i32 type into i64"))]
    CastTimeType {
        #[snafu(source)]
        error: std::num::TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Arrow failed to compute"))]
    ArrowCompute {
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to project arrow schema"))]
    ProjectArrowSchema {
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported column default constraint expression: {}", expr))]
    UnsupportedDefaultExpr {
        expr: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Default value should not be null for non null column"))]
    NullDefault {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Incompatible default value type, reason: {}", reason))]
    DefaultValueType {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Duplicated metadata for {}", key))]
    DuplicateMeta {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert value into scalar value, reason: {}", reason))]
    ToScalarValue {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid timestamp precision: {}", precision))]
    InvalidTimestampPrecision {
        precision: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Column {} already exists", column))]
    DuplicateColumn {
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to unpack value to given type: {}", reason))]
    TryFromValue {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to specify the precision {} and scale {}", precision, scale))]
    InvalidPrecisionOrScale {
        precision: u8,
        scale: i8,
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid JSON: {}", value))]
    InvalidJson {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid Vector: {}", msg))]
    InvalidVector {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Value exceeds the precision {} bound", precision))]
    ValueExceedsPrecision {
        precision: u8,
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert Arrow array to scalars"))]
    ConvertArrowArrayToScalars {
        #[snafu(source)]
        error: datafusion_common::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert scalar value to Arrow array"))]
    ConvertScalarToArrowArray {
        #[snafu(source)]
        error: datafusion_common::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse extended type in metadata: {}", value))]
    ParseExtendedType {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid fulltext option: {}", msg))]
    InvalidFulltextOption {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid skipping index option: {}", msg))]
    InvalidSkippingIndexOption {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Inconsistent struct field count {field_len} and item count {item_len}"))]
    InconsistentStructFieldsAndItems {
        field_len: usize,
        item_len: usize,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to process JSONB value"))]
    InvalidJsonb {
        error: jsonb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to merge JSON datatype: {reason}"))]
    MergeJsonDatatype {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse or serialize arrow metadata"))]
    ArrowMetadata {
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            UnsupportedOperation { .. }
            | UnsupportedArrowType { .. }
            | UnsupportedDefaultExpr { .. } => StatusCode::Unsupported,

            DuplicateColumn { .. }
            | BadArrayAccess { .. }
            | NullDefault { .. }
            | InvalidTimestampIndex { .. }
            | DefaultValueType { .. }
            | DuplicateMeta { .. }
            | InvalidTimestampPrecision { .. }
            | InvalidPrecisionOrScale { .. }
            | InvalidJson { .. }
            | InvalidJsonb { .. }
            | InvalidVector { .. }
            | InvalidFulltextOption { .. }
            | InvalidSkippingIndexOption { .. }
            | MergeJsonDatatype { .. } => StatusCode::InvalidArguments,

            ValueExceedsPrecision { .. }
            | CastType { .. }
            | CastTimeType { .. }
            | Conversion { .. } => StatusCode::IllegalState,

            Serialize { .. }
            | Deserialize { .. }
            | UnknownVector { .. }
            | ParseSchemaVersion { .. }
            | ArrowCompute { .. }
            | ProjectArrowSchema { .. }
            | ToScalarValue { .. }
            | TryFromValue { .. }
            | ConvertArrowArrayToScalars { .. }
            | ConvertScalarToArrowArray { .. }
            | ParseExtendedType { .. }
            | InconsistentStructFieldsAndItems { .. }
            | ArrowMetadata { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
