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
use datafusion_common::ScalarValue;
use datatypes::arrow;
use datatypes::prelude::Value;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::expr::{Operand, PartitionExpr};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to find table route: {}", table_id))]
    TableRouteNotFound {
        table_id: TableId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table route manager error"))]
    TableRouteManager {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get partition info"))]
    GetPartitionInfo {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get meta info from cache, error: {}", err_msg))]
    GetCache {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to find region routes for table {}, region id: {}",
        table_id,
        region_id
    ))]
    FindRegionRoutes {
        table_id: TableId,
        region_id: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize value to json"))]
    SerializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize value from json"))]
    DeserializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expect {} region keys, actual {}", expect, actual))]
    RegionKeysSize {
        expect: usize,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid DeleteRequest, reason: {}", reason))]
    InvalidDeleteRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert DataFusion's ScalarValue: {:?}", value))]
    ConvertScalarValue {
        value: ScalarValue,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to find leader of table id {} region {}", table_id, region_id))]
    FindLeader {
        table_id: TableId,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected table route type: {}", err_msg))]
    UnexpectedLogicalRouteTable {
        #[snafu(implicit)]
        location: Location,
        err_msg: String,
        source: common_meta::error::Error,
    },

    #[snafu(display("Invalid partition expr: {:?}", expr))]
    InvalidExpr {
        expr: PartitionExpr,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected operand: {:?}, want Expr", operand))]
    NoExprOperand {
        operand: Operand,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Undefined column: {}", column))]
    UndefinedColumn {
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected: {err_msg}"))]
    Unexpected {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert to vector"))]
    ConvertToVector {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to evaluate record batch"))]
    EvaluateRecordBatch {
        #[snafu(source)]
        error: datafusion_common::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to compute arrow kernel"))]
    ComputeArrowKernel {
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected evaluation result column type: {}", data_type))]
    UnexpectedColumnType {
        data_type: arrow::datatypes::DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert to DataFusion's Schema"))]
    ToDFSchema {
        #[snafu(source)]
        error: datafusion_common::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create physical expression"))]
    CreatePhysicalExpr {
        #[snafu(source)]
        error: datafusion_common::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Partition expr value is not supported: {:?}", value))]
    UnsupportedPartitionExprValue {
        value: Value,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Duplicate expr: {:?}", expr))]
    DuplicateExpr {
        expr: PartitionExpr,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Checkpoint `{}` is not covered", checkpoint))]
    CheckpointNotCovered {
        checkpoint: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Checkpoint `{}` is overlapped", checkpoint))]
    CheckpointOverlapped {
        checkpoint: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::GetCache { .. } => StatusCode::StorageUnavailable,
            Error::FindLeader { .. } => StatusCode::TableUnavailable,

            Error::InvalidExpr { .. }
            | Error::NoExprOperand { .. }
            | Error::UndefinedColumn { .. }
            | Error::DuplicateExpr { .. }
            | Error::CheckpointNotCovered { .. }
            | Error::CheckpointOverlapped { .. } => StatusCode::InvalidArguments,

            Error::RegionKeysSize { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::InvalidDeleteRequest { .. } => StatusCode::InvalidArguments,

            Error::ConvertScalarValue { .. }
            | Error::SerializeJson { .. }
            | Error::DeserializeJson { .. } => StatusCode::Internal,

            Error::Unexpected { .. } | Error::FindRegionRoutes { .. } => StatusCode::Unexpected,
            Error::TableRouteNotFound { .. } => StatusCode::TableNotFound,
            Error::TableRouteManager { source, .. } => source.status_code(),
            Error::GetPartitionInfo { source, .. } => source.status_code(),
            Error::UnexpectedLogicalRouteTable { source, .. } => source.status_code(),
            Error::ConvertToVector { source, .. } => source.status_code(),
            Error::EvaluateRecordBatch { .. } => StatusCode::Internal,
            Error::ComputeArrowKernel { .. } => StatusCode::Internal,
            Error::UnexpectedColumnType { .. } => StatusCode::Internal,
            Error::ToDFSchema { .. } => StatusCode::Internal,
            Error::CreatePhysicalExpr { .. } => StatusCode::Internal,
            Error::UnsupportedPartitionExprValue { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
