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
use datatypes::timestamp::TimestampNanosecond;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Pipeline table not found"))]
    PipelineTableNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to insert pipeline to pipelines table"))]
    InsertPipeline {
        #[snafu(source)]
        source: operator::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse pipeline"))]
    CompilePipeline {
        #[snafu(source)]
        source: crate::etl::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Pipeline not found, name: {}, version: {}", name, version.map(|ts| ts.0.to_iso8601_string()).unwrap_or("latest".to_string())))]
    PipelineNotFound {
        name: String,
        version: Option<TimestampNanosecond>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to collect record batch"))]
    CollectRecords {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to cast type, msg: {}", msg))]
    CastType {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build DataFusion logical plan"))]
    BuildDfLogicalPlan {
        #[snafu(source)]
        error: datafusion_common::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute internal statement"))]
    ExecuteInternalStatement {
        #[snafu(source)]
        source: query::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create dataframe"))]
    DataFrame {
        #[snafu(source)]
        source: query::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("General catalog error"))]
    Catalog {
        #[snafu(source)]
        source: catalog::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create table"))]
    CreateTable {
        #[snafu(source)]
        source: operator::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute pipeline"))]
    PipelineTransform {
        #[snafu(source)]
        source: crate::etl::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid pipeline version format: {}", version))]
    InvalidPipelineVersion {
        version: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            CastType { .. } => StatusCode::Unexpected,
            PipelineTableNotFound { .. } => StatusCode::TableNotFound,
            InsertPipeline { source, .. } => source.status_code(),
            CollectRecords { source, .. } => source.status_code(),
            PipelineNotFound { .. }
            | CompilePipeline { .. }
            | PipelineTransform { .. }
            | InvalidPipelineVersion { .. } => StatusCode::InvalidArguments,
            BuildDfLogicalPlan { .. } => StatusCode::Internal,
            ExecuteInternalStatement { source, .. } => source.status_code(),
            DataFrame { source, .. } => source.status_code(),
            Catalog { source, .. } => source.status_code(),
            CreateTable { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
