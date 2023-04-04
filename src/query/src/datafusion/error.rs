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

use common_error::prelude::*;
use datafusion::error::DataFusionError;
use snafu::Location;

/// Inner error of datafusion based query engine.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("{}: {}", msg, source))]
    Datafusion {
        msg: &'static str,
        source: DataFusionError,
        location: Location,
    },

    #[snafu(display("PhysicalPlan downcast failed"))]
    PhysicalPlanDowncast { location: Location },

    #[snafu(display("Fail to convert arrow schema, source: {}", source))]
    ConvertSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert table schema, source: {}", source))]
    TableSchemaMismatch {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display(
        "Failed to convert DataFusion's recordbatch stream, source: {}",
        source
    ))]
    ConvertDfRecordBatchStream {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to execute physical plan, source: {}", source))]
    ExecutePhysicalPlan {
        #[snafu(backtrace)]
        source: common_query::error::Error,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        use InnerError::*;

        match self {
            // TODO(yingwen): Further categorize datafusion error.
            Datafusion { .. } => StatusCode::EngineExecuteQuery,
            // This downcast should not fail in usual case.
            PhysicalPlanDowncast { .. } | ConvertSchema { .. } | TableSchemaMismatch { .. } => {
                StatusCode::Unexpected
            }
            ConvertDfRecordBatchStream { source } => source.status_code(),
            ExecutePhysicalPlan { source } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
