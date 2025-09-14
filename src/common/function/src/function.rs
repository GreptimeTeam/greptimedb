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

use std::fmt;
use std::sync::Arc;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::{ExecuteSnafu, Result};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature};
use datatypes::vectors::VectorRef;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::ResultExt;

use crate::state::FunctionState;

/// The function execution context
#[derive(Clone)]
pub struct FunctionContext {
    pub query_ctx: QueryContextRef,
    pub state: Arc<FunctionState>,
}

impl FunctionContext {
    /// Create a mock [`FunctionContext`] for test.
    #[cfg(any(test, feature = "testing"))]
    pub fn mock() -> Self {
        Self {
            query_ctx: QueryContextBuilder::default().build().into(),
            state: Arc::new(FunctionState::mock()),
        }
    }
}

impl std::fmt::Display for FunctionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FunctionContext {{ query_ctx: {} }}", self.query_ctx)
    }
}

impl Default for FunctionContext {
    fn default() -> Self {
        Self {
            query_ctx: QueryContextBuilder::default().build().into(),
            state: Arc::new(FunctionState::default()),
        }
    }
}

/// Scalar function trait, modified from databend to adapt datafusion
/// TODO(dennis): optimize function by it's features such as monotonicity etc.
pub trait Function: fmt::Display + Sync + Send {
    /// Returns the name of the function, should be unique.
    fn name(&self) -> &str;

    /// The returned data type of function execution.
    fn return_type(&self, input_types: &[DataType]) -> Result<DataType>;

    /// The signature of function.
    fn signature(&self) -> Signature;

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        // TODO(LFC): Remove default implementation once all UDFs have implemented this function.
        let _ = args;
        Err(datafusion_common::DataFusionError::NotImplemented(
            "invoke_with_args".to_string(),
        ))
    }

    /// Evaluate the function, e.g. run/execute the function.
    /// TODO(LFC): Remove `eval` when all UDFs are rewritten to `invoke_with_args`
    fn eval(&self, _: &FunctionContext, _: &[VectorRef]) -> Result<VectorRef> {
        Err(BoxedError::new(PlainError::new(
            "unsupported".to_string(),
            StatusCode::Unsupported,
        )))
        .context(ExecuteSnafu)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

pub type FunctionRef = Arc<dyn Function>;
