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

use common_query::error::Result;
use common_query::prelude::Signature;
use datatypes::data_type::ConcreteDataType;
use datatypes::vectors::VectorRef;
use session::context::{QueryContextBuilder, QueryContextRef};

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
    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType>;

    /// The signature of function.
    fn signature(&self) -> Signature;

    /// Evaluate the function, e.g. run/execute the function.
    fn eval(&self, ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef>;
}

pub type FunctionRef = Arc<dyn Function>;

/// Async Scalar function trait
#[async_trait::async_trait]
pub trait AsyncFunction: fmt::Display + Sync + Send {
    /// Returns the name of the function, should be unique.
    fn name(&self) -> &str;

    /// The returned data type of function execution.
    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType>;

    /// The signature of function.
    fn signature(&self) -> Signature;

    /// Evaluate the function, e.g. run/execute the function.
    /// TODO(dennis): simplify the signature and refactor all the admin functions.
    async fn eval(&self, _func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef>;
}

pub type AsyncFunctionRef = Arc<dyn AsyncFunction>;
