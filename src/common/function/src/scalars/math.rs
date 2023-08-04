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

mod pow;
mod rate;

use std::fmt;
use std::sync::Arc;

use common_query::error::{GeneralDataFusionSnafu, Result};
use common_query::prelude::Signature;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::VectorRef;
pub use pow::PowFunction;
pub use rate::RateFunction;
use snafu::ResultExt;

use super::function::FunctionContext;
use super::Function;
use crate::scalars::function_registry::FunctionRegistry;

pub(crate) struct MathFunction;

impl MathFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(PowFunction::default()));
        registry.register(Arc::new(RateFunction::default()));
        registry.register(Arc::new(RangeFunction::default()))
    }
}

/// `RangeFunction` will never be used as a normal function,
/// just for datafusion to generate logical plan for RangeSelect
#[derive(Clone, Debug, Default)]
struct RangeFunction;

impl fmt::Display for RangeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RANGE_FN")
    }
}

impl Function for RangeFunction {
    fn name(&self) -> &str {
        "range_fn"
    }

    // range_fn will never been used, return_type could be arbitrary value, is not important
    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    /// `range_fn` will never been used, signature is not important.
    /// In fact, the arguments loaded by `range_fn` are very complicated, and it is difficult to use `Signature` to describe
    fn signature(&self) -> Signature {
        Signature::any(6, Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        Err(DataFusionError::Internal(
            "range_fn just a empty function used in range select, It should not be eval!".into(),
        ))
        .context(GeneralDataFusionSnafu)
    }
}
