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

pub mod clamp;
mod modulo;
mod pow;
mod rate;

use std::fmt;
use std::sync::Arc;

pub use clamp::{ClampFunction, ClampMaxFunction, ClampMinFunction};
use common_query::error::{GeneralDataFusionSnafu, Result};
use common_query::prelude::Signature;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::VectorRef;
pub use pow::PowFunction;
pub use rate::RateFunction;
use snafu::ResultExt;

use crate::function::{Function, FunctionContext};
use crate::function_registry::FunctionRegistry;
use crate::scalars::math::modulo::ModuloFunction;

pub(crate) struct MathFunction;

impl MathFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(ModuloFunction));
        registry.register(Arc::new(PowFunction));
        registry.register(Arc::new(RateFunction));
        registry.register(Arc::new(RangeFunction));
        registry.register(Arc::new(ClampFunction));
        registry.register(Arc::new(ClampMinFunction));
        registry.register(Arc::new(ClampMaxFunction));
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

    // The first argument to range_fn is the expression to be evaluated
    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        input_types
            .first()
            .cloned()
            .ok_or(DataFusionError::Internal(
                "No expr found in range_fn".into(),
            ))
            .context(GeneralDataFusionSnafu)
    }

    /// `range_fn` will never been used. As long as a legal signature is returned, the specific content of the signature does not matter.
    /// In fact, the arguments loaded by `range_fn` are very complicated, and it is difficult to use `Signature` to describe
    fn signature(&self) -> Signature {
        Signature::variadic_any(Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        Err(DataFusionError::Internal(
            "range_fn just a empty function used in range select, It should not be eval!".into(),
        ))
        .context(GeneralDataFusionSnafu)
    }
}
