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
mod rate;

use std::fmt;

pub use clamp::{ClampFunction, ClampMaxFunction, ClampMinFunction};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::internal_err;
use datafusion_expr::{ScalarFunctionArgs, Signature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;
use crate::scalars::math::modulo::ModuloFunction;
use crate::scalars::math::rate::RateFunction;

pub(crate) struct MathFunction;

impl MathFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(ModuloFunction::default());
        registry.register_scalar(RateFunction::default());
        registry.register_scalar(RangeFunction::default());
        registry.register_scalar(ClampFunction::default());
        registry.register_scalar(ClampMinFunction::default());
        registry.register_scalar(ClampMaxFunction::default());
    }
}

/// `RangeFunction` will never be used as a normal function,
/// just for datafusion to generate logical plan for RangeSelect
#[derive(Clone, Debug)]
struct RangeFunction {
    signature: Signature,
}

impl fmt::Display for RangeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RANGE_FN")
    }
}

impl Default for RangeFunction {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Function for RangeFunction {
    fn name(&self) -> &str {
        "range_fn"
    }

    // The first argument to range_fn is the expression to be evaluated
    fn return_type(&self, input_types: &[DataType]) -> datafusion_common::Result<DataType> {
        input_types
            .first()
            .cloned()
            .ok_or(DataFusionError::Internal(
                "No expr found in range_fn".into(),
            ))
    }

    /// `range_fn` will never been used. As long as a legal signature is returned, the specific content of the signature does not matter.
    /// In fact, the arguments loaded by `range_fn` are very complicated, and it is difficult to use `Signature` to describe
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> datafusion_common::Result<ColumnarValue> {
        internal_err!("not expected to invoke 'range_fn' directly")
    }
}
