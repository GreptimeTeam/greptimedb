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

mod binary;
mod ctx;
mod if_func;
mod is_null;
mod unary;

pub use binary::scalar_binary_op;
pub use ctx::EvalContext;
pub use unary::scalar_unary_op;

use crate::function_registry::FunctionRegistry;
use crate::scalars::expression::if_func::IfFunction;
use crate::scalars::expression::is_null::IsNullFunction;

pub(crate) struct ExpressionFunction;

impl ExpressionFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(IsNullFunction::default());
        registry.register_scalar(IfFunction::default());
    }
}
