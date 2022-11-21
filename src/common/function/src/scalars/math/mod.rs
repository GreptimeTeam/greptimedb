// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod pow;

use std::sync::Arc;

pub use pow::PowFunction;

use crate::scalars::function_registry::FunctionRegistry;

pub(crate) struct MathFunction;

impl MathFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(PowFunction::default()));
    }
}
