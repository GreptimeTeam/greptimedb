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

use crate::function_registry::FunctionRegistry;

pub(crate) mod hll;
mod uddsketch;

pub(crate) struct ApproximateFunction;

impl ApproximateFunction {
    pub fn register(registry: &FunctionRegistry) {
        // uddsketch
        registry.register_aggr(uddsketch::UddSketchState::state_udf_impl());
        registry.register_aggr(uddsketch::UddSketchState::merge_udf_impl());

        // hll
        registry.register_aggr(hll::HllState::state_udf_impl());
        registry.register_aggr(hll::HllState::merge_udf_impl());
    }
}
