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

use crate::aggrs::vector::avg::VectorAvg;
use crate::aggrs::vector::product::VectorProduct;
use crate::aggrs::vector::sum::VectorSum;
use crate::function_registry::FunctionRegistry;

mod avg;
mod product;
mod sum;

pub(crate) struct VectorFunction;

impl VectorFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_aggr(VectorSum::uadf_impl());
        registry.register_aggr(VectorProduct::uadf_impl());
        registry.register_aggr(VectorAvg::uadf_impl());
    }
}
