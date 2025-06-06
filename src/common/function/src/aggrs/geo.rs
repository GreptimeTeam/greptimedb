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

mod encoding;
mod geo_path;

pub(crate) struct GeoFunction;

impl GeoFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_aggr(geo_path::GeoPathAccumulator::uadf_impl());
        registry.register_aggr(encoding::JsonPathAccumulator::uadf_impl());
    }
}
