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

use std::sync::Arc;
mod geohash;
mod h3;

use geohash::GeohashFunction;
use h3::H3Function;

use crate::function_registry::FunctionRegistry;

pub(crate) struct GeoFunctions;

impl GeoFunctions {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(GeohashFunction));
        registry.register(Arc::new(H3Function));
    }
}
