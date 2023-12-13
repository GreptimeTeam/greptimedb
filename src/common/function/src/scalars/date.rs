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
mod date_add;
mod date_sub;

use date_add::DateAddFunction;
use date_sub::DateSubFunction;

use crate::function_registry::FunctionRegistry;

pub(crate) struct DateFunction;

impl DateFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(DateAddFunction));
        registry.register(Arc::new(DateSubFunction));
    }
}
