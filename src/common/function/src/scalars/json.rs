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
mod get_by_path;
mod json_to_string;
mod to_json;

use get_by_path::{GetByPathBool, GetByPathFloat, GetByPathInt, GetByPathString};
use json_to_string::JsonToStringFunction;
use to_json::ToJsonFunction;

use crate::function_registry::FunctionRegistry;

pub(crate) struct JsonFunction;

impl JsonFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(JsonToStringFunction));
        registry.register(Arc::new(ToJsonFunction));

        registry.register(Arc::new(GetByPathInt));
        registry.register(Arc::new(GetByPathFloat));
        registry.register(Arc::new(GetByPathString));
        registry.register(Arc::new(GetByPathBool));
    }
}
