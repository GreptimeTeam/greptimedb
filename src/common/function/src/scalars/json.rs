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

pub mod json_get;
mod json_is;
mod json_path_exists;
mod json_path_match;
mod json_to_string;
mod parse_json;

use json_get::{JsonGetBool, JsonGetFloat, JsonGetInt, JsonGetString};
use json_is::{
    JsonIsArray, JsonIsBool, JsonIsFloat, JsonIsInt, JsonIsNull, JsonIsObject, JsonIsString,
};
use json_to_string::JsonToStringFunction;
use parse_json::ParseJsonFunction;

use crate::function_registry::FunctionRegistry;

pub(crate) struct JsonFunction;

impl JsonFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(JsonToStringFunction);
        registry.register_scalar(ParseJsonFunction);

        registry.register_scalar(JsonGetInt);
        registry.register_scalar(JsonGetFloat);
        registry.register_scalar(JsonGetString);
        registry.register_scalar(JsonGetBool);

        registry.register_scalar(JsonIsNull);
        registry.register_scalar(JsonIsInt);
        registry.register_scalar(JsonIsFloat);
        registry.register_scalar(JsonIsString);
        registry.register_scalar(JsonIsBool);
        registry.register_scalar(JsonIsArray);
        registry.register_scalar(JsonIsObject);

        registry.register_scalar(json_path_exists::JsonPathExistsFunction);
        registry.register_scalar(json_path_match::JsonPathMatchFunction);
    }
}
