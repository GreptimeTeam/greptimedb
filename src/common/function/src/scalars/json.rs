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

use json_get::{JsonGetBool, JsonGetFloat, JsonGetInt, JsonGetObject, JsonGetString};
use json_is::{
    JsonIsArray, JsonIsBool, JsonIsFloat, JsonIsInt, JsonIsNull, JsonIsObject, JsonIsString,
};
use json_to_string::JsonToStringFunction;
use parse_json::ParseJsonFunction;

use crate::function_registry::FunctionRegistry;
use crate::scalars::json::json_get::JsonGetWithType;

pub(crate) struct JsonFunction;

impl JsonFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(JsonToStringFunction::default());
        registry.register_scalar(ParseJsonFunction::default());

        registry.register_scalar(JsonGetInt::default());
        registry.register_scalar(JsonGetFloat::default());
        registry.register_scalar(JsonGetString::default());
        registry.register_scalar(JsonGetBool::default());
        registry.register_scalar(JsonGetObject::default());
        registry.register_scalar(JsonGetWithType::default());

        registry.register_scalar(JsonIsNull::default());
        registry.register_scalar(JsonIsInt::default());
        registry.register_scalar(JsonIsFloat::default());
        registry.register_scalar(JsonIsString::default());
        registry.register_scalar(JsonIsBool::default());
        registry.register_scalar(JsonIsArray::default());
        registry.register_scalar(JsonIsObject::default());

        registry.register_scalar(json_path_exists::JsonPathExistsFunction::default());
        registry.register_scalar(json_path_match::JsonPathMatchFunction::default());
    }
}
