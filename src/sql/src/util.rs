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

use std::collections::HashMap;

use sqlparser::ast::{SqlOption, Value};

pub fn parse_option_string(value: Value) -> Option<String> {
    match value {
        Value::SingleQuotedString(v) | Value::DoubleQuotedString(v) => Some(v),
        _ => None,
    }
}

/// Converts options to HashMap<String, String>.
/// All keys are lowercase.
pub fn to_lowercase_options_map(opts: &[SqlOption]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(opts.len());
    for SqlOption { name, value } in opts {
        let value_str = match value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            _ => value.to_string(),
        };
        let _ = map.insert(name.value.to_lowercase().clone(), value_str);
    }
    map
}
