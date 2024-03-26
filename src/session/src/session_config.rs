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

use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use sql::ast::Value;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Invalid value for parameter \"{}\": {}\nHint: {}", name, value, hint,))]
    InvalidConfigValue {
        name: String,
        value: String,
        hint: String,
        location: Location,
    },
}

#[derive(Clone, Copy, Debug, Default)]
pub enum PGByteaOutputValue {
    #[default]
    HEX,
    ESCAPE,
}

impl TryFrom<Value> for PGByteaOutputValue {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match &value {
            Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => {
                match s.to_uppercase().as_str() {
                    "ESCAPE" => Ok(PGByteaOutputValue::ESCAPE),
                    "HEX" => Ok(PGByteaOutputValue::HEX),
                    _ => InvalidConfigValueSnafu {
                        name: "BYTEA_OUTPUT",
                        value: value.to_string(),
                        hint: "Available values: escape, hex",
                    }
                    .fail(),
                }
            }
            _ => InvalidConfigValueSnafu {
                name: "BYTEA_OUTPUT",
                value: value.to_string(),
                hint: "Available values: escape, hex",
            }
            .fail(),
        }
    }
}
