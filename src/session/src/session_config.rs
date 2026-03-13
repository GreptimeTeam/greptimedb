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

use std::fmt::Display;

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
        #[snafu(implicit)]
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

impl Display for PGByteaOutputValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PGByteaOutputValue::HEX => write!(f, "hex"),
            PGByteaOutputValue::ESCAPE => write!(f, "escape"),
        }
    }
}

// Refers to: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE
#[derive(Default, PartialEq, Eq, Clone, Copy, Debug)]
pub enum PGDateOrder {
    #[default]
    MDY,
    DMY,
    YMD,
}

impl Display for PGDateOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PGDateOrder::MDY => write!(f, "MDY"),
            PGDateOrder::DMY => write!(f, "DMY"),
            PGDateOrder::YMD => write!(f, "YMD"),
        }
    }
}

impl TryFrom<&str> for PGDateOrder {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_uppercase().as_str() {
            "US" | "NONEURO" | "NONEUROPEAN" | "MDY" => Ok(PGDateOrder::MDY),
            "EUROPEAN" | "EURO" | "DMY" => Ok(PGDateOrder::DMY),
            "YMD" => Ok(PGDateOrder::YMD),
            _ => InvalidConfigValueSnafu {
                name: "DateStyle",
                value: s,
                hint: format!("Unrecognized key word: {}", s),
            }
            .fail(),
        }
    }
}
impl TryFrom<&Value> for PGDateOrder {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => {
                Self::try_from(s.as_str())
            }
            _ => InvalidConfigValueSnafu {
                name: "DateStyle",
                value: value.to_string(),
                hint: format!("Unrecognized key word: {}", value),
            }
            .fail(),
        }
    }
}

#[derive(Default, PartialEq, Eq, Clone, Copy, Debug)]
pub enum PGDateTimeStyle {
    #[default]
    ISO,
    SQL,
    Postgres,
    German,
}

impl Display for PGDateTimeStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PGDateTimeStyle::ISO => write!(f, "ISO"),
            PGDateTimeStyle::SQL => write!(f, "SQL"),
            PGDateTimeStyle::Postgres => write!(f, "Postgres"),
            PGDateTimeStyle::German => write!(f, "German"),
        }
    }
}

impl TryFrom<&str> for PGDateTimeStyle {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_uppercase().as_str() {
            "ISO" => Ok(PGDateTimeStyle::ISO),
            "SQL" => Ok(PGDateTimeStyle::SQL),
            "POSTGRES" => Ok(PGDateTimeStyle::Postgres),
            "GERMAN" => Ok(PGDateTimeStyle::German),
            _ => InvalidConfigValueSnafu {
                name: "DateStyle",
                value: s,
                hint: format!("Unrecognized key word: {}", s),
            }
            .fail(),
        }
    }
}

impl TryFrom<&Value> for PGDateTimeStyle {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => {
                Self::try_from(s.as_str())
            }
            _ => InvalidConfigValueSnafu {
                name: "DateStyle",
                value: value.to_string(),
                hint: format!("Unrecognized key word: {}", value),
            }
            .fail(),
        }
    }
}

#[derive(Default, PartialEq, Eq, Clone, Copy, Debug)]
pub enum PGIntervalStyle {
    ISO,
    SQL,
    #[default]
    Postgres,
    PostgresVerbose,
}

impl Display for PGIntervalStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PGIntervalStyle::ISO => write!(f, "iso_8601"),
            PGIntervalStyle::SQL => write!(f, "sql_standard"),
            PGIntervalStyle::Postgres => write!(f, "postgres"),
            PGIntervalStyle::PostgresVerbose => write!(f, "postgres_verbose"),
        }
    }
}

impl TryFrom<&str> for PGIntervalStyle {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_uppercase().as_str() {
            "ISO" | "ISO_8601" => Ok(PGIntervalStyle::ISO),
            "SQL" | "SQL_STANDARD" => Ok(PGIntervalStyle::SQL),
            "POSTGRES" => Ok(PGIntervalStyle::Postgres),
            "POSTGRES_VERBOSE" | "POSTGRES, VERBOSE" => Ok(PGIntervalStyle::PostgresVerbose),
            _ => InvalidConfigValueSnafu {
                name: "IntervalStyle",
                value: s,
                hint: format!("Unrecognized key word: {}", s),
            }
            .fail(),
        }
    }
}

impl TryFrom<&Value> for PGIntervalStyle {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => {
                Self::try_from(s.as_str())
            }
            _ => InvalidConfigValueSnafu {
                name: "IntervalStyle",
                value: value.to_string(),
                hint: format!("Unrecognized key word: {}", value),
            }
            .fail(),
        }
    }
}
