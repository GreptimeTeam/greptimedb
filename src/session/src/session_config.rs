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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum SessionConfigOption {
    Mysql,
    Postgres(PostgresOption),
}
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unkonw Postgres option '{}'", name))]
    UnknownOption { name: String, location: Location },
    #[snafu(display("Invalid value: {} for '{}'", value, name))]
    InvalidConfigValue {
        name: String,
        value: String,
        location: Location,
    },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum PostgresOption {
    ByteaOutput,
}

impl Display for PostgresOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresOption::ByteaOutput => write!(f, "BYTEA_OUTPUT"),
        }
    }
}

impl TryFrom<&str> for PostgresOption {
    type Error = Error;
    fn try_from(name: &str) -> Result<Self, Self::Error> {
        match name.to_uppercase().as_str() {
            "BYTEA_OUTPUT" => Ok(PostgresOption::ByteaOutput),
            _ => UnknownOptionSnafu { name }.fail(),
        }
    }
}

pub fn postgres_option(option: PostgresOption) -> SessionConfigOption {
    SessionConfigOption::Postgres(option)
}

#[derive(Debug, Clone)]
pub enum SessionConfigValue {
    MySql,
    Postgres(PostgresConfigValue),
}

pub fn postgres_config_value(value: PostgresConfigValue) -> SessionConfigValue {
    SessionConfigValue::Postgres(value)
}

#[derive(Debug, Clone)]
pub enum PostgresConfigValue {
    ByteaOutput(ByteaOutputValue),
}

#[derive(Clone, Copy, Debug)]
pub enum ByteaOutputValue {
    DEFAULT,
    HEX,
    ESCAPE,
}
impl TryFrom<Value> for ByteaOutputValue {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match &value {
            Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => {
                match s.to_uppercase().as_str() {
                    "DEFAULT" => Ok(ByteaOutputValue::DEFAULT),
                    "ESCAPE" => Ok(ByteaOutputValue::ESCAPE),
                    "HEX" => Ok(ByteaOutputValue::HEX),
                    _ => InvalidConfigValueSnafu {
                        name: "BYTEA_OUTPUT",
                        value: value.to_string(),
                    }
                    .fail(),
                }
            }
            _ => InvalidConfigValueSnafu {
                name: "BYTEA_OUTPUT",
                value: value.to_string(),
            }
            .fail(),
        }
    }
}

pub fn try_into_postgres_config(
    name: &str,
    value: &Value,
) -> Result<(SessionConfigOption, SessionConfigValue), Error> {
    let option = PostgresOption::try_from(name)?;
    let value = match option {
        PostgresOption::ByteaOutput => ByteaOutputValue::try_from(value.clone())?,
    };
    Ok((
        postgres_option(option),
        postgres_config_value(PostgresConfigValue::ByteaOutput(value)),
    ))
}

pub fn try_into_mysql_config(
    _name: &str,
    _value: &Value,
) -> Result<(SessionConfigOption, SessionConfigValue), Error> {
    // place holder
    Ok((SessionConfigOption::Mysql, SessionConfigValue::MySql))
}
