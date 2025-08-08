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

use std::str::FromStr;
use std::time::Duration;

use common_time::Timezone;
use lazy_static::lazy_static;
use regex::Regex;
use session::context::Channel::Postgres;
use session::context::QueryContextRef;
use session::session_config::{PGByteaOutputValue, PGDateOrder, PGDateTimeStyle};
use session::ReadPreference;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{Expr, Ident, Value};
use sql::statements::set_variables::SetVariables;
use sqlparser::ast::ValueWithSpan;

use crate::error::{InvalidConfigValueSnafu, InvalidSqlSnafu, NotSupportedSnafu, Result};

lazy_static! {
    // Regex rules:
    // The string must start with a number (one or more digits).
    // The number must be followed by one of the valid time units (ms, s, min, h, d).
    // The string must end immediately after the unit, meaning there can be no extra
    // characters or spaces after the valid time specification.
    static ref PG_TIME_INPUT_REGEX: Regex = Regex::new(r"^(\d+)(ms|s|min|h|d)$").unwrap();
}

pub fn set_read_preference(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let read_preference_expr = exprs.first().context(NotSupportedSnafu {
        feat: "No read preference find in set variable statement",
    })?;

    match read_preference_expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(expr),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(expr),
            ..
        }) => {
            match ReadPreference::from_str(expr.as_str().to_lowercase().as_str()) {
                Ok(read_preference) => ctx.set_read_preference(read_preference),
                Err(_) => {
                    return NotSupportedSnafu {
                        feat: format!(
                            "Invalid read preference expr {} in set variable statement",
                            expr,
                        ),
                    }
                    .fail()
                }
            }
            Ok(())
        }
        expr => NotSupportedSnafu {
            feat: format!(
                "Unsupported read preference expr {} in set variable statement",
                expr
            ),
        }
        .fail(),
    }
}

pub fn set_timezone(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let tz_expr = exprs.first().context(NotSupportedSnafu {
        feat: "No timezone find in set variable statement",
    })?;
    match tz_expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(tz),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(tz),
            ..
        }) => {
            match Timezone::from_tz_string(tz.as_str()) {
                Ok(timezone) => ctx.set_timezone(timezone),
                Err(_) => {
                    return NotSupportedSnafu {
                        feat: format!("Invalid timezone expr {} in set variable statement", tz),
                    }
                    .fail()
                }
            }
            Ok(())
        }
        expr => NotSupportedSnafu {
            feat: format!(
                "Unsupported timezone expr {} in set variable statement",
                expr
            ),
        }
        .fail(),
    }
}

pub fn set_bytea_output(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let Some((var_value, [])) = exprs.split_first() else {
        return (NotSupportedSnafu {
            feat: "Set variable value must have one and only one value for bytea_output",
        })
        .fail();
    };
    let Expr::Value(value) = var_value else {
        return (NotSupportedSnafu {
            feat: "Set variable value must be a value",
        })
        .fail();
    };
    ctx.configuration_parameter().set_postgres_bytea_output(
        PGByteaOutputValue::try_from(value.value.clone()).context(InvalidConfigValueSnafu)?,
    );
    Ok(())
}

pub fn set_search_path(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let search_expr = exprs.first().context(NotSupportedSnafu {
        feat: "No search path find in set variable statement",
    })?;
    match search_expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(search_path),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(search_path),
            ..
        }) => {
            ctx.set_current_schema(search_path);
            Ok(())
        }
        Expr::Identifier(Ident { value, .. }) => {
            ctx.set_current_schema(value);
            Ok(())
        }
        expr => NotSupportedSnafu {
            feat: format!(
                "Unsupported search path expr {} in set variable statement",
                expr
            ),
        }
        .fail(),
    }
}

pub fn validate_client_encoding(set: SetVariables) -> Result<()> {
    let Some((encoding, [])) = set.value.split_first() else {
        return InvalidSqlSnafu {
            err_msg: "must provide one and only one client encoding value",
        }
        .fail();
    };
    let encoding = match encoding {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(x),
            ..
        })
        | Expr::Identifier(Ident {
            value: x,
            quote_style: _,
            span: _,
        }) => x.to_uppercase(),
        _ => {
            return InvalidSqlSnafu {
                err_msg: format!("client encoding must be a string, actual: {:?}", encoding),
            }
            .fail();
        }
    };
    // For the sake of simplicity, we only support "UTF8" ("UNICODE" is the alias for it,
    // see https://www.postgresql.org/docs/current/multibyte.html#MULTIBYTE-CHARSET-SUPPORTED).
    // "UTF8" is universal and sufficient for almost all cases.
    // GreptimeDB itself is always using "UTF8" as the internal encoding.
    ensure!(
        encoding == "UTF8" || encoding == "UNICODE",
        NotSupportedSnafu {
            feat: format!("client encoding of '{}'", encoding)
        }
    );
    Ok(())
}

// if one of original value and new value is none, return the other one
// returns new values only when it equals to original one else return error.
// This is only used for handling datestyle
fn merge_datestyle_value<T>(value: Option<T>, new_value: Option<T>) -> Result<Option<T>>
where
    T: PartialEq,
{
    match (&value, &new_value) {
        (None, _) => Ok(new_value),
        (_, None) => Ok(value),
        (Some(v1), Some(v2)) if v1 == v2 => Ok(new_value),
        _ => InvalidSqlSnafu {
            err_msg: "Conflicting \"datestyle\" specifications.",
        }
        .fail(),
    }
}

fn try_parse_datestyle(expr: &Expr) -> Result<(Option<PGDateTimeStyle>, Option<PGDateOrder>)> {
    enum ParsedDateStyle {
        Order(PGDateOrder),
        Style(PGDateTimeStyle),
    }
    fn try_parse_str(s: &str) -> Result<ParsedDateStyle> {
        PGDateTimeStyle::try_from(s)
            .map_or_else(
                |_| PGDateOrder::try_from(s).map(ParsedDateStyle::Order),
                |style| Ok(ParsedDateStyle::Style(style)),
            )
            .context(InvalidConfigValueSnafu)
    }
    match expr {
        Expr::Identifier(Ident {
            value: s,
            quote_style: _,
            span: _,
        })
        | Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        }) => s
            .split(',')
            .map(|s| s.trim())
            .try_fold((None, None), |(style, order), s| match try_parse_str(s)? {
                ParsedDateStyle::Order(o) => Ok((style, merge_datestyle_value(order, Some(o))?)),
                ParsedDateStyle::Style(s) => Ok((merge_datestyle_value(style, Some(s))?, order)),
            }),
        _ => NotSupportedSnafu {
            feat: "Not supported expression for datestyle",
        }
        .fail(),
    }
}

/// Set the allow query fallback configuration parameter to true or false based on the provided expressions.
///
pub fn set_allow_query_fallback(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let allow_fallback_expr = exprs.first().context(NotSupportedSnafu {
        feat: "No allow query fallback value find in set variable statement",
    })?;
    match allow_fallback_expr {
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(allow),
            span: _,
        }) => {
            ctx.configuration_parameter()
                .set_allow_query_fallback(*allow);
            Ok(())
        }
        expr => NotSupportedSnafu {
            feat: format!(
                "Unsupported allow query fallback expr {} in set variable statement",
                expr
            ),
        }
        .fail(),
    }
}

pub fn set_datestyle(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    // ORDER,
    // STYLE,
    // ORDER,ORDER
    // ORDER,STYLE
    // STYLE,ORDER
    let (style, order) = exprs
        .iter()
        .try_fold((None, None), |(style, order), expr| {
            let (new_style, new_order) = try_parse_datestyle(expr)?;
            Ok((
                merge_datestyle_value(style, new_style)?,
                merge_datestyle_value(order, new_order)?,
            ))
        })?;

    let (old_style, older_order) = *ctx.configuration_parameter().pg_datetime_style();
    ctx.configuration_parameter()
        .set_pg_datetime_style(style.unwrap_or(old_style), order.unwrap_or(older_order));
    Ok(())
}

pub fn set_query_timeout(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let timeout_expr = exprs.first().context(NotSupportedSnafu {
        feat: "No timeout value find in set query timeout statement",
    })?;
    match timeout_expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(timeout, _),
            ..
        }) => {
            match timeout.parse::<u64>() {
                Ok(timeout) => ctx.set_query_timeout(Duration::from_millis(timeout)),
                Err(_) => {
                    return NotSupportedSnafu {
                        feat: format!("Invalid timeout expr {} in set variable statement", timeout),
                    }
                    .fail()
                }
            }
            Ok(())
        }
        // postgres support time units i.e. SET STATEMENT_TIMEOUT = '50ms';
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(timeout),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(timeout),
            ..
        }) => {
            if ctx.channel() != Postgres {
                return NotSupportedSnafu {
                    feat: format!("Invalid timeout expr {} in set variable statement", timeout),
                }
                .fail();
            }
            let timeout = parse_pg_query_timeout_input(timeout)?;
            ctx.set_query_timeout(Duration::from_millis(timeout));
            Ok(())
        }
        expr => NotSupportedSnafu {
            feat: format!(
                "Unsupported timeout expr {} in set variable statement",
                expr
            ),
        }
        .fail(),
    }
}

// support time units in ms, s, min, h, d for postgres protocol.
// https://www.postgresql.org/docs/8.4/config-setting.html#:~:text=Valid%20memory%20units%20are%20kB,%2C%20and%20d%20(days).
fn parse_pg_query_timeout_input(input: &str) -> Result<u64> {
    match input.parse::<u64>() {
        Ok(timeout) => Ok(timeout),
        Err(_) => {
            if let Some(captures) = PG_TIME_INPUT_REGEX.captures(input) {
                let value = captures[1].parse::<u64>().expect("regex failed");
                let unit = &captures[2];

                match unit {
                    "ms" => Ok(value),
                    "s" => Ok(value * 1000),
                    "min" => Ok(value * 60 * 1000),
                    "h" => Ok(value * 60 * 60 * 1000),
                    "d" => Ok(value * 24 * 60 * 60 * 1000),
                    _ => unreachable!("regex failed"),
                }
            } else {
                NotSupportedSnafu {
                    feat: format!(
                        "Unsupported timeout expr {} in set variable statement",
                        input
                    ),
                }
                .fail()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::statement::set::parse_pg_query_timeout_input;

    #[test]
    fn test_parse_pg_query_timeout_input() {
        assert!(parse_pg_query_timeout_input("").is_err());
        assert!(parse_pg_query_timeout_input(" 50 ms").is_err());
        assert!(parse_pg_query_timeout_input("5s 1ms").is_err());
        assert!(parse_pg_query_timeout_input("3a").is_err());
        assert!(parse_pg_query_timeout_input("1.5min").is_err());
        assert!(parse_pg_query_timeout_input("ms").is_err());
        assert!(parse_pg_query_timeout_input("a").is_err());
        assert!(parse_pg_query_timeout_input("-1").is_err());

        assert_eq!(50, parse_pg_query_timeout_input("50").unwrap());
        assert_eq!(12, parse_pg_query_timeout_input("12ms").unwrap());
        assert_eq!(2000, parse_pg_query_timeout_input("2s").unwrap());
        assert_eq!(60000, parse_pg_query_timeout_input("1min").unwrap());
    }
}
