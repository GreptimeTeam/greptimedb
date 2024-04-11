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

use common_time::Timezone;
use session::context::QueryContextRef;
use session::session_config::{PGByteaOutputValue, PGDateOrder, PGDateTimeStyle};
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{Expr, Ident, Value};
use sql::statements::set_variables::SetVariables;

use crate::error::{InvalidConfigValueSnafu, InvalidSqlSnafu, NotSupportedSnafu, Result};

pub fn set_timezone(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let tz_expr = exprs.first().context(NotSupportedSnafu {
        feat: "No timezone find in set variable statement",
    })?;
    match tz_expr {
        Expr::Value(Value::SingleQuotedString(tz)) | Expr::Value(Value::DoubleQuotedString(tz)) => {
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
        PGByteaOutputValue::try_from(value.clone()).context(InvalidConfigValueSnafu)?,
    );
    Ok(())
}

pub fn validate_client_encoding(set: SetVariables) -> Result<()> {
    let Some((encoding, [])) = set.value.split_first() else {
        return InvalidSqlSnafu {
            err_msg: "must provide one and only one client encoding value",
        }
        .fail();
    };
    let encoding = match encoding {
        Expr::Value(Value::SingleQuotedString(x))
        | Expr::Identifier(Ident {
            value: x,
            quote_style: _,
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
        })
        | Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s)) => {
            s.split(',')
                .map(|s| s.trim())
                .try_fold((None, None), |(style, order), s| match try_parse_str(s)? {
                    ParsedDateStyle::Order(o) => {
                        Ok((style, merge_datestyle_value(order, Some(o))?))
                    }
                    ParsedDateStyle::Style(s) => {
                        Ok((merge_datestyle_value(style, Some(s))?, order))
                    }
                })
        }
        _ => NotSupportedSnafu {
            feat: "Not supported expression for datestyle",
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
