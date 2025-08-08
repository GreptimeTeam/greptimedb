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

use common_time::timezone::Timezone;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::constraint::{CURRENT_TIMESTAMP, CURRENT_TIMESTAMP_FN};
use datatypes::schema::ColumnDefaultConstraint;
use sqlparser::ast::ValueWithSpan;
pub use sqlparser::ast::{
    visit_expressions_mut, visit_statements_mut, BinaryOperator, ColumnDef, ColumnOption,
    ColumnOptionDef, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    Ident, ObjectName, SqlOption, TableConstraint, TimezoneInfo, UnaryOperator, Value as SqlValue,
    Visit, VisitMut, Visitor, VisitorMut,
};

use crate::convert::{sql_number_to_value, sql_value_to_value};
use crate::error::{Result, UnsupportedDefaultValueSnafu};

pub fn parse_column_default_constraint(
    column_name: &str,
    data_type: &ConcreteDataType,
    opts: &[ColumnOptionDef],
    timezone: Option<&Timezone>,
) -> Result<Option<ColumnDefaultConstraint>> {
    if let Some(opt) = opts
        .iter()
        .find(|o| matches!(o.option, ColumnOption::Default(_)))
    {
        let default_constraint = match &opt.option {
            ColumnOption::Default(Expr::Value(v)) => ColumnDefaultConstraint::Value(
                sql_value_to_value(column_name, data_type, &v.value, timezone, None, false)?,
            ),
            ColumnOption::Default(Expr::Function(func)) => {
                let mut func = format!("{func}").to_lowercase();
                // normalize CURRENT_TIMESTAMP to CURRENT_TIMESTAMP()
                if func == CURRENT_TIMESTAMP {
                    func = CURRENT_TIMESTAMP_FN.to_string();
                }
                // Always use lowercase for function expression
                ColumnDefaultConstraint::Function(func.to_lowercase())
            }

            ColumnOption::Default(Expr::UnaryOp { op, expr }) => {
                // Specialized process for handling numerical inputs to prevent
                // overflow errors during the parsing of negative numbers,
                // See https://github.com/GreptimeTeam/greptimedb/issues/4351
                if let (
                    UnaryOperator::Minus,
                    Expr::Value(ValueWithSpan {
                        value: SqlValue::Number(n, _),
                        span: _,
                    }),
                ) = (op, expr.as_ref())
                {
                    return Ok(Some(ColumnDefaultConstraint::Value(sql_number_to_value(
                        data_type,
                        &format!("-{n}"),
                    )?)));
                }

                if let Expr::Value(v) = &**expr {
                    let value = sql_value_to_value(
                        column_name,
                        data_type,
                        &v.value,
                        timezone,
                        Some(*op),
                        false,
                    )?;
                    ColumnDefaultConstraint::Value(value)
                } else {
                    return UnsupportedDefaultValueSnafu {
                        column_name,
                        expr: *expr.clone(),
                    }
                    .fail();
                }
            }
            ColumnOption::Default(others) => {
                return UnsupportedDefaultValueSnafu {
                    column_name,
                    expr: others.clone(),
                }
                .fail();
            }
            _ => {
                return UnsupportedDefaultValueSnafu {
                    column_name,
                    expr: Expr::Value(SqlValue::Null.into()),
                }
                .fail();
            }
        };

        Ok(Some(default_constraint))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;

    use datatypes::prelude::{ConcreteDataType, Value};
    use datatypes::types::BooleanType;

    use super::*;

    #[test]
    pub fn test_parse_column_default_constraint() {
        let bool_value = sqlparser::ast::Value::Boolean(true);

        let opts = vec![
            ColumnOptionDef {
                name: None,
                option: ColumnOption::Default(Expr::Value(bool_value.into())),
            },
            ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            },
        ];

        let constraint = parse_column_default_constraint(
            "coll",
            &ConcreteDataType::Boolean(BooleanType),
            &opts,
            None,
        )
        .unwrap();

        assert_matches!(
            constraint,
            Some(ColumnDefaultConstraint::Value(Value::Boolean(true)))
        );

        // Test negative number
        let opts = vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::Default(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(
                    SqlValue::Number("32768".to_string(), false).into(),
                )),
            }),
        }];

        let constraint = parse_column_default_constraint(
            "coll",
            &ConcreteDataType::int16_datatype(),
            &opts,
            None,
        )
        .unwrap();

        assert_matches!(
            constraint,
            Some(ColumnDefaultConstraint::Value(Value::Int16(-32768)))
        );
    }

    #[test]
    fn test_incorrect_default_value_issue_3479() {
        let opts = vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::Default(Expr::Value(
                SqlValue::Number("0.047318541668048164".into(), false).into(),
            )),
        }];
        let constraint = parse_column_default_constraint(
            "coll",
            &ConcreteDataType::float64_datatype(),
            &opts,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!("0.047318541668048164", constraint.to_string());
        let encoded: Vec<u8> = constraint.clone().try_into().unwrap();
        let decoded = ColumnDefaultConstraint::try_from(encoded.as_ref()).unwrap();
        assert_eq!(decoded, constraint);
    }
}
