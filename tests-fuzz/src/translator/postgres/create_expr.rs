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

use datatypes::data_type::ConcreteDataType;
use sql::statements::concrete_data_type_to_sql_data_type;

use crate::error::{Error, Result};
use crate::ir::create_expr::ColumnOption;
use crate::ir::{Column, CreateTableExpr};
use crate::translator::postgres::sql_data_type_to_postgres_data_type;
use crate::translator::DslTranslator;

pub struct CreateTableExprTranslator;

impl DslTranslator<CreateTableExpr, String> for CreateTableExprTranslator {
    type Error = Error;

    fn translate(&self, input: &CreateTableExpr) -> Result<String> {
        Ok(format!(
            "CREATE TABLE{}{}(\n{}\n);",
            Self::create_if_not_exists(input),
            input.name,
            Self::format_columns(input),
        ))
    }
}

impl CreateTableExprTranslator {
    fn create_if_not_exists(input: &CreateTableExpr) -> &str {
        if input.if_not_exists {
            " IF NOT EXISTS "
        } else {
            " "
        }
    }

    fn format_columns(input: &CreateTableExpr) -> String {
        let mut output =
            Vec::with_capacity(input.columns.len() + (!input.primary_keys.is_empty()) as usize);
        for column in &input.columns {
            output.push(Self::format_column(column));
        }
        output.join(",\n")
    }

    fn format_column(column: &Column) -> String {
        vec![
            column.name.to_string(),
            Self::format_column_type(&column.column_type),
            Self::format_column_options(&column.options),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
    }

    fn format_column_type(column_type: &ConcreteDataType) -> String {
        // Safety: We don't use the `Dictionary` type
        sql_data_type_to_postgres_data_type(
            concrete_data_type_to_sql_data_type(column_type).unwrap(),
        )
    }

    fn acceptable_column_option(option: &ColumnOption) -> bool {
        matches!(
            option,
            ColumnOption::Null
                | ColumnOption::NotNull
                | ColumnOption::DefaultValue(_)
                | ColumnOption::DefaultFn(_)
        )
    }

    fn format_column_options(options: &[ColumnOption]) -> String {
        let mut output = Vec::with_capacity(options.len());
        for option in options {
            if Self::acceptable_column_option(option) {
                output.push(option.to_string());
            }
        }
        output.join(" ")
    }
}
