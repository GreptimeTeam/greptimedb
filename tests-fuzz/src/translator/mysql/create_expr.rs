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
use datatypes::value::Value;
use partition::partition::PartitionBound;
use sql::statements::concrete_data_type_to_sql_data_type;

use crate::error::{Error, Result};
use crate::ir::create_expr::ColumnOption;
use crate::ir::{Column, CreateTableExpr};
use crate::translator::DslTranslator;

pub struct CreateTableExprTranslator;

impl DslTranslator<CreateTableExpr, String> for CreateTableExprTranslator {
    type Error = Error;

    fn translate(&self, input: &CreateTableExpr) -> Result<String> {
        Ok(format!(
            "CREATE TABLE{}{}(\n{}\n)\n{}{};",
            Self::create_if_not_exists(input),
            input.table_name,
            Self::format_columns(input),
            Self::format_table_options(input),
            Self::format_with_clause(input),
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
        if let Some(primary_keys) = Self::format_primary_keys(input) {
            output.push(primary_keys);
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

    fn format_partition(input: &CreateTableExpr) -> Option<String> {
        input.partition.as_ref().map(|partition| {
            format!(
                "PARTITION BY RANGE COLUMNS({}) (\n{}\n)",
                partition.partition_columns().join(", "),
                partition
                    .partition_bounds()
                    .iter()
                    .enumerate()
                    .map(|(i, bound)| format!(
                        "PARTITION r{} VALUES LESS THAN ({})",
                        i,
                        Self::format_partition_bound(bound)
                    ))
                    .collect::<Vec<_>>()
                    .join(",\n")
            )
        })
    }

    fn format_partition_bound(bound: &PartitionBound) -> String {
        match bound {
            PartitionBound::Value(v) => match v {
                Value::String(v) => format!("'{}'", v.as_utf8()),
                _ => format!("{v}"),
            },
            PartitionBound::MaxValue => "MAXVALUE".to_string(),
            PartitionBound::Expr(expr) => expr.to_parser_expr().to_string(),
        }
    }

    fn format_column_type(column_type: &ConcreteDataType) -> String {
        // Safety: We don't use the `Dictionary` type
        concrete_data_type_to_sql_data_type(column_type)
            .unwrap()
            .to_string()
    }

    fn format_column_options(options: &[ColumnOption]) -> String {
        let mut output = Vec::with_capacity(options.len());
        for option in options {
            if option != &ColumnOption::PrimaryKey {
                output.push(option.to_string());
            }
        }
        output.join(" ")
    }

    fn format_primary_keys(input: &CreateTableExpr) -> Option<String> {
        if input.primary_keys.is_empty() {
            None
        } else {
            Some(format!(
                "PRIMARY KEY({})",
                input
                    .primary_keys
                    .iter()
                    .map(|idx| input.columns[*idx].name.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    }

    fn format_table_options(input: &CreateTableExpr) -> String {
        let mut output = vec![];
        if !input.engine.is_empty() {
            output.push(format!("ENGINE={}", input.engine));
        }
        if let Some(partition) = Self::format_partition(input) {
            output.push(partition);
        }

        output.join("\n")
    }

    fn format_with_clause(input: &CreateTableExpr) -> String {
        if input.options.is_empty() {
            String::new()
        } else {
            let mut output = vec![];
            for (key, value) in &input.options {
                output.push(format!("\"{key}\" = \"{value}\""));
            }
            format!(" with ({})", output.join("\n"))
        }
    }
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use partition::partition::{PartitionBound, PartitionDef};

    use super::CreateTableExprTranslator;
    use crate::ir::create_expr::CreateTableExprBuilder;
    use crate::test_utils;
    use crate::translator::DslTranslator;

    #[test]
    fn test_create_table_expr_translator() {
        let test_ctx = test_utils::new_test_ctx();
        let create_table_expr = CreateTableExprBuilder::default()
            .columns(test_ctx.columns.clone())
            .table_name("system_metrics")
            .engine("mito")
            .primary_keys(vec![0, 1])
            .partition(PartitionDef::new(
                vec!["idc".to_string()],
                vec![
                    PartitionBound::Value(Value::String("a".into())),
                    PartitionBound::Value(Value::String("f".into())),
                    PartitionBound::MaxValue,
                ],
            ))
            .build()
            .unwrap();

        let output = CreateTableExprTranslator
            .translate(&create_table_expr)
            .unwrap();

        assert_eq!(
            "CREATE TABLE system_metrics(
host STRING,
idc STRING,
cpu_util DOUBLE,
memory_util DOUBLE,
disk_util DOUBLE,
ts TIMESTAMP(3) TIME INDEX,
PRIMARY KEY(host, idc)
)
ENGINE=mito
PARTITION BY RANGE COLUMNS(idc) (
PARTITION r0 VALUES LESS THAN ('a'),
PARTITION r1 VALUES LESS THAN ('f'),
PARTITION r2 VALUES LESS THAN (MAXVALUE)
);",
            output
        );
    }
}
