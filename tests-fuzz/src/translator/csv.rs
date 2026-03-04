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

use crate::error::Error;
use crate::ir::insert_expr::{InsertIntoExpr, RowValue};
use crate::translator::DslTranslator;

/// One CSV record converted from an insert row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvRecord {
    /// Cell values in column order.
    pub values: Vec<String>,
}

/// CSV records converted from an insert expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvRecords {
    /// Target table name from insert expression.
    pub table_name: String,
    /// Header values from insert columns.
    pub headers: Vec<String>,
    /// Converted row records.
    pub records: Vec<CsvRecord>,
}

/// Translates `InsertIntoExpr` into CSV-writer-ready records.
pub struct InsertExprToCsvRecordsTranslator;

impl DslTranslator<InsertIntoExpr, CsvRecords> for InsertExprToCsvRecordsTranslator {
    type Error = Error;

    fn translate(&self, input: &InsertIntoExpr) -> Result<CsvRecords, Self::Error> {
        let headers = input
            .columns
            .iter()
            .map(|column| column.name.to_string())
            .collect::<Vec<_>>();
        let records = input
            .values_list
            .iter()
            .map(|row| CsvRecord {
                values: row.iter().map(Self::format_row_value).collect(),
            })
            .collect::<Vec<_>>();

        Ok(CsvRecords {
            table_name: input.table_name.to_string(),
            headers,
            records,
        })
    }
}

impl InsertExprToCsvRecordsTranslator {
    fn format_row_value(value: &RowValue) -> String {
        match value {
            RowValue::Value(datatypes::value::Value::Null) => String::new(),
            RowValue::Value(v) => v.to_string(),
            RowValue::Default => "DEFAULT".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;

    use super::InsertExprToCsvRecordsTranslator;
    use crate::ir::create_expr::ColumnOption;
    use crate::ir::insert_expr::{InsertIntoExpr, RowValue};
    use crate::ir::{Column, Ident};
    use crate::translator::DslTranslator;

    #[test]
    fn test_translate_insert_expr_to_csv_records() {
        let input = InsertIntoExpr {
            table_name: Ident::new("metric_a"),
            omit_column_list: false,
            columns: vec![
                Column {
                    name: "host".into(),
                    column_type: ConcreteDataType::string_datatype(),
                    options: vec![ColumnOption::PrimaryKey],
                },
                Column {
                    name: "value".into(),
                    column_type: ConcreteDataType::float64_datatype(),
                    options: vec![],
                },
            ],
            values_list: vec![
                vec![
                    RowValue::Value(datatypes::value::Value::String("web-1".into())),
                    RowValue::Value(datatypes::value::Value::Int32(15)),
                ],
                vec![
                    RowValue::Value(datatypes::value::Value::Null),
                    RowValue::Default,
                ],
            ],
        };

        let output = InsertExprToCsvRecordsTranslator.translate(&input).unwrap();
        assert_eq!(output.table_name, "metric_a");
        assert_eq!(output.headers, vec!["host", "value"]);
        assert_eq!(output.records.len(), 2);
        assert_eq!(output.records[0].values, vec!["web-1", "15"]);
        assert_eq!(output.records[1].values, vec!["", "DEFAULT"]);
    }
}
