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
use std::sync::Arc;
use std::vec;

use common_query::AddColumnLocation;
use common_telemetry::info;
use common_time::timezone::parse_timezone;
use datatypes::value::Value;
use partition::partition::PartitionDef;
use rand::Rng;
use snafu::{ensure, OptionExt};

use crate::error::{self, Result};
use crate::generator::Random;
use crate::ir::alter_expr::AlterTableOperation;
use crate::ir::create_expr::ColumnOption;
use crate::ir::insert_expr::RowValue;
use crate::ir::{AlterTableExpr, Column, CreateTableExpr, Ident, InsertIntoExpr};

pub type TableContextRef = Arc<TableContext>;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
struct Cell {
    value: mysql::Value,
}

impl Cell {
    fn null() -> Self {
        Self {
            value: mysql::Value::NULL,
        }
    }
}

impl Display for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value.as_sql(true))
    }
}

impl From<Value> for Cell {
    fn from(value: Value) -> Self {
        if value.is_null() {
            Self::null()
        } else {
            let s = match value {
                Value::Timestamp(t) => {
                    t.to_timezone_aware_string(Some(&parse_timezone(Some("+08:00"))))
                }
                Value::Boolean(b) => if b { "1" } else { "0" }.to_string(),
                _ => value.to_string(),
            };
            Self {
                value: mysql::Value::Bytes(s.into_bytes()),
            }
        }
    }
}

impl From<mysql::Value> for Cell {
    fn from(value: mysql::Value) -> Self {
        Self { value }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Row {
    cells: Vec<Cell>,
}

impl Row {
    fn with(columns_len: usize) -> Self {
        Self {
            cells: vec![Cell::null(); columns_len],
        }
    }

    fn set_cell(&mut self, at: usize, cell: Cell) {
        self.cells[at] = cell;
    }

    fn add_column(&mut self, at: usize, cell: Cell) {
        self.cells.insert(at, cell);
    }

    fn drop_column(&mut self, at: usize) {
        self.cells.remove(at);
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, v) in self.cells.iter().enumerate() {
            write!(f, "{}\t", v)?;
            if i + 1 < self.cells.len() {
                write!(f, "\t")?;
            }
        }
        write!(f, "]")
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Rows {
    rows: Vec<Row>,
}

impl Rows {
    pub fn empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn new() -> Self {
        Self { rows: vec![] }
    }

    pub fn fill(rows: Vec<mysql::Row>) -> Self {
        Self {
            rows: rows
                .into_iter()
                .map(|r| Row {
                    cells: r.unwrap().into_iter().map(Into::into).collect(),
                })
                .collect(),
        }
    }

    fn with(rows: usize) -> Self {
        Self {
            rows: Vec::with_capacity(rows),
        }
    }

    fn add_column(&mut self, at: usize, column: &Column) {
        let cell = column
            .options
            .iter()
            .find_map(|x| {
                if let ColumnOption::DefaultValue(v) = x {
                    Some(v.clone().into())
                } else {
                    None
                }
            })
            .unwrap_or(Cell::null());
        self.rows
            .iter_mut()
            .for_each(|x| x.add_column(at, cell.clone()))
    }

    fn drop_column(&mut self, at: usize) {
        self.rows.iter_mut().for_each(|x| x.drop_column(at))
    }

    fn add_row(&mut self, row: Row) {
        self.rows.push(row)
    }

    fn extend(&mut self, rows: Rows) {
        self.rows.extend(rows.rows)
    }
}

impl Display for Rows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Rows")?;
        for r in &self.rows {
            writeln!(f, "{}", r)?;
        }
        Ok(())
    }
}

/// TableContext stores table info.
#[derive(Debug, Clone)]
pub struct TableContext {
    pub name: Ident,
    pub columns: Vec<Column>,
    pub rows: Rows,

    // GreptimeDB specific options
    pub partition: Option<PartitionDef>,
    pub primary_keys: Vec<usize>,
}

impl From<&CreateTableExpr> for TableContext {
    fn from(
        CreateTableExpr {
            table_name: name,
            columns,
            partition,
            primary_keys,
            ..
        }: &CreateTableExpr,
    ) -> Self {
        Self {
            name: name.clone(),
            columns: columns.clone(),
            rows: Rows::new(),
            partition: partition.clone(),
            primary_keys: primary_keys.clone(),
        }
    }
}

impl TableContext {
    pub fn clear_data(&mut self) {
        self.rows = Rows::new();
    }

    pub fn insert(&mut self, expr: InsertIntoExpr) -> Result<()> {
        fn find_default_value(column: &Column) -> Cell {
            column
                .options
                .iter()
                .find_map(|opt| {
                    if let ColumnOption::DefaultValue(v) = opt {
                        Some(v.clone().into())
                    } else {
                        None
                    }
                })
                .unwrap_or(Cell::null())
        }

        let mut rows = Rows::with(expr.values_list.len());

        for insert_values in expr.values_list.into_iter() {
            let mut row = Row::with(self.columns.len());

            for (i, column) in self.columns.iter().enumerate() {
                let cell = if let Some(v) = expr
                    .columns
                    .iter()
                    .zip(insert_values.iter())
                    .find_map(|(x, y)| if x.name == column.name { Some(y) } else { None })
                {
                    match v {
                        RowValue::Value(v) => v.clone().into(),
                        RowValue::Default => find_default_value(column),
                    }
                } else {
                    find_default_value(column)
                };
                row.set_cell(i, cell);
            }
            rows.add_row(row);
        }
        self.rows.extend(rows);

        let time_index = self.columns.iter().position(|x| x.is_time_index()).unwrap();
        self.rows.rows.sort_by(|x, y| {
            x.cells[time_index]
                .partial_cmp(&y.cells[time_index])
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        info!("after insertion, expected rows: {}", self.rows);
        Ok(())
    }

    fn find_column_index(&self, column_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|col| col.name.to_string() == column_name)
            .context(error::UnexpectedSnafu {
                violated: format!("Column: {column_name} not found"),
            })
    }

    /// Applies the [AlterTableExpr].
    pub fn alter(&mut self, expr: AlterTableExpr) -> Result<()> {
        match expr.alter_options {
            AlterTableOperation::AddColumn { column, location } => {
                ensure!(
                    !self.columns.iter().any(|col| col.name == column.name),
                    error::UnexpectedSnafu {
                        violated: format!("Column {} exists", column.name),
                    }
                );
                match location {
                    Some(AddColumnLocation::First) => {
                        self.rows.add_column(0, &column);
                        self.columns.insert(0, column);
                    }
                    Some(AddColumnLocation::After { column_name }) => {
                        let index = self.find_column_index(&column_name)?;
                        self.rows.add_column(index + 1, &column);
                        self.columns.insert(index + 1, column);
                    }
                    None => {
                        self.rows.add_column(self.columns.len(), &column);
                        self.columns.push(column);
                    }
                }
                // Re-generates the primary_keys
                self.primary_keys = self
                    .columns
                    .iter()
                    .enumerate()
                    .flat_map(|(idx, col)| {
                        if col.is_primary_key() {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                    .collect();
            }
            AlterTableOperation::DropColumn { name } => {
                let at = self.find_column_index(&name.to_string())?;
                self.columns.remove(at);
                self.rows.drop_column(at);

                // Re-generates the primary_keys
                self.primary_keys = self
                    .columns
                    .iter()
                    .enumerate()
                    .flat_map(|(idx, col)| {
                        if col.is_primary_key() {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                    .collect();
            }
            AlterTableOperation::RenameTable { new_table_name } => {
                ensure!(
                    new_table_name != self.name,
                    error::UnexpectedSnafu {
                        violated: "The new table name is equal the current name",
                    }
                );
                self.name = new_table_name;
            }
        }
        Ok(())
    }

    pub fn generate_unique_column_name<R: Rng>(
        &self,
        rng: &mut R,
        generator: &dyn Random<Ident, R>,
    ) -> Ident {
        let mut name = generator.gen(rng);
        while self.columns.iter().any(|col| col.name.value == name.value) {
            name = generator.gen(rng);
        }
        name
    }

    pub fn generate_unique_table_name<R: Rng>(
        &self,
        rng: &mut R,
        generator: &dyn Random<Ident, R>,
    ) -> Ident {
        let mut name = generator.gen(rng);
        while self.name.value == name.value {
            name = generator.gen(rng);
        }
        name
    }
}

#[cfg(test)]
mod tests {
    use common_query::AddColumnLocation;
    use datatypes::data_type::ConcreteDataType;

    use super::*;
    use crate::ir::alter_expr::AlterTableOperation;
    use crate::ir::create_expr::ColumnOption;
    use crate::ir::{AlterTableExpr, Column, Ident};

    #[test]
    fn test_table_context_alter() {
        let mut table_ctx = TableContext {
            name: "foo".into(),
            columns: vec![],
            rows: Rows::new(),
            partition: None,
            primary_keys: vec![],
        };
        // Add a column
        let expr = AlterTableExpr {
            table_name: "foo".into(),
            alter_options: AlterTableOperation::AddColumn {
                column: Column {
                    name: "a".into(),
                    column_type: ConcreteDataType::timestamp_microsecond_datatype(),
                    options: vec![ColumnOption::PrimaryKey],
                },
                location: None,
            },
        };
        table_ctx.alter(expr).unwrap();
        assert_eq!(table_ctx.columns[0].name, Ident::new("a"));
        assert_eq!(table_ctx.primary_keys, vec![0]);

        // Add a column at first
        let expr = AlterTableExpr {
            table_name: "foo".into(),
            alter_options: AlterTableOperation::AddColumn {
                column: Column {
                    name: "b".into(),
                    column_type: ConcreteDataType::timestamp_microsecond_datatype(),
                    options: vec![ColumnOption::PrimaryKey],
                },
                location: Some(AddColumnLocation::First),
            },
        };
        table_ctx.alter(expr).unwrap();
        assert_eq!(table_ctx.columns[0].name, Ident::new("b"));
        assert_eq!(table_ctx.primary_keys, vec![0, 1]);

        // Add a column after "b"
        let expr = AlterTableExpr {
            table_name: "foo".into(),
            alter_options: AlterTableOperation::AddColumn {
                column: Column {
                    name: "c".into(),
                    column_type: ConcreteDataType::timestamp_microsecond_datatype(),
                    options: vec![ColumnOption::PrimaryKey],
                },
                location: Some(AddColumnLocation::After {
                    column_name: "b".into(),
                }),
            },
        };
        table_ctx.alter(expr).unwrap();
        assert_eq!(table_ctx.columns[1].name, Ident::new("c"));
        assert_eq!(table_ctx.primary_keys, vec![0, 1, 2]);

        // Drop the column "b"
        let expr = AlterTableExpr {
            table_name: "foo".into(),
            alter_options: AlterTableOperation::DropColumn { name: "b".into() },
        };
        table_ctx.alter(expr).unwrap();
        assert_eq!(table_ctx.columns[1].name, Ident::new("a"));
        assert_eq!(table_ctx.primary_keys, vec![0, 1]);
    }
}
