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

use std::f64::consts::E;
use std::sync::Arc;

use common_query::AddColumnLocation;
use faker_rand::lorem::Word;
use rand::{random, Rng};
use snafu::ensure;

use crate::error::{self, Result};
use crate::ir::alter_expr::{AlterTableExpr, AlterTableOperation};
use crate::ir::{droppable_columns, Column};

pub struct AlterTableExprGenerator {
    name: String,
    columns: Arc<Vec<Column>>,
    ignore_no_droppable_column: bool,
}

impl AlterTableExprGenerator {
    pub fn new(name: String, columns: Arc<Vec<Column>>) -> Self {
        Self {
            name,
            columns,
            ignore_no_droppable_column: false,
        }
    }

    /// If the `ignore_no_droppable_column` is true, it retries if there is no droppable column.
    pub fn ignore_no_droppable_column(mut self, v: bool) -> Self {
        self.ignore_no_droppable_column = v;
        self
    }

    fn generate_inner(&self) -> Result<AlterTableExpr> {
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..3);
        // 0 -> AddColumn
        // 1 -> DropColumn(invariant: We can't non-primary key columns, non-ts columns)
        // 2 -> RenameTable
        let alter_expr = match idx {
            0 => {
                let with_location = rng.gen::<bool>();
                let location = if with_location {
                    let use_first = rng.gen::<bool>();
                    let location = if use_first {
                        AddColumnLocation::First
                    } else {
                        AddColumnLocation::After {
                            column_name: self.columns[rng.gen_range(0..self.columns.len())]
                                .name
                                .to_string(),
                        }
                    };
                    Some(location)
                } else {
                    None
                };
                let column = rng.gen::<Column>();
                AlterTableExpr {
                    name: self.name.to_string(),
                    alter_options: AlterTableOperation::AddColumn { column, location },
                }
            }
            1 => {
                let droppable = droppable_columns(&self.columns);
                ensure!(!droppable.is_empty(), error::DroppableColumnsSnafu);
                let name = droppable[rng.gen_range(0..droppable.len())]
                    .name
                    .to_string();
                AlterTableExpr {
                    name: self.name.to_string(),
                    alter_options: AlterTableOperation::DropColumn { name },
                }
            }
            2 => {
                let mut new_table_name = rng.gen::<Word>().to_string();
                if new_table_name == self.name {
                    new_table_name = format!("{}-{}", self.name, rng.gen::<u64>());
                }
                AlterTableExpr {
                    name: self.name.to_string(),
                    alter_options: AlterTableOperation::RenameTable { new_table_name },
                }
            }
            _ => unreachable!(),
        };

        Ok(alter_expr)
    }

    /// Generates the [AlterTableExpr].
    pub fn generate(&self) -> Result<AlterTableExpr> {
        match self.generate_inner() {
            Ok(expr) => Ok(expr),
            Err(err) => {
                if matches!(err, error::Error::DroppableColumns { .. }) {
                    return self.generate();
                }
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::AlterTableExprGenerator;
    use crate::generator::create_expr::CreateTableExprGenerator;
    use crate::generator::Generator;

    #[test]
    fn test_alter_table_expr_generator() {
        let create_expr = CreateTableExprGenerator::default()
            .columns(10)
            .generate()
            .unwrap();

        let alter_expr = AlterTableExprGenerator::new(
            create_expr.name.to_string(),
            Arc::new(create_expr.columns),
        )
        .ignore_no_droppable_column(true)
        .generate()
        .unwrap();
    }
}
