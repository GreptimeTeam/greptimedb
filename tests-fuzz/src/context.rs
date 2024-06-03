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

use std::sync::Arc;

use common_query::AddColumnLocation;
use partition::partition::PartitionDef;
use rand::Rng;
use snafu::{ensure, OptionExt};

use crate::error::{self, Result};
use crate::generator::Random;
use crate::ir::alter_expr::AlterTableOperation;
use crate::ir::{AlterTableExpr, Column, CreateTableExpr, Ident};

pub type TableContextRef = Arc<TableContext>;

/// TableContext stores table info.
#[derive(Debug, Clone)]
pub struct TableContext {
    pub name: Ident,
    pub columns: Vec<Column>,

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
            partition: partition.clone(),
            primary_keys: primary_keys.clone(),
        }
    }
}

impl TableContext {
    /// Applies the [AlterTableExpr].
    pub fn alter(mut self, expr: AlterTableExpr) -> Result<TableContext> {
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
                        let mut columns = Vec::with_capacity(self.columns.len() + 1);
                        columns.push(column);
                        columns.extend(self.columns);
                        self.columns = columns;
                    }
                    Some(AddColumnLocation::After { column_name }) => {
                        let index = self
                            .columns
                            .iter()
                            // TODO(weny): find a better way?
                            .position(|col| col.name.to_string() == column_name)
                            .context(error::UnexpectedSnafu {
                                violated: format!("Column: {column_name} not found"),
                            })?;
                        self.columns.insert(index + 1, column);
                    }
                    None => self.columns.push(column),
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
                Ok(self)
            }
            AlterTableOperation::DropColumn { name } => {
                self.columns.retain(|col| col.name != name);
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
                Ok(self)
            }
            AlterTableOperation::RenameTable { new_table_name } => {
                ensure!(
                    new_table_name != self.name,
                    error::UnexpectedSnafu {
                        violated: "The new table name is equal the current name",
                    }
                );
                self.name = new_table_name;
                Ok(self)
            }
            AlterTableOperation::ModifyDataType { column } => {
                if let Some(idx) = self.columns.iter().position(|col| col.name == column.name) {
                    self.columns[idx].column_type = column.column_type;
                }
                Ok(self)
            }
        }
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

    use super::TableContext;
    use crate::ir::alter_expr::AlterTableOperation;
    use crate::ir::create_expr::ColumnOption;
    use crate::ir::{AlterTableExpr, Column, Ident};

    #[test]
    fn test_table_context_alter() {
        let table_ctx = TableContext {
            name: "foo".into(),
            columns: vec![],
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
        let table_ctx = table_ctx.alter(expr).unwrap();
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
        let table_ctx = table_ctx.alter(expr).unwrap();
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
        let table_ctx = table_ctx.alter(expr).unwrap();
        assert_eq!(table_ctx.columns[1].name, Ident::new("c"));
        assert_eq!(table_ctx.primary_keys, vec![0, 1, 2]);

        // Drop the column "b"
        let expr = AlterTableExpr {
            table_name: "foo".into(),
            alter_options: AlterTableOperation::DropColumn { name: "b".into() },
        };
        let table_ctx = table_ctx.alter(expr).unwrap();
        assert_eq!(table_ctx.columns[1].name, Ident::new("a"));
        assert_eq!(table_ctx.primary_keys, vec![0, 1]);
    }
}
