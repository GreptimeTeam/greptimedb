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

use crate::context::TableContextRef;
use crate::error::{self, Error, Result};
use crate::generator::Generator;
use crate::ir::alter_expr::{AlterTableExpr, AlterTableOperation};
use crate::ir::{droppable_columns, Column};

/// Generates the [AlterTableOperation::AddColumn] of [AlterTableExpr].
pub struct AlterExprAddColumnGenerator {
    table_ctx: TableContextRef,
    location: bool,
}

impl AlterExprAddColumnGenerator {
    /// Returns an [AlterExprAddColumnGenerator].
    pub fn new(table_ctx: &TableContextRef) -> Self {
        Self {
            table_ctx: table_ctx.clone(),
            location: false,
        }
    }

    /// Sets true to generate alter expr with a specific location.
    pub fn with_location(mut self, v: bool) -> Self {
        self.location = v;
        self
    }
}

impl Generator<AlterTableExpr> for AlterExprAddColumnGenerator {
    type Error = Error;

    fn generate(&self) -> Result<AlterTableExpr> {
        let mut rng = rand::thread_rng();
        let with_location = self.location && rng.gen::<bool>();
        let location = if with_location {
            let use_first = rng.gen::<bool>();
            let location = if use_first {
                AddColumnLocation::First
            } else {
                AddColumnLocation::After {
                    column_name: self.table_ctx.columns
                        [rng.gen_range(0..self.table_ctx.columns.len())]
                    .name
                    .to_string(),
                }
            };
            Some(location)
        } else {
            None
        };

        let column = rng.gen::<Column>();
        Ok(AlterTableExpr {
            name: self.table_ctx.name.to_string(),
            alter_options: AlterTableOperation::AddColumn { column, location },
        })
    }
}

/// Generates the [AlterTableOperation::DropColumn] of [AlterTableExpr].
pub struct AlterExprDropColumnGenerator {
    table_ctx: TableContextRef,
}

impl AlterExprDropColumnGenerator {
    /// Returns an [AlterExprDropColumnGenerator].
    pub fn new(table_ctx: &TableContextRef) -> Self {
        Self {
            table_ctx: table_ctx.clone(),
        }
    }
}

impl Generator<AlterTableExpr> for AlterExprDropColumnGenerator {
    type Error = Error;

    fn generate(&self) -> Result<AlterTableExpr> {
        let mut rng = rand::thread_rng();
        let droppable = droppable_columns(&self.table_ctx.columns);
        ensure!(!droppable.is_empty(), error::DroppableColumnsSnafu);
        let name = droppable[rng.gen_range(0..droppable.len())]
            .name
            .to_string();
        Ok(AlterTableExpr {
            name: self.table_ctx.name.to_string(),
            alter_options: AlterTableOperation::DropColumn { name },
        })
    }
}

pub struct AlterExprRenameGenerator {
    table_ctx: TableContextRef,
}

impl AlterExprRenameGenerator {
    /// Returns an [AlterExprRenameGenerator].
    pub fn new(table_ctx: &TableContextRef) -> Self {
        Self {
            table_ctx: table_ctx.clone(),
        }
    }
}

impl Generator<AlterTableExpr> for AlterExprRenameGenerator {
    type Error = Error;

    fn generate(&self) -> Result<AlterTableExpr> {
        let mut rng = rand::thread_rng();
        let mut new_table_name = rng.gen::<Word>().to_string();
        Ok(AlterTableExpr {
            name: self.table_ctx.name.to_string(),
            alter_options: AlterTableOperation::RenameTable { new_table_name },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::context::TableContext;
    use crate::generator::create_expr::CreateTableExprGenerator;
    use crate::generator::Generator;

    #[test]
    fn test_alter_table_expr_generator() {
        let create_expr = CreateTableExprGenerator::default()
            .columns(10)
            .generate()
            .unwrap();
        let table_ctx = Arc::new(TableContext::from(&create_expr));

        let alter_expr = AlterExprAddColumnGenerator::new(&table_ctx)
            .generate()
            .unwrap();

        let alter_expr = AlterExprRenameGenerator::new(&table_ctx)
            .generate()
            .unwrap();

        let alter_expr = AlterExprDropColumnGenerator::new(&table_ctx)
            .generate()
            .unwrap();
    }
}
