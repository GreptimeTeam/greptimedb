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

use std::marker::PhantomData;

use common_query::AddColumnLocation;
use datatypes::data_type::ConcreteDataType;
use derive_builder::Builder;
use rand::Rng;
use snafu::ensure;

use crate::context::TableContextRef;
use crate::error::{self, Error, Result};
use crate::fake::WordGenerator;
use crate::generator::{ColumnOptionGenerator, ConcreteDataTypeGenerator, Generator, Random};
use crate::ir::alter_expr::{AlterTableExpr, AlterTableOperation};
use crate::ir::create_expr::ColumnOption;
use crate::ir::{
    droppable_columns, generate_columns, generate_random_value, modifiable_columns, Column,
    ColumnTypeGenerator, Ident,
};

fn add_column_options_generator<R: Rng>(
    rng: &mut R,
    column_type: &ConcreteDataType,
) -> Vec<ColumnOption> {
    // 0 -> NULL
    // 1 -> DEFAULT VALUE
    // 2 -> PRIMARY KEY + DEFAULT VALUE
    let idx = rng.gen_range(0..3);
    match idx {
        0 => vec![ColumnOption::Null],
        1 => {
            vec![ColumnOption::DefaultValue(generate_random_value(
                rng,
                column_type,
                None,
            ))]
        }
        2 => {
            vec![
                ColumnOption::PrimaryKey,
                ColumnOption::DefaultValue(generate_random_value(rng, column_type, None)),
            ]
        }
        _ => unreachable!(),
    }
}

/// Generates the [AlterTableOperation::AddColumn] of [AlterTableExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct AlterExprAddColumnGenerator<R: Rng + 'static> {
    table_ctx: TableContextRef,
    #[builder(default)]
    location: bool,
    #[builder(default = "Box::new(WordGenerator)")]
    name_generator: Box<dyn Random<Ident, R>>,
    #[builder(default = "Box::new(add_column_options_generator)")]
    column_options_generator: ColumnOptionGenerator<R>,
    #[builder(default = "Box::new(ColumnTypeGenerator)")]
    column_type_generator: ConcreteDataTypeGenerator<R>,
}

impl<R: Rng + 'static> Generator<AlterTableExpr, R> for AlterExprAddColumnGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<AlterTableExpr> {
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

        let name = self
            .table_ctx
            .generate_unique_column_name(rng, self.name_generator.as_ref());
        let column = generate_columns(
            rng,
            vec![name],
            self.column_type_generator.as_ref(),
            self.column_options_generator.as_ref(),
        )
        .remove(0);
        Ok(AlterTableExpr {
            table_name: self.table_ctx.name.clone(),
            alter_options: AlterTableOperation::AddColumn { column, location },
        })
    }
}

/// Generates the [AlterTableOperation::DropColumn] of [AlterTableExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct AlterExprDropColumnGenerator<R> {
    table_ctx: TableContextRef,
    #[builder(default)]
    _phantom: PhantomData<R>,
}

impl<R: Rng> Generator<AlterTableExpr, R> for AlterExprDropColumnGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<AlterTableExpr> {
        let droppable = droppable_columns(&self.table_ctx.columns);
        ensure!(!droppable.is_empty(), error::DroppableColumnsSnafu);
        let name = droppable[rng.gen_range(0..droppable.len())].name.clone();
        Ok(AlterTableExpr {
            table_name: self.table_ctx.name.clone(),
            alter_options: AlterTableOperation::DropColumn { name },
        })
    }
}

/// Generates the [AlterTableOperation::RenameTable] of [AlterTableExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct AlterExprRenameGenerator<R: Rng> {
    table_ctx: TableContextRef,
    #[builder(default = "Box::new(WordGenerator)")]
    name_generator: Box<dyn Random<Ident, R>>,
}

impl<R: Rng> Generator<AlterTableExpr, R> for AlterExprRenameGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<AlterTableExpr> {
        let new_table_name = self
            .table_ctx
            .generate_unique_table_name(rng, self.name_generator.as_ref());
        Ok(AlterTableExpr {
            table_name: self.table_ctx.name.clone(),
            alter_options: AlterTableOperation::RenameTable { new_table_name },
        })
    }
}

/// Generates the [AlterTableOperation::ModifyDataType] of [AlterTableExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct AlterExprModifyDataTypeGenerator<R: Rng> {
    table_ctx: TableContextRef,
    #[builder(default = "Box::new(ColumnTypeGenerator)")]
    column_type_generator: ConcreteDataTypeGenerator<R>,
}

impl<R: Rng> Generator<AlterTableExpr, R> for AlterExprModifyDataTypeGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<AlterTableExpr> {
        let modifiable = modifiable_columns(&self.table_ctx.columns);
        let changed = modifiable[rng.gen_range(0..modifiable.len())].clone();
        let mut to_type = self.column_type_generator.gen(rng);
        while !changed.column_type.can_arrow_type_cast_to(&to_type) {
            to_type = self.column_type_generator.gen(rng);
        }

        Ok(AlterTableExpr {
            table_name: self.table_ctx.name.clone(),
            alter_options: AlterTableOperation::ModifyDataType {
                column: Column {
                    name: changed.name,
                    column_type: to_type,
                    options: vec![],
                },
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::SeedableRng;

    use super::*;
    use crate::context::TableContext;
    use crate::generator::create_expr::CreateTableExprGeneratorBuilder;
    use crate::generator::Generator;

    #[test]
    fn test_alter_table_expr_generator_deterministic() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);
        let create_expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let table_ctx = Arc::new(TableContext::from(&create_expr));

        let expr = AlterExprAddColumnGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_options":{"AddColumn":{"column":{"name":{"value":"velit","quote_style":null},"column_type":{"Int32":{}},"options":[{"DefaultValue":{"Int32":1606462472}}]},"location":null}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprRenameGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_options":{"RenameTable":{"new_table_name":{"value":"nihil","quote_style":null}}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprDropColumnGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_options":{"DropColumn":{"name":{"value":"cUmquE","quote_style":null}}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprModifyDataTypeGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_options":{"ModifyDataType":{"column":{"name":{"value":"toTAm","quote_style":null},"column_type":{"Int64":{}},"options":[]}}}}"#;
        assert_eq!(expected, serialized);
    }
}
