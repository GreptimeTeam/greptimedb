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

use common_base::readable_size::ReadableSize;
use common_query::AddColumnLocation;
use datatypes::data_type::ConcreteDataType;
use derive_builder::Builder;
use rand::Rng;
use snafu::ensure;
use strum::IntoEnumIterator;

use crate::context::TableContextRef;
use crate::error::{self, Error, Result};
use crate::fake::WordGenerator;
use crate::generator::{ColumnOptionGenerator, ConcreteDataTypeGenerator, Generator, Random};
use crate::ir::alter_expr::{AlterTableExpr, AlterTableOperation, AlterTableOption};
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
            alter_kinds: AlterTableOperation::AddColumn { column, location },
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
            alter_kinds: AlterTableOperation::DropColumn { name },
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
            alter_kinds: AlterTableOperation::RenameTable { new_table_name },
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
            alter_kinds: AlterTableOperation::ModifyDataType {
                column: Column {
                    name: changed.name,
                    column_type: to_type,
                    options: vec![],
                },
            },
        })
    }
}

/// Generates the [AlterTableOperation::SetTableOptions] of [AlterTableExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct AlterExprSetTableOptionsGenerator<R: Rng> {
    table_ctx: TableContextRef,
    #[builder(default)]
    _phantom: PhantomData<R>,
}

impl<R: Rng> Generator<AlterTableExpr, R> for AlterExprSetTableOptionsGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<AlterTableExpr> {
        let all_options = AlterTableOption::iter().collect::<Vec<_>>();
        // Generate random distinct options
        let mut option_templates_idx = vec![];
        for _ in 1..rng.gen_range(2..=all_options.len()) {
            let option = rng.gen_range(0..all_options.len());
            if !option_templates_idx.contains(&option) {
                option_templates_idx.push(option);
            }
        }
        let options = option_templates_idx
            .iter()
            .map(|idx| match all_options[*idx] {
                AlterTableOption::Ttl(_) => {
                    let ttl: u32 = rng.gen();
                    AlterTableOption::Ttl((ttl as i64).into())
                }
                AlterTableOption::TwcsTimeWindow(_) => {
                    let time_window: u32 = rng.gen();
                    AlterTableOption::TwcsTimeWindow((time_window as i64).into())
                }
                AlterTableOption::TwcsMaxOutputFileSize(_) => {
                    let max_output_file_size: u64 = rng.gen();
                    AlterTableOption::TwcsMaxOutputFileSize(ReadableSize(max_output_file_size))
                }
                AlterTableOption::TwcsMaxInactiveWindowRuns(_) => {
                    let max_inactive_window_runs: u64 = rng.gen();
                    AlterTableOption::TwcsMaxInactiveWindowRuns(max_inactive_window_runs)
                }
                AlterTableOption::TwcsMaxActiveWindowFiles(_) => {
                    let max_active_window_files: u64 = rng.gen();
                    AlterTableOption::TwcsMaxActiveWindowFiles(max_active_window_files)
                }
                AlterTableOption::TwcsMaxActiveWindowRuns(_) => {
                    let max_active_window_runs: u64 = rng.gen();
                    AlterTableOption::TwcsMaxActiveWindowRuns(max_active_window_runs)
                }
                AlterTableOption::TwcsMaxInactiveWindowFiles(_) => {
                    let max_inactive_window_files: u64 = rng.gen();
                    AlterTableOption::TwcsMaxInactiveWindowFiles(max_inactive_window_files)
                }
            })
            .collect();
        Ok(AlterTableExpr {
            table_name: self.table_ctx.name.clone(),
            alter_kinds: AlterTableOperation::SetTableOptions { options },
        })
    }
}

/// Generates the [AlterTableOperation::UnsetTableOptions] of [AlterTableExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct AlterExprUnsetTableOptionsGenerator<R: Rng> {
    table_ctx: TableContextRef,
    #[builder(default)]
    _phantom: PhantomData<R>,
}

impl<R: Rng> Generator<AlterTableExpr, R> for AlterExprUnsetTableOptionsGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<AlterTableExpr> {
        let all_options = AlterTableOption::iter().collect::<Vec<_>>();
        // Generate random distinct options
        let mut option_templates_idx = vec![];
        for _ in 1..rng.gen_range(2..=all_options.len()) {
            let option = rng.gen_range(0..all_options.len());
            if !option_templates_idx.contains(&option) {
                option_templates_idx.push(option);
            }
        }
        let options = option_templates_idx
            .iter()
            .map(|idx| all_options[*idx].key().to_string())
            .collect();
        Ok(AlterTableExpr {
            table_name: self.table_ctx.name.clone(),
            alter_kinds: AlterTableOperation::UnsetTableOptions { keys: options },
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
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_kinds":{"AddColumn":{"column":{"name":{"value":"velit","quote_style":null},"column_type":{"Int32":{}},"options":[{"DefaultValue":{"Int32":1606462472}}]},"location":null}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprRenameGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_kinds":{"RenameTable":{"new_table_name":{"value":"nihil","quote_style":null}}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprDropColumnGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_kinds":{"DropColumn":{"name":{"value":"cUmquE","quote_style":null}}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprModifyDataTypeGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_kinds":{"ModifyDataType":{"column":{"name":{"value":"toTAm","quote_style":null},"column_type":{"Int64":{}},"options":[]}}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprSetTableOptionsGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_kinds":{"SetTableOptions":{"options":[{"TwcsMaxActiveWindowRuns":14908016120444947142},{"TwcsMaxActiveWindowFiles":5840340123887173415},{"TwcsMaxOutputFileSize":17740311466571102265}]}}}"#;
        assert_eq!(expected, serialized);

        let expr = AlterExprUnsetTableOptionsGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"alter_kinds":{"UnsetTableOptions":{"keys":["compaction.twcs.max_active_window_runs"]}}}"#;
        assert_eq!(expected, serialized);
    }
}
