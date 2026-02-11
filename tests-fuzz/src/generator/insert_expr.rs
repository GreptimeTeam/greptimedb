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

use datatypes::value::Value;
use derive_builder::Builder;
use rand::Rng;
use rand::seq::{IndexedRandom, SliceRandom};

use super::TsValueGenerator;
use crate::context::TableContextRef;
use crate::error::{Error, Result};
use crate::fake::WordGenerator;
use crate::generator::{Generator, Random, ValueGenerator, ValueOverride};
use crate::ir::insert_expr::{InsertIntoExpr, RowValue};
use crate::ir::{Ident, generate_random_timestamp, generate_random_value};

/// Generates [InsertIntoExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct InsertExprGenerator<R: Rng + 'static> {
    table_ctx: TableContextRef,
    // Whether to omit all columns, i.e. INSERT INTO table_name VALUES (...)
    omit_column_list: bool,
    #[builder(default)]
    required_columns: Vec<Ident>,
    #[builder(default)]
    value_overrides: Option<ValueOverride<R>>,
    #[builder(default = "1")]
    rows: usize,
    #[builder(default = "Box::new(WordGenerator)")]
    word_generator: Box<dyn Random<Ident, R>>,
    #[builder(default = "Box::new(generate_random_value)")]
    value_generator: ValueGenerator<R>,
    #[builder(default = "Box::new(generate_random_timestamp)")]
    ts_value_generator: TsValueGenerator<R>,
    #[builder(default)]
    _phantom: PhantomData<R>,
}

impl<R: Rng + 'static> Generator<InsertIntoExpr, R> for InsertExprGenerator<R> {
    type Error = Error;

    /// Generates the [InsertIntoExpr].
    fn generate(&self, rng: &mut R) -> Result<InsertIntoExpr> {
        let mut values_columns = vec![];
        if self.omit_column_list {
            // If omit column list, then all columns are required in the values list
            values_columns.clone_from(&self.table_ctx.columns);
        } else {
            for column in &self.table_ctx.columns {
                let is_required = self.required_columns.contains(&column.name);
                let can_omit = column.is_nullable() || column.has_default_value();

                // 50% chance to omit a column if it's not required
                if is_required || !can_omit || rng.random_bool(0.5) {
                    values_columns.push(column.clone());
                }
            }
            values_columns.shuffle(rng);

            // If all columns are omitted, pick a random column
            if values_columns.is_empty() {
                values_columns.push(self.table_ctx.columns.choose(rng).unwrap().clone());
            }
        }

        let mut values_list = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            let mut row = Vec::with_capacity(values_columns.len());
            for column in &values_columns {
                if let Some(override_fn) = self.value_overrides.as_ref()
                    && let Some(value) = override_fn(column, rng)
                {
                    row.push(value);
                    continue;
                }
                if column.is_nullable() && rng.random_bool(0.2) {
                    row.push(RowValue::Value(Value::Null));
                    continue;
                }

                // if column.has_default_value() && rng.random_bool(0.2) {
                //     row.push(RowValue::Default);
                //     continue;
                // }
                if column.is_time_index() {
                    row.push(RowValue::Value((self.ts_value_generator)(
                        rng,
                        column.timestamp_type().unwrap(),
                    )));
                } else {
                    row.push(RowValue::Value((self.value_generator)(
                        rng,
                        &column.column_type,
                        Some(self.word_generator.as_ref()),
                    )));
                }
            }

            values_list.push(row);
        }

        Ok(InsertIntoExpr {
            table_name: self.table_ctx.name.clone(),
            omit_column_list: self.omit_column_list,
            columns: values_columns,
            values_list,
        })
    }
}
