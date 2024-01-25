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

use derive_builder::Builder;
use rand::seq::SliceRandom;
use rand::Rng;

use crate::context::TableContextRef;
use crate::error::{Error, Result};
use crate::fake::WordGenerator;
use crate::generator::{Generator, Random};
use crate::ir::generate_random_value;
use crate::ir::insert_expr::InsertIntoExpr;

/// Generates [InsertIntoExpr].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct InsertExprGenerator<R: Rng + 'static> {
    table_ctx: TableContextRef,
    #[builder(default = "1")]
    rows: usize,
    #[builder(default = "Box::new(WordGenerator)")]
    word_generator: Box<dyn Random<String, R>>,
    #[builder(default)]
    _phantom: PhantomData<R>,
}

impl<R: Rng + 'static> Generator<InsertIntoExpr, R> for InsertExprGenerator<R> {
    type Error = Error;

    /// Generates the [CreateTableExpr].
    fn generate(&self, rng: &mut R) -> Result<InsertIntoExpr> {
        let mut columns = self.table_ctx.columns.clone();
        columns.shuffle(rng);

        let mut rows = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            let mut row = Vec::with_capacity(columns.len());
            for column in &columns {
                // TODO(weny): generates the special cases
                row.push(generate_random_value(
                    rng,
                    &column.column_type,
                    Some(self.word_generator.as_ref()),
                ));
            }

            rows.push(row);
        }

        Ok(InsertIntoExpr {
            table_name: self.table_ctx.name.to_string(),
            columns,
            rows,
        })
    }
}
