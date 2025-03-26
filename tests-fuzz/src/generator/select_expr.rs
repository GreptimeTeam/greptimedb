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
use rand::seq::{IndexedRandom, SliceRandom};
use rand::Rng;

use crate::context::TableContextRef;
use crate::error::{Error, Result};
use crate::generator::Generator;
use crate::ir::select_expr::{Direction, SelectExpr};

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct SelectExprGenerator<R: Rng + 'static> {
    table_ctx: TableContextRef,
    #[builder(default = "8192")]
    max_limit: usize,
    #[builder(default)]
    _phantom: PhantomData<R>,
}

impl<R: Rng + 'static> Generator<SelectExpr, R> for SelectExprGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<SelectExpr> {
        let selection = rng.random_range(1..self.table_ctx.columns.len());
        let mut selected_columns = self
            .table_ctx
            .columns
            .choose_multiple(rng, selection)
            .cloned()
            .collect::<Vec<_>>();
        selected_columns.shuffle(rng);

        let order_by_selection = rng.random_range(1..selection);

        let order_by = selected_columns
            .choose_multiple(rng, order_by_selection)
            .map(|c| c.name.to_string())
            .collect::<Vec<_>>();

        let limit = rng.random_range(1..self.max_limit);

        let direction = if rng.random_bool(1.0 / 2.0) {
            Direction::Asc
        } else {
            Direction::Desc
        };

        Ok(SelectExpr {
            table_name: self.table_ctx.name.to_string(),
            columns: selected_columns,
            order_by,
            direction,
            limit,
        })
    }
}
