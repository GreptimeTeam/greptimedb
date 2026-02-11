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

pub mod alter_expr;
pub mod create_expr;
pub mod insert_expr;
pub mod repartition_expr;
pub mod select_expr;

use std::fmt;

use datatypes::data_type::ConcreteDataType;
use datatypes::types::TimestampType;
use datatypes::value::Value;
use rand::Rng;

use crate::error::Error;
use crate::ir::create_expr::ColumnOption;
use crate::ir::{AlterTableExpr, Column, CreateTableExpr, Ident, RowValue};

pub type CreateTableExprGenerator<R> =
    Box<dyn Generator<CreateTableExpr, R, Error = Error> + Sync + Send>;

pub type AlterTableExprGenerator<R> =
    Box<dyn Generator<AlterTableExpr, R, Error = Error> + Sync + Send>;

pub type ColumnOptionGenerator<R> = Box<dyn Fn(&mut R, &ConcreteDataType) -> Vec<ColumnOption>>;

pub type ConcreteDataTypeGenerator<R> = Box<dyn Random<ConcreteDataType, R>>;

pub type ValueGenerator<R> =
    Box<dyn Fn(&mut R, &ConcreteDataType, Option<&dyn Random<Ident, R>>) -> Value>;

pub type TsValueGenerator<R> = Box<dyn Fn(&mut R, TimestampType) -> Value>;

pub type ValueOverride<R> = Box<dyn Fn(&Column, &mut R) -> Option<RowValue>>;

pub trait Generator<T, R: Rng> {
    type Error: Sync + Send + fmt::Debug;

    fn generate(&self, rng: &mut R) -> Result<T, Self::Error>;
}

pub trait Random<T, R: Rng> {
    /// Generates a random element.
    fn generate(&self, rng: &mut R) -> T {
        self.choose(rng, 1).remove(0)
    }

    /// Uniformly sample `amount` distinct elements.
    fn choose(&self, rng: &mut R, amount: usize) -> Vec<T>;
}

#[macro_export]
macro_rules! impl_random {
    ($type: ident, $value:ident, $values: ident) => {
        impl<R: Rng> Random<$type, R> for $value {
            fn choose(&self, rng: &mut R, amount: usize) -> Vec<$type> {
                // Collects the elements in deterministic order first.
                let mut result = std::collections::BTreeSet::new();
                while result.len() != amount {
                    result.insert($values.choose(rng).unwrap().clone());
                }
                let mut result = result.into_iter().map(Into::into).collect::<Vec<_>>();
                // Shuffles the result slice.
                result.shuffle(rng);
                result
            }
        }
    };
}
