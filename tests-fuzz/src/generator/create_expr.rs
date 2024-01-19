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

use faker_rand::lorem::Word;
use partition::partition::{PartitionBound, PartitionDef};
use rand::seq::SliceRandom;
use rand::Rng;
use snafu::{ensure, ResultExt};

use super::Generator;
use crate::error::{self, Error, Result};
use crate::ir::create_expr::{ColumnOption, CreateTableExprBuilder};
use crate::ir::{
    self, generate_random_value, Column, CreateTableExpr, PartibleColumn, TsColumn,
    PARTIBLE_DATA_TYPES,
};

pub struct CreateTableExprGenerator {
    columns: usize,
    engine: String,
    partition: usize,
    if_not_exists: bool,
}

const DEFAULT_ENGINE: &str = "mito";

impl Default for CreateTableExprGenerator {
    fn default() -> Self {
        Self {
            columns: 0,
            engine: DEFAULT_ENGINE.to_string(),
            if_not_exists: false,
            partition: 0,
        }
    }
}

impl CreateTableExprGenerator {
    /// Sets column num.
    pub fn columns(mut self, columns: usize) -> Self {
        self.columns = columns;
        self
    }

    /// Sets the `if_not_exists`.
    pub fn create_is_not_exists(mut self, v: bool) -> Self {
        self.if_not_exists = v;
        self
    }

    /// Sets the `engine`.
    pub fn engine(mut self, engine: &str) -> Self {
        self.engine = engine.to_string();
        self
    }

    /// Sets the partition num.
    /// If there is no primary key column,
    /// it appends a primary key atomically.
    pub fn partitions(mut self, partition: usize) -> Self {
        self.partition = partition;
        self
    }

    /// Generates the [CreateTableExpr].
    pub fn generate(&self) -> Result<CreateTableExpr> {
        ensure!(
            self.columns != 0,
            error::UnexpectedSnafu {
                violated: "The columns must larger than zero"
            }
        );

        let mut builder = CreateTableExprBuilder::default();
        let mut columns = Vec::with_capacity(self.columns + 1);
        let mut primary_keys = vec![];
        let mut rng = rand::thread_rng();

        // Generates columns.
        for i in 0..self.columns {
            let mut column = rng.gen::<Column>();
            let is_primary_key = column
                .options
                .iter()
                .any(|option| option == &ColumnOption::PrimaryKey);
            if is_primary_key {
                primary_keys.push(i);
            }
            columns.push(column);
        }

        // Shuffles the primary keys.
        primary_keys.shuffle(&mut rng);
        let partition = if self.partition > 1 {
            // Finds a partible primary keys.
            let partible_primary_keys = primary_keys
                .iter()
                .flat_map(|i| {
                    if PARTIBLE_DATA_TYPES.contains(&columns[*i].column_type) {
                        Some(*i)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            // Generates the partition.
            if partible_primary_keys.is_empty() {
                columns.push(rng.gen::<PartibleColumn>().0);
                primary_keys.push(columns.len() - 1);
            }
            let primary_key_idx = primary_keys[rng.gen_range(0..primary_keys.len())];
            let primary_column = &columns[primary_key_idx];

            // Generates partition bounds.
            let mut partition_bounds = Vec::with_capacity(self.partition);
            for _ in 0..self.partition - 1 {
                partition_bounds.push(PartitionBound::Value(generate_random_value(
                    &primary_column.column_type,
                )));
                partition_bounds.sort();
            }
            partition_bounds.push(PartitionBound::MaxValue);

            Some(PartitionDef::new(
                vec![primary_column.name.to_string()],
                partition_bounds,
            ))
        } else {
            None
        };
        // Generates ts column.
        columns.push(rng.gen::<TsColumn>().0);

        builder.columns(columns);
        builder.primary_keys(primary_keys);
        builder.engine(self.engine.to_string());
        builder.if_not_exists(self.if_not_exists);
        builder.name(rng.gen::<Word>().to_string());
        builder.partition(partition);
        builder.build().context(error::BuildCreateTableExprSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_expr_generator() {
        let expr = CreateTableExprGenerator::default()
            .columns(10)
            .partitions(3)
            .create_is_not_exists(true)
            .engine("mito2")
            .generate()
            .unwrap();
        assert_eq!(expr.engine, "mito2");
        assert!(expr.if_not_exists);
        assert!(expr.columns.len() >= 11);
        assert_eq!(expr.partition.unwrap().partition_bounds().len(), 3);

        let expr = CreateTableExprGenerator::default()
            .columns(10)
            .partitions(1)
            .generate()
            .unwrap();
        assert_eq!(expr.columns.len(), 11);
        assert!(expr.partition.is_none());
    }
}
