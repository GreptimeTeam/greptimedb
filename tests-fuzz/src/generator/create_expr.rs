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

use derive_builder::Builder;
use partition::partition::{PartitionBound, PartitionDef};
use rand::seq::SliceRandom;
use rand::Rng;
use snafu::{ensure, ResultExt};

use super::Generator;
use crate::error::{self, Error, Result};
use crate::fake::{random_capitalize_map, MappedGenerator, WordGenerator};
use crate::generator::{ColumnOptionGenerator, ConcreteDataTypeGenerator, Random};
use crate::ir::create_expr::{CreateDatabaseExprBuilder, CreateTableExprBuilder};
use crate::ir::{
    column_options_generator, generate_columns, generate_random_value,
    partible_column_options_generator, ts_column_options_generator, ColumnTypeGenerator,
    CreateDatabaseExpr, CreateTableExpr, Ident, PartibleColumnTypeGenerator, TsColumnTypeGenerator,
};

#[derive(Builder)]
#[builder(default, pattern = "owned")]
pub struct CreateTableExprGenerator<R: Rng + 'static> {
    columns: usize,
    #[builder(setter(into))]
    engine: String,
    partition: usize,
    if_not_exists: bool,
    #[builder(setter(into))]
    name: String,
    name_generator: Box<dyn Random<Ident, R>>,
    ts_column_type_generator: ConcreteDataTypeGenerator<R>,
    column_type_generator: ConcreteDataTypeGenerator<R>,
    partible_column_type_generator: ConcreteDataTypeGenerator<R>,
    partible_column_options_generator: ColumnOptionGenerator<R>,
    column_options_generator: ColumnOptionGenerator<R>,
    ts_column_options_generator: ColumnOptionGenerator<R>,
}

const DEFAULT_ENGINE: &str = "mito";

impl<R: Rng + 'static> Default for CreateTableExprGenerator<R> {
    fn default() -> Self {
        Self {
            columns: 0,
            engine: DEFAULT_ENGINE.to_string(),
            if_not_exists: false,
            partition: 0,
            name: String::new(),
            name_generator: Box::new(MappedGenerator::new(WordGenerator, random_capitalize_map)),
            ts_column_type_generator: Box::new(TsColumnTypeGenerator),
            column_type_generator: Box::new(ColumnTypeGenerator),
            partible_column_type_generator: Box::new(PartibleColumnTypeGenerator),
            partible_column_options_generator: Box::new(partible_column_options_generator),
            column_options_generator: Box::new(column_options_generator),
            ts_column_options_generator: Box::new(ts_column_options_generator),
        }
    }
}

impl<R: Rng + 'static> Generator<CreateTableExpr, R> for CreateTableExprGenerator<R> {
    type Error = Error;

    /// Generates the [CreateTableExpr].
    fn generate(&self, rng: &mut R) -> Result<CreateTableExpr> {
        ensure!(
            self.columns != 0,
            error::UnexpectedSnafu {
                violated: "The columns must larger than zero"
            }
        );

        let mut builder = CreateTableExprBuilder::default();
        let mut columns = Vec::with_capacity(self.columns);
        let mut primary_keys = vec![];
        let need_partible_column = self.partition > 1;
        let mut column_names = self.name_generator.choose(rng, self.columns);

        if self.columns == 1 {
            // Generates the ts column.
            // Safety: columns must large than 0.
            let name = column_names.pop().unwrap();
            let column = generate_columns(
                rng,
                vec![name.clone()],
                self.ts_column_type_generator.as_ref(),
                self.ts_column_options_generator.as_ref(),
            )
            .remove(0);

            if need_partible_column {
                // Generates partition bounds.
                let mut partition_bounds = Vec::with_capacity(self.partition);
                for _ in 0..self.partition - 1 {
                    partition_bounds.push(PartitionBound::Value(generate_random_value(
                        rng,
                        &column.column_type,
                        None,
                    )));
                    partition_bounds.sort();
                }
                partition_bounds.push(PartitionBound::MaxValue);
                builder.partition(PartitionDef::new(
                    vec![name.value.to_string()],
                    partition_bounds,
                ));
            }

            columns.push(column);
        } else {
            // Generates the partible column.
            if need_partible_column {
                // Safety: columns must large than 0.
                let name = column_names.pop().unwrap();
                let column = generate_columns(
                    rng,
                    vec![name.clone()],
                    self.partible_column_type_generator.as_ref(),
                    self.partible_column_options_generator.as_ref(),
                )
                .remove(0);

                // Generates partition bounds.
                let mut partition_bounds = Vec::with_capacity(self.partition);
                for _ in 0..self.partition - 1 {
                    partition_bounds.push(PartitionBound::Value(generate_random_value(
                        rng,
                        &column.column_type,
                        None,
                    )));
                    partition_bounds.sort();
                }
                partition_bounds.push(PartitionBound::MaxValue);
                builder.partition(PartitionDef::new(
                    vec![name.value.to_string()],
                    partition_bounds,
                ));
                columns.push(column);
            }
            // Generates the ts column.
            // Safety: columns must large than 1.
            let name = column_names.pop().unwrap();
            columns.extend(generate_columns(
                rng,
                vec![name],
                self.ts_column_type_generator.as_ref(),
                self.ts_column_options_generator.as_ref(),
            ));
            // Generates rest columns
            columns.extend(generate_columns(
                rng,
                column_names,
                self.column_type_generator.as_ref(),
                self.column_options_generator.as_ref(),
            ));
        }

        for (idx, column) in columns.iter().enumerate() {
            if column.is_primary_key() {
                primary_keys.push(idx);
            }
        }
        // Shuffles the primary keys.
        primary_keys.shuffle(rng);

        builder.columns(columns);
        builder.primary_keys(primary_keys);
        builder.engine(self.engine.to_string());
        builder.if_not_exists(self.if_not_exists);
        if self.name.is_empty() {
            builder.table_name(self.name_generator.gen(rng));
        } else {
            builder.table_name(self.name.to_string());
        }
        builder.build().context(error::BuildCreateTableExprSnafu)
    }
}

#[derive(Builder)]
#[builder(default, pattern = "owned")]
pub struct CreateDatabaseExprGenerator<R: Rng + 'static> {
    #[builder(setter(into))]
    database_name: String,
    name_generator: Box<dyn Random<Ident, R>>,
    if_not_exists: bool,
}

impl<R: Rng + 'static> Default for CreateDatabaseExprGenerator<R> {
    fn default() -> Self {
        Self {
            database_name: String::new(),
            name_generator: Box::new(MappedGenerator::new(WordGenerator, random_capitalize_map)),
            if_not_exists: false,
        }
    }
}

impl<R: Rng + 'static> Generator<CreateDatabaseExpr, R> for CreateDatabaseExprGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<CreateDatabaseExpr> {
        let mut builder = CreateDatabaseExprBuilder::default();
        builder.if_not_exists(self.if_not_exists);
        if self.database_name.is_empty() {
            builder.database_name(self.name_generator.gen(rng));
        } else {
            builder.database_name(self.database_name.to_string());
        }
        builder.build().context(error::BuildCreateDatabaseExprSnafu)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn test_float64() {
        let value = Value::from(0.047318541668048164);
        assert_eq!("0.047318541668048164", value.to_string());
        let value: f64 = "0.047318541668048164".parse().unwrap();
        assert_eq!("0.047318541668048164", value.to_string());
    }

    #[test]
    fn test_create_table_expr_generator() {
        let mut rng = rand::thread_rng();

        let expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .partition(3)
            .if_not_exists(true)
            .engine("mito2")
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        assert_eq!(expr.engine, "mito2");
        assert!(expr.if_not_exists);
        assert_eq!(expr.columns.len(), 10);
        assert_eq!(expr.partition.unwrap().partition_bounds().len(), 3);

        let expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .partition(1)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        assert_eq!(expr.columns.len(), 10);
        assert!(expr.partition.is_none());
    }

    #[test]
    fn test_create_table_expr_generator_deterministic() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);
        let expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .partition(3)
            .if_not_exists(true)
            .engine("mito2")
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();

        let serialized = serde_json::to_string(&expr).unwrap();
        let expected = r#"{"table_name":{"value":"tEmporIbUS","quote_style":null},"columns":[{"name":{"value":"IMpEdIT","quote_style":null},"column_type":{"String":null},"options":["PrimaryKey","NotNull"]},{"name":{"value":"natuS","quote_style":null},"column_type":{"Timestamp":{"Nanosecond":null}},"options":["TimeIndex"]},{"name":{"value":"ADIPisCI","quote_style":null},"column_type":{"Int16":{}},"options":[{"DefaultValue":{"Int16":4864}}]},{"name":{"value":"EXpEdita","quote_style":null},"column_type":{"Int64":{}},"options":["PrimaryKey"]},{"name":{"value":"cUlpA","quote_style":null},"column_type":{"Float64":{}},"options":["NotNull"]},{"name":{"value":"MOLeStIAs","quote_style":null},"column_type":{"Boolean":null},"options":["Null"]},{"name":{"value":"cUmquE","quote_style":null},"column_type":{"Float32":{}},"options":[{"DefaultValue":{"Float32":0.21569687}}]},{"name":{"value":"toTAm","quote_style":null},"column_type":{"Float64":{}},"options":["NotNull"]},{"name":{"value":"deBitIs","quote_style":null},"column_type":{"Float32":{}},"options":["Null"]},{"name":{"value":"QUi","quote_style":null},"column_type":{"Int64":{}},"options":["Null"]}],"if_not_exists":true,"partition":{"partition_columns":["IMpEdIT"],"partition_bounds":[{"Value":{"String":"򟘲"}},{"Value":{"String":"򴥫"}},"MaxValue"]},"engine":"mito2","options":{},"primary_keys":[0,3]}"#;
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_create_database_expr_generator() {
        let mut rng = rand::thread_rng();

        let expr = CreateDatabaseExprGeneratorBuilder::default()
            .if_not_exists(true)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        assert!(expr.if_not_exists);
    }

    #[test]
    fn test_create_database_expr_generator_deterministic() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);
        let expr = CreateDatabaseExprGeneratorBuilder::default()
            .if_not_exists(true)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();

        let serialized = serde_json::to_string(&expr).unwrap();
        let expected =
            r#"{"database_name":{"value":"eXPedITa","quote_style":null},"if_not_exists":true}"#;
        assert_eq!(expected, serialized);
    }
}
