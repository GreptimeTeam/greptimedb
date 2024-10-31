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

use std::collections::HashMap;

use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use derive_builder::Builder;
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use partition::partition::{PartitionBound, PartitionDef};
use rand::seq::SliceRandom;
use rand::Rng;
use snafu::{ensure, ResultExt};

use super::Generator;
use crate::context::TableContextRef;
use crate::error::{self, Error, Result};
use crate::fake::{random_capitalize_map, MappedGenerator, WordGenerator};
use crate::generator::{ColumnOptionGenerator, ConcreteDataTypeGenerator, Random};
use crate::ir::create_expr::{ColumnOption, CreateDatabaseExprBuilder, CreateTableExprBuilder};
use crate::ir::{
    column_options_generator, generate_columns, generate_partition_bounds, generate_random_value,
    partible_column_options_generator, primary_key_options_generator, ts_column_options_generator,
    Column, ColumnTypeGenerator, CreateDatabaseExpr, CreateTableExpr, Ident,
    PartibleColumnTypeGenerator, StringColumnTypeGenerator, TsColumnTypeGenerator,
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
    name: Ident,
    #[builder(setter(into))]
    with_clause: HashMap<String, String>,
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
            name: Ident::new(""),
            with_clause: HashMap::default(),
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
                let partition_def = generate_partition_def(
                    self.partition,
                    column.column_type.clone(),
                    name.clone(),
                );
                builder.partition(partition_def);
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
            builder.table_name(self.name.clone());
        }
        if !self.with_clause.is_empty() {
            let mut options = HashMap::new();
            for (key, value) in &self.with_clause {
                options.insert(key.to_string(), Value::from(value.to_string()));
            }
            builder.options(options);
        }
        builder.build().context(error::BuildCreateTableExprSnafu)
    }
}

fn generate_partition_def(
    partitions: usize,
    column_type: ConcreteDataType,
    column_name: Ident,
) -> PartitionDef {
    let bounds = generate_partition_bounds(&column_type, partitions - 1);
    let mut partition_bounds = Vec::with_capacity(partitions);

    let first_bound = bounds[0].clone();
    partition_bounds.push(PartitionBound::Expr(PartitionExpr::new(
        Operand::Column(column_name.to_string()),
        RestrictedOp::Lt,
        Operand::Value(first_bound),
    )));
    for bound_idx in 1..bounds.len() {
        partition_bounds.push(PartitionBound::Expr(PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Column(column_name.to_string()),
                RestrictedOp::GtEq,
                Operand::Value(bounds[bound_idx - 1].clone()),
            )),
            RestrictedOp::And,
            Operand::Expr(PartitionExpr::new(
                Operand::Column(column_name.to_string()),
                RestrictedOp::Lt,
                Operand::Value(bounds[bound_idx].clone()),
            )),
        )));
    }
    let last_bound = bounds.last().unwrap().clone();
    partition_bounds.push(PartitionBound::Expr(PartitionExpr::new(
        Operand::Column(column_name.to_string()),
        RestrictedOp::GtEq,
        Operand::Value(last_bound),
    )));

    PartitionDef::new(vec![column_name.to_string()], partition_bounds)
}

/// Generate a physical table with 2 columns: ts of TimestampType::Millisecond as time index and val of Float64Type.
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct CreatePhysicalTableExprGenerator<R: Rng + 'static> {
    #[builder(default = "Box::new(WordGenerator)")]
    name_generator: Box<dyn Random<Ident, R>>,
    #[builder(default = "false")]
    if_not_exists: bool,
    #[builder(default, setter(into))]
    with_clause: HashMap<String, String>,
}

impl<R: Rng + 'static> Generator<CreateTableExpr, R> for CreatePhysicalTableExprGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<CreateTableExpr> {
        let mut options = HashMap::with_capacity(self.with_clause.len() + 1);
        options.insert("physical_metric_table".to_string(), Value::from(""));
        for (key, value) in &self.with_clause {
            options.insert(key.to_string(), Value::from(value.to_string()));
        }

        Ok(CreateTableExpr {
            table_name: self.name_generator.gen(rng),
            columns: vec![
                Column {
                    name: Ident::new("ts"),
                    column_type: ConcreteDataType::timestamp_millisecond_datatype(),
                    options: vec![ColumnOption::TimeIndex],
                },
                Column {
                    name: Ident::new("val"),
                    column_type: ConcreteDataType::float64_datatype(),
                    options: vec![],
                },
            ],
            if_not_exists: self.if_not_exists,
            partition: None,
            engine: "metric".to_string(),
            options,
            primary_keys: vec![],
        })
    }
}

/// Generate a logical table based on an existing physical table.
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct CreateLogicalTableExprGenerator<R: Rng + 'static> {
    physical_table_ctx: TableContextRef,
    labels: usize,
    if_not_exists: bool,
    #[builder(default = "Box::new(WordGenerator)")]
    name_generator: Box<dyn Random<Ident, R>>,
}

impl<R: Rng + 'static> Generator<CreateTableExpr, R> for CreateLogicalTableExprGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<CreateTableExpr> {
        // Currently we mock the usage of GreptimeDB as Prometheus' backend, the physical table must have two columns.
        ensure!(
            self.physical_table_ctx.columns.len() == 2,
            error::UnexpectedSnafu {
                violated: "The physical table must have two columns"
            }
        );

        // Generates the logical table columns based on the physical table.
        let logical_table_name = self
            .physical_table_ctx
            .generate_unique_table_name(rng, self.name_generator.as_ref());
        let mut logical_table = CreateTableExpr {
            table_name: logical_table_name,
            columns: self.physical_table_ctx.columns.clone(),
            if_not_exists: self.if_not_exists,
            partition: None,
            engine: "metric".to_string(),
            options: [(
                "on_physical_table".to_string(),
                self.physical_table_ctx.name.value.clone().into(),
            )]
            .into(),
            primary_keys: vec![],
        };

        let column_names = self.name_generator.choose(rng, self.labels);
        logical_table.columns.extend(generate_columns(
            rng,
            column_names,
            &StringColumnTypeGenerator,
            Box::new(primary_key_options_generator),
        ));

        // Currently only the `primary key` option is kept in physical table,
        // so we only keep the `primary key` option in the logical table for fuzz test.
        let mut primary_keys = vec![];
        for (idx, column) in logical_table.columns.iter().enumerate() {
            if column.is_primary_key() {
                primary_keys.push(idx);
            }
        }
        primary_keys.shuffle(rng);
        logical_table.primary_keys = primary_keys;

        Ok(logical_table)
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
    use std::sync::Arc;

    use datatypes::data_type::ConcreteDataType;
    use datatypes::value::Value;
    use rand::SeedableRng;

    use super::*;
    use crate::context::TableContext;

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
        let expected = r#"{"table_name":{"value":"animI","quote_style":null},"columns":[{"name":{"value":"IMpEdIT","quote_style":null},"column_type":{"Float64":{}},"options":["PrimaryKey","NotNull"]},{"name":{"value":"natuS","quote_style":null},"column_type":{"Timestamp":{"Millisecond":null}},"options":["TimeIndex"]},{"name":{"value":"ADIPisCI","quote_style":null},"column_type":{"Float64":{}},"options":["Null"]},{"name":{"value":"EXpEdita","quote_style":null},"column_type":{"Int16":{}},"options":[{"DefaultValue":{"Int16":4864}}]},{"name":{"value":"cUlpA","quote_style":null},"column_type":{"Int64":{}},"options":["PrimaryKey"]},{"name":{"value":"MOLeStIAs","quote_style":null},"column_type":{"Float64":{}},"options":["NotNull"]},{"name":{"value":"cUmquE","quote_style":null},"column_type":{"Boolean":null},"options":["Null"]},{"name":{"value":"toTAm","quote_style":null},"column_type":{"Float32":{}},"options":[{"DefaultValue":{"Float32":0.21569687}}]},{"name":{"value":"deBitIs","quote_style":null},"column_type":{"Float64":{}},"options":["NotNull"]},{"name":{"value":"QUi","quote_style":null},"column_type":{"Float32":{}},"options":["Null"]}],"if_not_exists":true,"partition":{"partition_columns":["IMpEdIT"],"partition_bounds":[{"Expr":{"lhs":{"Column":"IMpEdIT"},"op":"Lt","rhs":{"Value":{"Float64":5.992310449541053e307}}}},{"Expr":{"lhs":{"Expr":{"lhs":{"Column":"IMpEdIT"},"op":"GtEq","rhs":{"Value":{"Float64":5.992310449541053e307}}}},"op":"And","rhs":{"Expr":{"lhs":{"Column":"IMpEdIT"},"op":"Lt","rhs":{"Value":{"Float64":1.1984620899082105e308}}}}}},{"Expr":{"lhs":{"Column":"IMpEdIT"},"op":"GtEq","rhs":{"Value":{"Float64":1.1984620899082105e308}}}}]},"engine":"mito2","options":{},"primary_keys":[0,4]}"#;
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_create_logical_table_expr_generator() {
        let mut rng = rand::thread_rng();

        let physical_table_expr = CreatePhysicalTableExprGeneratorBuilder::default()
            .if_not_exists(false)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        assert_eq!(physical_table_expr.engine, "metric");
        assert_eq!(physical_table_expr.columns.len(), 2);

        let physical_ts = physical_table_expr.columns.iter().position(|column| {
            column
                .options
                .iter()
                .any(|option| option == &ColumnOption::TimeIndex)
        });
        let physical_ts_name = physical_table_expr.columns[physical_ts.unwrap()]
            .name
            .value
            .to_string();

        let physical_table_ctx = Arc::new(TableContext::from(&physical_table_expr));

        let logical_table_expr = CreateLogicalTableExprGeneratorBuilder::default()
            .physical_table_ctx(physical_table_ctx)
            .labels(5)
            .if_not_exists(false)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let logical_ts = logical_table_expr.columns.iter().position(|column| {
            column
                .options
                .iter()
                .any(|option| option == &ColumnOption::TimeIndex)
        });
        let logical_ts_name = logical_table_expr.columns[logical_ts.unwrap()]
            .name
            .value
            .to_string();

        assert_eq!(logical_table_expr.engine, "metric");
        assert_eq!(logical_table_expr.columns.len(), 7);
        assert_eq!(logical_ts_name, physical_ts_name);
        assert!(logical_table_expr
            .columns
            .iter()
            .all(
                |column| column.column_type != ConcreteDataType::string_datatype()
                    || column
                        .options
                        .iter()
                        .any(|option| option == &ColumnOption::PrimaryKey)
            ));
    }

    #[test]
    fn test_create_logical_table_expr_generator_deterministic() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);
        let physical_table_expr = CreatePhysicalTableExprGeneratorBuilder::default()
            .if_not_exists(false)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let physical_table_serialized = serde_json::to_string(&physical_table_expr).unwrap();
        let physical_table_expected = r#"{"table_name":{"value":"expedita","quote_style":null},"columns":[{"name":{"value":"ts","quote_style":null},"column_type":{"Timestamp":{"Millisecond":null}},"options":["TimeIndex"]},{"name":{"value":"val","quote_style":null},"column_type":{"Float64":{}},"options":[]}],"if_not_exists":false,"partition":null,"engine":"metric","options":{"physical_metric_table":{"String":""}},"primary_keys":[]}"#;
        assert_eq!(physical_table_expected, physical_table_serialized);

        let physical_table_ctx = Arc::new(TableContext::from(&physical_table_expr));

        let logical_table_expr = CreateLogicalTableExprGeneratorBuilder::default()
            .physical_table_ctx(physical_table_ctx)
            .labels(5)
            .if_not_exists(false)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();

        let logical_table_serialized = serde_json::to_string(&logical_table_expr).unwrap();
        let logical_table_expected = r#"{"table_name":{"value":"impedit","quote_style":null},"columns":[{"name":{"value":"ts","quote_style":null},"column_type":{"Timestamp":{"Millisecond":null}},"options":["TimeIndex"]},{"name":{"value":"val","quote_style":null},"column_type":{"Float64":{}},"options":[]},{"name":{"value":"qui","quote_style":null},"column_type":{"String":null},"options":["PrimaryKey"]},{"name":{"value":"totam","quote_style":null},"column_type":{"String":null},"options":["PrimaryKey"]},{"name":{"value":"molestias","quote_style":null},"column_type":{"String":null},"options":["PrimaryKey"]},{"name":{"value":"natus","quote_style":null},"column_type":{"String":null},"options":["PrimaryKey"]},{"name":{"value":"cumque","quote_style":null},"column_type":{"String":null},"options":["PrimaryKey"]}],"if_not_exists":false,"partition":null,"engine":"metric","options":{"on_physical_table":{"String":"expedita"}},"primary_keys":[2,5,3,6,4]}"#;
        assert_eq!(logical_table_expected, logical_table_serialized);
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
