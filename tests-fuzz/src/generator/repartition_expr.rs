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
use rand::Rng;

use crate::context::TableContextRef;
use crate::error::{Error, Result};
use crate::generator::Generator;
use crate::ir::partition_expr::{SimplePartitions, generate_unique_bound};
use crate::ir::repartition_expr::{MergePartitionExpr, SplitPartitionExpr};

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct SplitPartitionExprGenerator {
    table_ctx: TableContextRef,
}

impl<R: Rng + 'static> Generator<SplitPartitionExpr, R> for SplitPartitionExprGenerator {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<SplitPartitionExpr> {
        let table_name = self.table_ctx.name.clone();
        let partition_def = self
            .table_ctx
            .partition
            .as_ref()
            .expect("expected partition def");
        let mut partitions =
            SimplePartitions::from_exprs(partition_def.columns[0].clone(), &partition_def.exprs)?;
        let column = self
            .table_ctx
            .columns
            .iter()
            .find(|c| c.name.value == partitions.column_name.value)
            .unwrap_or_else(|| {
                panic!(
                    "partition column not found: {}, columns: {:?}",
                    partitions.column_name.value,
                    self.table_ctx
                        .columns
                        .iter()
                        .map(|c| &c.name.value)
                        .collect::<Vec<_>>()
                )
            });
        let new_bound = generate_unique_bound(rng, &column.column_type, &partitions.bounds)?;
        let insert_pos = partitions.insert_bound(new_bound)?;

        let from_expr = partition_def.exprs[insert_pos].clone();
        let new_exprs = partitions.generate()?;
        let left = new_exprs[insert_pos].clone();
        let right = new_exprs[insert_pos + 1].clone();
        Ok(SplitPartitionExpr {
            table_name,
            target: from_expr,
            into: vec![left, right],
        })
    }
}

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct MergePartitionExprGenerator {
    table_ctx: TableContextRef,
}

impl<R: Rng + 'static> Generator<MergePartitionExpr, R> for MergePartitionExprGenerator {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<MergePartitionExpr> {
        let table_name = self.table_ctx.name.clone();
        let partition_def = self
            .table_ctx
            .partition
            .as_ref()
            .expect("expected partition def");
        let mut partitions =
            SimplePartitions::from_exprs(partition_def.columns[0].clone(), &partition_def.exprs)?;
        let remove_idx = rng.random_range(0..partitions.bounds.len());
        partitions.remove_bound(remove_idx)?;
        let left = partition_def.exprs[remove_idx].clone();
        let right = partition_def.exprs[remove_idx + 1].clone();

        Ok(MergePartitionExpr {
            table_name,
            targets: vec![left, right],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::{Rng, SeedableRng};

    use crate::context::TableContext;
    use crate::generator::Generator;
    use crate::generator::create_expr::CreateTableExprGeneratorBuilder;
    use crate::generator::repartition_expr::{
        MergePartitionExprGeneratorBuilder, SplitPartitionExprGeneratorBuilder,
    };
    use crate::ir::repartition_expr::RepartitionExpr;
    use crate::translator::DslTranslator;
    use crate::translator::mysql::repartition_expr::RepartitionExprTranslator;

    #[test]
    fn test_split_partition_expr() {
        common_telemetry::init_default_ut_logging();
        let mut rng = rand::rng();
        let seed = rng.random::<u64>();
        common_telemetry::info!("initializing rng with seed: {seed}");
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        let expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .partition(10)
            .if_not_exists(true)
            .engine("mito2")
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let table_ctx = Arc::new(TableContext::from(&expr));
        SplitPartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
    }

    #[test]
    fn test_split_partition_expr_deterministic() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(1234);
        let expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .partition(10)
            .if_not_exists(true)
            .engine("mito2")
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let table_ctx = Arc::new(TableContext::from(&expr));
        let expr = SplitPartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();

        let sql = RepartitionExprTranslator
            .translate(&RepartitionExpr::Split(expr))
            .unwrap();
        let expected = r#"ALTER TABLE quO SPLIT PARTITION (
  MaGnI >= 1844674407370955160 AND MaGnI < 2767011611056432740
) INTO (
  MaGnI >= 1844674407370955160 AND MaGnI < 2290232243991136014,
  MaGnI >= 2290232243991136014 AND MaGnI < 2767011611056432740
);"#;
        assert_eq!(expected, sql);
    }

    #[test]
    fn test_merge_partition_expr() {
        common_telemetry::init_default_ut_logging();
        let mut rng = rand::rng();
        let seed = rng.random::<u64>();
        common_telemetry::info!("initializing rng with seed: {seed}");
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        let expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .partition(10)
            .if_not_exists(true)
            .engine("mito2")
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let table_ctx = Arc::new(TableContext::from(&expr));
        MergePartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
    }

    #[test]
    fn test_merge_partition_expr_deterministic() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(1234);
        let expr = CreateTableExprGeneratorBuilder::default()
            .columns(10)
            .partition(10)
            .if_not_exists(true)
            .engine("mito2")
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let table_ctx = Arc::new(TableContext::from(&expr));
        let expr = MergePartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();

        let sql = RepartitionExprTranslator
            .translate(&RepartitionExpr::Merge(expr))
            .unwrap();
        let expected = r#"ALTER TABLE quO MERGE PARTITION (
  MaGnI >= 3689348814741910320 AND MaGnI < 4611686018427387900,
  MaGnI >= 4611686018427387900 AND MaGnI < 5534023222112865480
);"#;
        assert_eq!(expected, sql);
    }
}
