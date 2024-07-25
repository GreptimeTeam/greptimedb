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

#![no_main]

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::{ensure, ResultExt};
use sqlx::{Executor, MySql, Pool};
use store_api::storage::RegionId;
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{
    format_columns, generate_random_value, generate_unique_timestamp_for_mysql, replace_default,
    sort_by_primary_keys, CreateTableExpr, InsertIntoExpr, MySQLTsColumnTypeGenerator,
};
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::cluster_info::{fetch_nodes, PEER_TYPE_DATANODE};
use tests_fuzz::utils::migration::migrate_region;
use tests_fuzz::utils::partition::{fetch_partition, fetch_partitions, region_distribution};
use tests_fuzz::utils::procedure::procedure_state;
use tests_fuzz::utils::wait::wait_condition_fn;
use tests_fuzz::utils::{
    compact_table, flush_memtable, init_greptime_connections_via_env, Connections,
};
use tests_fuzz::validator;

struct FuzzContext {
    greptime: Pool<MySql>,
}

impl FuzzContext {
    async fn close(self) {
        self.greptime.close().await;
    }
}

#[derive(Copy, Clone, Debug)]
struct FuzzInput {
    seed: u64,
    columns: usize,
    partitions: usize,
    rows: usize,
    inserts: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let partitions = rng.gen_range(3..32);
        let columns = rng.gen_range(2..30);
        let rows = rng.gen_range(128..1024);
        let inserts = rng.gen_range(2..8);
        Ok(FuzzInput {
            partitions,
            columns,
            rows,
            seed,
            inserts,
        })
    }
}

fn generate_create_expr<R: Rng + 'static>(
    input: FuzzInput,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(input.columns)
        .partition(input.partitions)
        .engine("mito")
        .ts_column_type_generator(Box::new(MySQLTsColumnTypeGenerator))
        .build()
        .unwrap();
    create_table_generator.generate(rng)
}

fn generate_insert_exprs<R: Rng + 'static>(
    input: FuzzInput,
    rng: &mut R,
    table_ctx: TableContextRef,
) -> Result<Vec<InsertIntoExpr>> {
    let omit_column_list = rng.gen_bool(0.2);
    let insert_generator = InsertExprGeneratorBuilder::default()
        .table_ctx(table_ctx.clone())
        .omit_column_list(omit_column_list)
        .rows(input.rows)
        .ts_value_generator(generate_unique_timestamp_for_mysql(0))
        .value_generator(Box::new(generate_random_value))
        .build()
        .unwrap();
    (0..input.inserts)
        .map(|_| insert_generator.generate(rng))
        .collect::<Result<Vec<_>>>()
}

#[derive(Debug)]
struct Migration {
    from_peer: u64,
    to_peer: u64,
    region_id: RegionId,
}

async fn execute_region_migration(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {:?}", input);
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    let create_expr = generate_create_expr(input, &mut rng)?;
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_expr)?;
    let _result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;

    let table_ctx = Arc::new(TableContext::from(&create_expr));
    // Inserts data into the table
    let insert_exprs = generate_insert_exprs(input, &mut rng, table_ctx.clone())?;
    for insert_expr in &insert_exprs {
        let translator = InsertIntoExprTranslator;
        let sql = translator.translate(insert_expr)?;
        let result = ctx
            .greptime
            // unprepared query, see <https://github.com/GreptimeTeam/greptimedb/issues/3500>
            .execute(sql.as_str())
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        ensure!(
            result.rows_affected() == input.rows as u64,
            error::AssertSnafu {
                reason: format!(
                    "expected rows affected: {}, actual: {}",
                    input.rows,
                    result.rows_affected(),
                )
            }
        );
        if rng.gen_bool(0.2) {
            flush_memtable(&ctx.greptime, &create_expr.table_name).await?;
        }
        if rng.gen_bool(0.1) {
            compact_table(&ctx.greptime, &create_expr.table_name).await?;
        }
    }

    // Fetches region distribution
    let partitions = fetch_partitions(&ctx.greptime, table_ctx.name.clone()).await?;
    let num_partitions = partitions.len();
    let region_distribution = region_distribution(partitions);
    info!("Region distribution: {region_distribution:?}");
    let datanodes = fetch_nodes(&ctx.greptime)
        .await?
        .into_iter()
        .flat_map(|node| {
            if node.peer_type == PEER_TYPE_DATANODE {
                Some(node)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    info!("List datanodes: {:?}", datanodes);

    // Generates region migration task.
    let mut migrations = Vec::with_capacity(num_partitions);
    let mut new_distribution: BTreeMap<u64, HashSet<_>> = BTreeMap::new();
    for (datanode_id, regions) in region_distribution {
        let step = rng.gen_range(1..datanodes.len());
        for region in regions {
            let to_peer = (datanode_id + step as u64) % datanodes.len() as u64;
            new_distribution.entry(to_peer).or_default().insert(region);
            migrations.push(Migration {
                from_peer: datanode_id,
                to_peer,
                region_id: region,
            })
        }
    }

    let mut procedure_ids = Vec::with_capacity(migrations.len());
    // Triggers region migrations
    for Migration {
        from_peer,
        to_peer,
        region_id,
    } in &migrations
    {
        let procedure_id =
            migrate_region(&ctx.greptime, region_id.as_u64(), *from_peer, *to_peer, 120).await;
        info!("Migrating region: {region_id} from {from_peer} to {to_peer}, procedure: {procedure_id}");
        procedure_ids.push(procedure_id);
    }
    info!("Excepted new region distribution: {new_distribution:?}");

    for (migration, procedure_id) in migrations.into_iter().zip(procedure_ids) {
        info!("Waits for migration: {migration:?}");
        let region_id = migration.region_id.as_u64();
        wait_condition_fn(
            Duration::from_secs(120),
            || {
                let greptime = ctx.greptime.clone();
                let procedure_id = procedure_id.to_string();
                Box::pin(async move {
                    {
                        let output = procedure_state(&greptime, &procedure_id).await;
                        info!("Checking procedure: {procedure_id}, output: {output}");
                        fetch_partition(&greptime, region_id).await.unwrap()
                    }
                })
            },
            |partition| {
                info!("Region: {region_id},  datanode: {}", partition.datanode_id);
                partition.datanode_id == migration.to_peer
            },
            Duration::from_secs(5),
        )
        .await;
    }

    // Values validation
    info!("Validating rows");
    let ts_column = table_ctx.timestamp_column().unwrap();
    for (idx, insert_expr) in insert_exprs[0..insert_exprs.len() - 1].iter().enumerate() {
        let ts_column_idx = insert_expr.timestamp_column_idx().unwrap();
        let ts_value = insert_expr.values_list[0][ts_column_idx].clone();
        let next_batch_ts_column_idx = insert_exprs[idx + 1].timestamp_column_idx().unwrap();
        let next_batch_ts = insert_exprs[idx + 1].values_list[0][next_batch_ts_column_idx].clone();

        let primary_keys_idx = insert_expr.primary_key_column_idx();
        let column_list = format_columns(&insert_expr.columns);
        let primary_keys_column_list = format_columns(&insert_expr.primary_key_columns());
        let select_sql = format!(
            "SELECT {} FROM {} WHERE {} >= {} AND {} < {} ORDER BY {};",
            column_list,
            create_expr.table_name,
            ts_column.name,
            ts_value,
            ts_column.name,
            next_batch_ts,
            primary_keys_column_list
        );
        info!("Executing sql: {select_sql}");
        let fetched_rows = ctx.greptime.fetch_all(select_sql.as_str()).await.unwrap();
        let mut expected_rows = replace_default(&insert_expr.values_list, &table_ctx, insert_expr);
        sort_by_primary_keys(&mut expected_rows, primary_keys_idx);
        validator::row::assert_eq::<MySql>(&insert_expr.columns, &fetched_rows, &expected_rows)?;
    }
    let insert_expr = insert_exprs.last().unwrap();
    let ts_column_idx = insert_expr.timestamp_column_idx().unwrap();
    let ts_value = insert_expr.values_list[0][ts_column_idx].clone();
    let primary_keys_idx = insert_expr.primary_key_column_idx();
    let column_list = format_columns(&insert_expr.columns);
    let primary_keys_column_list = format_columns(&insert_expr.primary_key_columns());
    let select_sql = format!(
        "SELECT {} FROM {} WHERE {} >= {} ORDER BY {};",
        column_list, create_expr.table_name, ts_column.name, ts_value, primary_keys_column_list
    );
    info!("Executing sql: {select_sql}");
    let fetched_rows = ctx.greptime.fetch_all(select_sql.as_str()).await.unwrap();
    let mut expected_rows = replace_default(&insert_expr.values_list, &table_ctx, insert_expr);
    sort_by_primary_keys(&mut expected_rows, primary_keys_idx);
    validator::row::assert_eq::<MySql>(&insert_expr.columns, &fetched_rows, &expected_rows)?;

    // Cleans up
    let sql = format!("DROP TABLE {}", table_ctx.name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Drop table: {}\n\nResult: {result:?}\n\n", table_ctx.name);
    ctx.close().await;
    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_global(async {
        let Connections { mysql } = init_greptime_connections_via_env().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };

        execute_region_migration(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
