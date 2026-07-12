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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use common_meta::distributed_time_constants::default_distributed_time_constants;
use common_telemetry::info;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaChaRng};
use snafu::{ResultExt, ensure};
use sqlx::{Executor, MySql, Pool};
use store_api::storage::RegionId;
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    MappedGenerator, WordGenerator, merge_two_word_map_fn, random_capitalize_map,
    uppercase_and_keyword_backtick_map,
};
use tests_fuzz::generator::Generator;
use tests_fuzz::generator::create_expr::{
    CreateLogicalTableExprGeneratorBuilder, CreatePhysicalTableExprGeneratorBuilder,
};
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::ir::{
    CreateTableExpr, Ident, InsertIntoExpr, generate_random_timestamp_for_mysql,
    generate_random_value,
};
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::utils::cluster_info::{PEER_TYPE_DATANODE, fetch_nodes};
use tests_fuzz::utils::migration::migrate_region;
use tests_fuzz::utils::partition::{fetch_partition, fetch_partitions, region_distribution};
use tests_fuzz::utils::procedure::procedure_state;
use tests_fuzz::utils::wait::wait_condition_fn;
use tests_fuzz::utils::{
    Connections, compact_table, flush_memtable, get_fuzz_override, get_gt_fuzz_input_max_rows,
    get_gt_fuzz_input_max_tables, init_greptime_connections_via_env,
};
use tests_fuzz::validator::row::count_values;

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
    rows: usize,
    tables: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = get_fuzz_override::<u64>("SEED").unwrap_or(u.int_in_range(u64::MIN..=u64::MAX)?);
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let max_rows = get_gt_fuzz_input_max_rows();
        let rows =
            get_fuzz_override::<usize>("ROWS").unwrap_or_else(|| rng.random_range(2..max_rows));
        let max_tables = get_gt_fuzz_input_max_tables();
        let tables =
            get_fuzz_override::<usize>("TABLES").unwrap_or_else(|| rng.random_range(1..max_tables));

        Ok(FuzzInput { rows, seed, tables })
    }
}

fn generate_create_physical_table_expr<R: Rng + 'static>(rng: &mut R) -> Result<CreateTableExpr> {
    let physical_table_if_not_exists = rng.random_bool(0.5);
    let create_physical_table_expr = CreatePhysicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .if_not_exists(physical_table_if_not_exists)
        .build()
        .unwrap();
    create_physical_table_expr.generate(rng)
}

async fn create_physical_table<R: Rng + 'static>(
    ctx: &FuzzContext,
    rng: &mut R,
) -> Result<TableContextRef> {
    // Create a physical table and a logical table on top of it
    let create_physical_table_expr = generate_create_physical_table_expr(rng).unwrap();
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_physical_table_expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create physical table: {sql}, result: {result:?}");

    Ok(Arc::new(TableContext::from(&create_physical_table_expr)))
}

fn generate_create_logical_table_expr<R: Rng + 'static>(
    physical_table_ctx: TableContextRef,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    let labels = rng.random_range(1..=5);
    let logical_table_if_not_exists = rng.random_bool(0.5);

    let create_logical_table_expr = CreateLogicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .physical_table_ctx(physical_table_ctx)
        .labels(labels)
        .if_not_exists(logical_table_if_not_exists)
        .build()
        .unwrap();
    create_logical_table_expr.generate(rng)
}

fn generate_insert_expr<R: Rng + 'static>(
    rows: usize,
    rng: &mut R,
    table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    let insert_generator = InsertExprGeneratorBuilder::default()
        .omit_column_list(false)
        .table_ctx(table_ctx)
        .rows(rows)
        .value_generator(Box::new(generate_random_value))
        .ts_value_generator(Box::new(generate_random_timestamp_for_mysql))
        .build()
        .unwrap();
    insert_generator.generate(rng)
}

async fn insert_values<R: Rng + 'static>(
    rows: usize,
    ctx: &FuzzContext,
    rng: &mut R,
    logical_table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    let insert_expr = generate_insert_expr(rows, rng, logical_table_ctx.clone())?;
    let translator = InsertIntoExprTranslator;
    let sql = translator.translate(&insert_expr)?;
    let result = ctx
        .greptime
        // unprepared query, see <https://github.com/GreptimeTeam/greptimedb/issues/3500>
        .execute(sql.as_str())
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;

    ensure!(
        result.rows_affected() == rows as u64,
        error::AssertSnafu {
            reason: format!(
                "expected rows affected: {}, actual: {}",
                rows,
                result.rows_affected(),
            )
        }
    );

    Ok(insert_expr)
}

#[derive(Debug, Clone, Copy)]
struct Migration {
    from_peer: u64,
    to_peer: u64,
    region_id: RegionId,
}

async fn create_logical_table_and_insert_values(
    ctx: &FuzzContext,
    input: &FuzzInput,
    rng: &mut ChaCha20Rng,
    physical_table_ctx: &TableContextRef,
    table_num: usize,
    tables: &mut HashMap<Ident, (TableContextRef, InsertIntoExpr)>,
) -> Result<()> {
    for _ in 0..table_num {
        let translator = CreateTableExprTranslator;
        let create_logical_table_expr =
            generate_create_logical_table_expr(physical_table_ctx.clone(), rng).unwrap();
        if tables.contains_key(&create_logical_table_expr.table_name) {
            // Ignores same name logical table.
            continue;
        }
        let sql = translator.translate(&create_logical_table_expr)?;
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("Create logical table: {sql}, result: {result:?}");
        let logical_table_ctx = Arc::new(TableContext::from(&create_logical_table_expr));

        let insert_expr = insert_values(input.rows, ctx, rng, logical_table_ctx.clone()).await?;
        if rng.random_bool(0.1) {
            flush_memtable(&ctx.greptime, &physical_table_ctx.name).await?;
        }
        if rng.random_bool(0.1) {
            compact_table(&ctx.greptime, &physical_table_ctx.name).await?;
        }

        tables.insert(
            logical_table_ctx.name.clone(),
            (logical_table_ctx.clone(), insert_expr),
        );
    }

    Ok(())
}

async fn wait_for_migration(ctx: &FuzzContext, migration: &Migration, procedure_id: &str) {
    info!("Waits for migration: {migration:?}");
    let region_id = migration.region_id.as_u64();
    wait_condition_fn(
        Duration::from_secs(120),
        || {
            let greptime = ctx.greptime.clone();
            let procedure_id = procedure_id.to_string();
            Box::pin(async move {
                let output = procedure_state(&greptime, &procedure_id).await;
                info!("Checking procedure: {procedure_id}, output: {output}");
                (fetch_partition(&greptime, region_id).await.unwrap(), output)
            })
        },
        |(partition, output)| {
            info!("Region: {region_id},  datanode: {}", partition.datanode_id);
            partition.datanode_id == migration.to_peer && output.contains("Done")
        },
        Duration::from_secs(1),
    )
    .await;
}

async fn migrate_regions(ctx: &FuzzContext, migrations: &[Migration]) -> Result<()> {
    let mut procedure_ids = Vec::with_capacity(migrations.len());
    // Triggers region migrations
    for Migration {
        from_peer,
        to_peer,
        region_id,
    } in migrations
    {
        let procedure_id =
            migrate_region(&ctx.greptime, region_id.as_u64(), *from_peer, *to_peer, 120).await;
        info!(
            "Migrating region: {region_id} from {from_peer} to {to_peer}, procedure: {procedure_id}"
        );
        procedure_ids.push(procedure_id);
    }
    for (migration, procedure_id) in migrations.iter().zip(procedure_ids) {
        wait_for_migration(ctx, migration, &procedure_id).await;
    }

    tokio::time::sleep(default_distributed_time_constants().region_lease).await;

    Ok(())
}

async fn validate_rows(
    ctx: &FuzzContext,
    tables: &HashMap<Ident, (TableContextRef, InsertIntoExpr)>,
) -> Result<()> {
    info!("Validates num of rows");
    for (table_ctx, insert_expr) in tables.values() {
        let sql = format!("select count(1) as count from {}", table_ctx.name);
        let values = count_values(&ctx.greptime, &sql).await?;
        let expected_rows = insert_expr.values_list.len() as u64;
        assert_eq!(
            values.count as u64, expected_rows,
            "Expected rows: {}, got: {}, table: {}",
            expected_rows, values.count, table_ctx.name
        );
    }
    Ok(())
}

async fn execute_migration(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    let mut rng = ChaCha20Rng::seed_from_u64(input.seed);
    // Creates a physical table.
    let physical_table_ctx = create_physical_table(&ctx, &mut rng).await?;
    let mut tables = HashMap::with_capacity(input.tables);

    let table_num = (input.tables / 3).max(1);

    // Creates more logical tables and inserts values
    create_logical_table_and_insert_values(
        &ctx,
        &input,
        &mut rng,
        &physical_table_ctx,
        table_num,
        &mut tables,
    )
    .await?;

    // Fetches region distribution
    let partitions = fetch_partitions(&ctx.greptime, physical_table_ctx.name.clone()).await?;
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
        let step = rng.random_range(1..datanodes.len());
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

    info!("Excepted new region distribution: {new_distribution:?}");
    migrate_regions(&ctx, &migrations).await?;

    // Validates value rows
    validate_rows(&ctx, &tables).await?;

    // Creates more logical tables and inserts values
    create_logical_table_and_insert_values(
        &ctx,
        &input,
        &mut rng,
        &physical_table_ctx,
        table_num,
        &mut tables,
    )
    .await?;

    // Validates value rows
    validate_rows(&ctx, &tables).await?;

    // Recovers region distribution
    let migrations = migrations
        .into_iter()
        .map(
            |Migration {
                 from_peer,
                 to_peer,
                 region_id,
             }| Migration {
                from_peer: to_peer,
                to_peer: from_peer,
                region_id,
            },
        )
        .collect::<Vec<_>>();

    migrate_regions(&ctx, &migrations).await?;

    // Creates more logical tables and inserts values
    create_logical_table_and_insert_values(
        &ctx,
        &input,
        &mut rng,
        &physical_table_ctx,
        table_num,
        &mut tables,
    )
    .await?;

    // Validates value rows
    validate_rows(&ctx, &tables).await?;

    // Clean up
    for (table_ctx, _) in tables.values() {
        let sql = format!("DROP TABLE {}", table_ctx.name);
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql })?;
        info!("Drop table: {}\n\nResult: {result:?}\n\n", table_ctx.name);
    }

    let sql = format!("DROP TABLE {}", physical_table_ctx.name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!(
        "Drop table: {}\n\nResult: {result:?}\n\n",
        physical_table_ctx.name
    );

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
        execute_migration(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
