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

use std::env;
use std::sync::Arc;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use common_meta::distributed_time_constants::default_distributed_time_constants;
use common_telemetry::info;
use libfuzzer_sys::fuzz_target;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
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
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::ir::{
    CreateTableExpr, Ident, InsertIntoExpr, MySQLTsColumnTypeGenerator, format_columns,
    generate_random_value, generate_unique_timestamp_for_mysql, replace_default,
    sort_by_primary_keys,
};
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::utils::cluster_info::{PEER_TYPE_DATANODE, fetch_nodes};
use tests_fuzz::utils::kafka_wal_http::{HttpWalAdmin, HttpWalAdminConfig, WalAdmin};
use tests_fuzz::utils::migration::migrate_region;
use tests_fuzz::utils::partition::{fetch_partition, fetch_partitions};
use tests_fuzz::utils::procedure::procedure_state;
use tests_fuzz::utils::wait::wait_condition_fn;
use tests_fuzz::utils::{
    Connections, flush_memtable, get_fuzz_override, get_gt_fuzz_input_max_rows,
    get_gt_fuzz_input_max_tables, init_greptime_connections_via_env,
};
use tests_fuzz::validator;

const MIN_TABLES: usize = 4;
const MAX_PARTITIONS: usize = 8;
const MIN_DELETE_AFTER_SECS: u64 = 180;
const DEFAULT_DELETE_AFTER_SECS: u64 = 360;
const MIGRATION_TIMEOUT_SECS: u64 = 240;
const ACTIVITY_TICK_SECS: u64 = 5;
const DEFAULT_KAFKA_ADDR: &str = "127.0.0.1:9092";
const DEFAULT_KAFKA_WAL_HELPER_URL: &str = "http://127.0.0.1:8080";
const DEFAULT_WAL_TOPIC_PREFIX: &str = "greptimedb_wal_topic";
const DEFAULT_WAL_NUM_TOPICS: usize = 3;
const DEFAULT_WAL_PARTITION: i32 = 0;
const DEFAULT_DELETE_RECORDS_TIMEOUT_MS: i32 = 5000;

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
    tables: usize,
    partitions: usize,
    rows_per_insert: usize,
    rounds: usize,
    delete_after_secs: u64,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = get_fuzz_override::<u64>("SEED").unwrap_or(u.int_in_range(u64::MIN..=u64::MAX)?);
        let mut rng = ChaChaRng::seed_from_u64(seed);

        let max_tables = get_gt_fuzz_input_max_tables().max(MIN_TABLES);
        let tables = get_fuzz_override::<usize>("TABLES")
            .unwrap_or_else(|| rng.random_range(MIN_TABLES..=max_tables));
        let partitions = get_fuzz_override::<usize>("PARTITIONS")
            .unwrap_or_else(|| rng.random_range(1..=MAX_PARTITIONS));
        let max_rows = get_gt_fuzz_input_max_rows().max(32);
        let rows_per_insert =
            get_fuzz_override::<usize>("ROWS").unwrap_or_else(|| rng.random_range(32..=max_rows));
        let rounds =
            get_fuzz_override::<usize>("ROUNDS").unwrap_or_else(|| rng.random_range(2..=4));
        let delete_after_secs = get_fuzz_override::<u64>("DELETE_AFTER_SECS")
            .or_else(|| get_fuzz_override::<u64>("RETENTION_WAIT_SECS"))
            .unwrap_or(DEFAULT_DELETE_AFTER_SECS)
            .max(MIN_DELETE_AFTER_SECS);

        Ok(FuzzInput {
            seed,
            tables,
            partitions,
            rows_per_insert,
            rounds,
            delete_after_secs,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityProfile {
    Hot,
    Warm,
    Cold,
    Dormant,
}

impl ActivityProfile {
    fn for_table(idx: usize) -> Self {
        match idx % 4 {
            0 => Self::Hot,
            1 => Self::Warm,
            2 => Self::Cold,
            _ => Self::Dormant,
        }
    }

    fn should_insert<R: Rng>(&self, rng: &mut R) -> bool {
        match self {
            Self::Hot => true,
            Self::Warm => rng.random_bool(0.7),
            Self::Cold => rng.random_bool(0.2),
            Self::Dormant => false,
        }
    }

    fn should_flush<R: Rng>(&self, rng: &mut R) -> bool {
        match self {
            Self::Hot => rng.random_bool(0.05),
            Self::Warm => rng.random_bool(0.05),
            Self::Cold | Self::Dormant => false,
        }
    }
}

struct TableState {
    ctx: TableContextRef,
    profile: ActivityProfile,
    insert_exprs: Vec<InsertIntoExpr>,
}

#[derive(Debug)]
struct Migration {
    from_peer: u64,
    to_peer: u64,
    region_id: RegionId,
}

fn wal_admin_from_env() -> HttpWalAdmin {
    let _ = dotenv::dotenv();
    HttpWalAdmin::new(HttpWalAdminConfig {
        base_url: env::var("GT_KAFKA_WAL_HELPER_URL")
            .unwrap_or_else(|_| DEFAULT_KAFKA_WAL_HELPER_URL.to_string()),
        broker_endpoints: kafka_broker_endpoints(),
        topic_prefix: env::var("GT_WAL_TOPIC_PREFIX")
            .unwrap_or_else(|_| DEFAULT_WAL_TOPIC_PREFIX.to_string()),
        num_topics: env::var("GT_WAL_NUM_TOPICS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_WAL_NUM_TOPICS),
        partition: env::var("GT_WAL_PARTITION")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_WAL_PARTITION),
        delete_records_timeout_ms: env::var("GT_WAL_DELETE_RECORDS_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_DELETE_RECORDS_TIMEOUT_MS),
    })
}

fn kafka_broker_endpoints() -> Vec<String> {
    env::var("GT_KAFKA_ADDR")
        .or_else(|_| env::var("KAFKA_ADDR"))
        .unwrap_or_else(|_| DEFAULT_KAFKA_ADDR.to_string())
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn generate_create_expr<R: Rng + 'static>(
    input: FuzzInput,
    idx: usize,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    let table_name = Ident::new(format!("wal_logical_prune_{}_{}", input.seed, idx));
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .name(table_name)
        .columns(4)
        .partition(input.partitions)
        .engine("mito")
        .ts_column_type_generator(Box::new(MySQLTsColumnTypeGenerator))
        .build()
        .unwrap();

    create_table_generator.generate(rng)
}

fn generate_insert_expr<R: Rng + 'static>(
    rows: usize,
    ts_base: i64,
    rng: &mut R,
    table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    let insert_generator = InsertExprGeneratorBuilder::default()
        .table_ctx(table_ctx)
        .omit_column_list(false)
        .rows(rows)
        .ts_value_generator(generate_unique_timestamp_for_mysql(ts_base))
        .value_generator(Box::new(generate_random_value))
        .build()
        .unwrap();

    insert_generator.generate(rng)
}

async fn create_tables<R: Rng + 'static>(
    ctx: &FuzzContext,
    input: FuzzInput,
    rng: &mut R,
) -> Result<Vec<TableState>> {
    let mut tables = Vec::with_capacity(input.tables);

    for idx in 0..input.tables {
        let create_expr = generate_create_expr(input, idx, rng)?;
        let translator = CreateTableExprTranslator;
        let sql = translator.translate(&create_expr)?;
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!(
            "Create table: {}, profile: {:?}, result: {result:?}",
            create_expr.table_name,
            ActivityProfile::for_table(idx)
        );

        tables.push(TableState {
            ctx: Arc::new(TableContext::from(&create_expr)),
            profile: ActivityProfile::for_table(idx),
            insert_exprs: Vec::new(),
        });
    }

    Ok(tables)
}

async fn insert_table<R: Rng + 'static>(
    ctx: &FuzzContext,
    input: FuzzInput,
    table: &mut TableState,
    ts_base: i64,
    rng: &mut R,
) -> Result<()> {
    let insert_expr = generate_insert_expr(input.rows_per_insert, ts_base, rng, table.ctx.clone())?;
    let translator = InsertIntoExprTranslator;
    let sql = translator.translate(&insert_expr)?;
    let result = ctx
        .greptime
        // unprepared query, see <https://github.com/GreptimeTeam/greptimedb/issues/3500>
        .execute(sql.as_str())
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    ensure!(
        result.rows_affected() == insert_expr.values_list.len() as u64,
        error::AssertSnafu {
            reason: format!(
                "expected rows affected: {}, actual: {}",
                insert_expr.values_list.len(),
                result.rows_affected(),
            )
        }
    );

    table.insert_exprs.push(insert_expr);
    Ok(())
}

async fn flush_table(ctx: &FuzzContext, table: &TableState) -> Result<()> {
    flush_memtable(&ctx.greptime, &table.ctx.name).await
}

async fn validate_table(ctx: &FuzzContext, table: &TableState) -> Result<()> {
    if table.insert_exprs.is_empty() {
        return Ok(());
    }

    info!(
        "Validating table: {}, profile: {:?}, batches: {}",
        table.ctx.name,
        table.profile,
        table.insert_exprs.len()
    );
    validate_insert_exprs(ctx, &table.ctx, &table.insert_exprs).await
}

async fn validate_insert_exprs(
    ctx: &FuzzContext,
    table_ctx: &TableContextRef,
    insert_exprs: &[InsertIntoExpr],
) -> Result<()> {
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
            table_ctx.name,
            ts_column.name,
            ts_value,
            ts_column.name,
            next_batch_ts,
            primary_keys_column_list
        );
        info!("Executing sql: {select_sql}");
        let fetched_rows = ctx.greptime.fetch_all(select_sql.as_str()).await.unwrap();
        let mut expected_rows = replace_default(&insert_expr.values_list, table_ctx, insert_expr);
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
        column_list, table_ctx.name, ts_column.name, ts_value, primary_keys_column_list
    );
    info!("Executing sql: {select_sql}");
    let fetched_rows = ctx.greptime.fetch_all(select_sql.as_str()).await.unwrap();
    let mut expected_rows = replace_default(&insert_expr.values_list, table_ctx, insert_expr);
    sort_by_primary_keys(&mut expected_rows, primary_keys_idx);
    validator::row::assert_eq::<MySql>(&insert_expr.columns, &fetched_rows, &expected_rows)?;

    Ok(())
}

async fn migrate_all_regions<R: Rng + 'static>(
    ctx: &FuzzContext,
    tables: &[TableState],
    rng: &mut R,
) -> Result<()> {
    let datanodes = fetch_datanodes(ctx).await?;
    let mut migrations = Vec::new();

    for table in tables {
        let partitions = fetch_partitions(&ctx.greptime, table.ctx.name.clone()).await?;
        for partition in partitions {
            let from_peer = partition.datanode_id;
            let to_peer = choose_target_peer(&datanodes, from_peer, rng, || {
                format!(
                    "failed to choose migration target, table: {}, region: {}, from_peer: {}",
                    table.ctx.name, partition.region_id, from_peer
                )
            })?;
            migrations.push(Migration {
                from_peer,
                to_peer,
                region_id: RegionId::from_u64(partition.region_id),
            });
        }
    }

    migrate_regions(ctx, &migrations).await
}

async fn fetch_datanodes(ctx: &FuzzContext) -> Result<Vec<u64>> {
    let datanodes = fetch_nodes(&ctx.greptime)
        .await?
        .into_iter()
        .filter(|node| node.peer_type == PEER_TYPE_DATANODE)
        .map(|node| node.peer_id as u64)
        .collect::<Vec<_>>();
    ensure!(
        datanodes.len() >= 2,
        error::AssertSnafu {
            reason: format!(
                "remote WAL logical prune fuzz requires at least two datanodes, got {}",
                datanodes.len()
            )
        }
    );
    Ok(datanodes)
}

fn choose_target_peer<R, F>(
    datanodes: &[u64],
    from_peer: u64,
    rng: &mut R,
    error_message: F,
) -> Result<u64>
where
    R: Rng + 'static,
    F: FnOnce() -> String,
{
    datanodes
        .iter()
        .filter(|peer| **peer != from_peer)
        .copied()
        .collect::<Vec<_>>()
        .choose(rng)
        .copied()
        .ok_or_else(|| {
            error::AssertSnafu {
                reason: error_message(),
            }
            .build()
        })
}

async fn migrate_regions(ctx: &FuzzContext, migrations: &[Migration]) -> Result<()> {
    let mut procedure_ids = Vec::with_capacity(migrations.len());
    for migration in migrations {
        procedure_ids.push(submit_migration(ctx, migration).await);
    }

    for (migration, procedure_id) in migrations.iter().zip(procedure_ids) {
        wait_for_migration(ctx, migration, &procedure_id).await?;
    }

    if !migrations.is_empty() {
        tokio::time::sleep(default_distributed_time_constants().region_lease).await;
    }

    Ok(())
}

async fn submit_migration(ctx: &FuzzContext, migration: &Migration) -> String {
    let procedure_id = migrate_region(
        &ctx.greptime,
        migration.region_id.as_u64(),
        migration.from_peer,
        migration.to_peer,
        MIGRATION_TIMEOUT_SECS,
    )
    .await;
    info!(
        "Migrating region: {:?}, procedure: {}",
        migration, procedure_id
    );
    procedure_id
}

async fn wait_for_migration(
    ctx: &FuzzContext,
    migration: &Migration,
    procedure_id: &str,
) -> Result<()> {
    let region_id = migration.region_id.as_u64();
    wait_condition_fn(
        Duration::from_secs(MIGRATION_TIMEOUT_SECS),
        || {
            let greptime = ctx.greptime.clone();
            let procedure_id = procedure_id.to_string();
            Box::pin(async move {
                let output = procedure_state(&greptime, &procedure_id).await;
                info!("Checking procedure: {procedure_id}, output: {output}");
                (fetch_partition(&greptime, region_id).await.ok(), output)
            })
        },
        |(partition, output)| {
            if let Some(partition) = partition {
                partition.datanode_id == migration.to_peer && output.contains("Done")
            } else {
                false
            }
        },
        Duration::from_secs(5),
    )
    .await;

    Ok(())
}

fn next_ts_base(input: FuzzInput, batch_idx: usize) -> i64 {
    input.seed as i64 + (batch_idx as i64 * 1_000_000)
}

async fn run_activity_round<R: Rng + 'static>(
    ctx: &FuzzContext,
    input: FuzzInput,
    tables: &mut [TableState],
    batch_idx: &mut usize,
    round: usize,
    rng: &mut R,
) -> Result<()> {
    info!("Start mixed activity round: {round}");
    for table in tables.iter_mut() {
        if table.profile.should_insert(rng) {
            insert_table(ctx, input, table, next_ts_base(input, *batch_idx), rng).await?;
            *batch_idx += 1;

            if table.profile.should_flush(rng) {
                flush_table(ctx, table).await?;
            }
        }
    }

    for table in tables.iter() {
        match table.profile {
            ActivityProfile::Cold | ActivityProfile::Dormant => validate_table(ctx, table).await?,
            ActivityProfile::Hot | ActivityProfile::Warm if rng.random_bool(0.5) => {
                validate_table(ctx, table).await?
            }
            ActivityProfile::Hot | ActivityProfile::Warm => {}
        }
    }

    Ok(())
}

async fn execute_logical_prune_fuzz<W: WalAdmin>(
    ctx: FuzzContext,
    input: FuzzInput,
    wal_admin: &W,
) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);
    let mut tables = create_tables(&ctx, input, &mut rng).await?;
    let mut batch_idx = 0;

    for table in &mut tables {
        insert_table(&ctx, input, table, next_ts_base(input, batch_idx), &mut rng).await?;
        batch_idx += 1;
    }

    for table in &tables {
        validate_table(&ctx, table).await?;
    }

    info!("Recording Kafka WAL latest offsets via WAL admin");
    let delete_boundary_offsets = wal_admin.record_topic_latest_offsets().await?;
    for offset in &delete_boundary_offsets {
        info!(
            "Recorded Kafka WAL delete boundary, topic: {}, partition: {}, offset: {}",
            offset.topic, offset.partition, offset.offset
        );
    }
    let delete_deadline =
        tokio::time::Instant::now() + Duration::from_secs(input.delete_after_secs);
    info!(
        "Start foreground activity until DeleteRecords deadline, delete_after_secs: {}",
        input.delete_after_secs
    );

    let mut round = 0;
    while tokio::time::Instant::now() < delete_deadline {
        run_activity_round(&ctx, input, &mut tables, &mut batch_idx, round, &mut rng).await?;
        round += 1;

        if input.rounds > 0 && round.is_multiple_of(input.rounds) {
            info!(
                "Completed configured activity round batch before DeleteRecords, rounds: {round}"
            );
        }

        tokio::time::sleep(Duration::from_secs(ACTIVITY_TICK_SECS)).await;
    }

    for offset in &delete_boundary_offsets {
        info!(
            "Deleting Kafka WAL records to boundary, topic: {}, partition: {}, offset: {}",
            offset.topic, offset.partition, offset.offset
        );
    }
    wal_admin
        .delete_records_to_offsets(&delete_boundary_offsets)
        .await?;
    for offset in &delete_boundary_offsets {
        info!(
            "Deleted Kafka WAL records to boundary, topic: {}, partition: {}, offset: {}",
            offset.topic, offset.partition, offset.offset
        );
    }
    migrate_all_regions(&ctx, &tables, &mut rng).await?;

    for table in &tables {
        validate_table(&ctx, table).await?;
    }

    for table in tables {
        let sql = format!("DROP TABLE {}", table.ctx.name);
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql })?;
        info!("Drop table: {}, result: {result:?}", table.ctx.name);
    }

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
        let wal_admin = wal_admin_from_env();

        execute_logical_prune_fuzz(ctx, input, &wal_admin)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
