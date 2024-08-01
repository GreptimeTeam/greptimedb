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

use std::collections::HashMap;
use std::fs::create_dir_all;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use common_telemetry::info;
use common_telemetry::tracing::warn;
use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::Serialize;
use snafu::ensure;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};
use tests_fuzz::context::TableContext;
use tests_fuzz::error::Result;
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::CreateTableExpr;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::config::{get_conf_path, write_config_file};
use tests_fuzz::utils::health::HttpHealthChecker;
use tests_fuzz::utils::process::{ProcessManager, ProcessState, UnstableProcessController};
use tests_fuzz::utils::{get_gt_fuzz_input_max_tables, load_unstable_test_env_variables};
use tests_fuzz::{error, validator};
use tokio::sync::watch;

struct FuzzContext {
    greptime: Pool<MySql>,
}

impl FuzzContext {
    async fn close(self) {
        self.greptime.close().await;
    }
}

#[derive(Clone, Debug)]
struct FuzzInput {
    seed: u64,
    tables: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let max_tables = get_gt_fuzz_input_max_tables();
        let tables = rng.gen_range(1..max_tables);
        Ok(FuzzInput { seed, tables })
    }
}

const DEFAULT_TEMPLATE: &str = "standalone.template.toml";
const DEFAULT_CONFIG_NAME: &str = "standalone.template.toml";
const DEFAULT_ROOT_DIR: &str = "/tmp/unstable_greptime/";
const DEFAULT_MYSQL_URL: &str = "127.0.0.1:4002";
const DEFAULT_HTTP_HEALTH_URL: &str = "http://127.0.0.1:4000/health";

fn generate_create_table_expr<R: Rng + 'static>(rng: &mut R) -> CreateTableExpr {
    let columns = rng.gen_range(2..30);
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(columns)
        .engine("mito")
        .build()
        .unwrap();
    create_table_generator.generate(rng).unwrap()
}

async fn connect_mysql(addr: &str, database: &str) -> Pool<MySql> {
    loop {
        match MySqlPoolOptions::new()
            .acquire_timeout(Duration::from_secs(30))
            .connect(&format!("mysql://{addr}/{database}"))
            .await
        {
            Ok(mysql) => return mysql,
            Err(err) => {
                warn!("Reconnecting to {addr}, error: {err}")
            }
        }
    }
}

const FUZZ_TESTS_DATABASE: &str = "fuzz_tests";

async fn execute_unstable_create_table(
    unstable_process_controller: Arc<UnstableProcessController>,
    rx: watch::Receiver<ProcessState>,
    input: FuzzInput,
) -> Result<()> {
    // Starts the unstable process.
    let moved_unstable_process_controller = unstable_process_controller.clone();
    let handler = tokio::spawn(async move { moved_unstable_process_controller.start().await });
    let mysql_public = connect_mysql(DEFAULT_MYSQL_URL, "public").await;
    loop {
        let sql = format!("CREATE DATABASE IF NOT EXISTS {FUZZ_TESTS_DATABASE}");
        match sqlx::query(&sql).execute(&mysql_public).await {
            Ok(result) => {
                info!("Create database: {}, result: {result:?}", sql);
                break;
            }
            Err(err) => warn!("Failed to create database: {}, error: {err}", sql),
        }
    }
    let mysql = connect_mysql(DEFAULT_MYSQL_URL, FUZZ_TESTS_DATABASE).await;
    let mut rng = ChaChaRng::seed_from_u64(input.seed);
    let ctx = FuzzContext { greptime: mysql };
    let mut table_states = HashMap::new();

    for _ in 0..input.tables {
        let expr = generate_create_table_expr(&mut rng);
        let table_ctx = Arc::new(TableContext::from(&expr));
        let table_name = expr.table_name.to_string();
        if table_states.contains_key(&table_name) {
            warn!("ignores same name table: {table_name}");
            // ignores.
            continue;
        }

        let translator = CreateTableExprTranslator;
        let sql = translator.translate(&expr).unwrap();
        let result = sqlx::query(&sql).execute(&ctx.greptime).await;
        match result {
            Ok(result) => {
                let state = *rx.borrow();
                table_states.insert(table_name, state);
                validate_columns(&ctx.greptime, FUZZ_TESTS_DATABASE, &table_ctx).await;
                info!("Create table: {sql}, result: {result:?}");
            }
            Err(err) => {
                // FIXME(weny): support to retry it later.
                if matches!(err, sqlx::Error::PoolTimedOut { .. }) {
                    warn!("ignore pool timeout, sql: {sql}");
                    continue;
                }
                let state = *rx.borrow();
                ensure!(
                    !state.health(),
                    error::UnexpectedSnafu {
                        violated: format!("Failed to create table: {sql}, error: {err}")
                    }
                );
                table_states.insert(table_name, state);
                continue;
            }
        }
    }

    loop {
        let sql = format!("DROP DATABASE IF EXISTS {FUZZ_TESTS_DATABASE}");
        match sqlx::query(&sql).execute(&mysql_public).await {
            Ok(result) => {
                info!("Drop database: {}, result: {result:?}", sql);
                break;
            }
            Err(err) => warn!("Failed to drop database: {}, error: {err}", sql),
        }
    }
    // Cleans up
    ctx.close().await;
    unstable_process_controller.stop();
    let _ = handler.await;
    info!("Finishing test for input: {:?}", input);
    Ok(())
}

async fn validate_columns(client: &Pool<MySql>, schema_name: &str, table_ctx: &TableContext) {
    loop {
        match validator::column::fetch_columns(client, schema_name.into(), table_ctx.name.clone())
            .await
        {
            Ok(mut column_entries) => {
                column_entries.sort_by(|a, b| a.column_name.cmp(&b.column_name));
                let mut columns = table_ctx.columns.clone();
                columns.sort_by(|a, b| a.name.value.cmp(&b.name.value));
                validator::column::assert_eq(&column_entries, &columns).unwrap();
                return;
            }
            Err(err) => warn!(
                "Failed to fetch table '{}' columns, error: {}",
                table_ctx.name, err
            ),
        }
    }
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_global(async {
        let variables = load_unstable_test_env_variables();
        let root_dir = variables.root_dir.unwrap_or(DEFAULT_ROOT_DIR.to_string());
        create_dir_all(&root_dir).unwrap();
        let output_config_path = format!("{root_dir}{DEFAULT_CONFIG_NAME}");
        let data_home = format!("{root_dir}datahome");

        let mut conf_path = get_conf_path();
        conf_path.push(DEFAULT_TEMPLATE);
        let template_path = conf_path.to_str().unwrap().to_string();

        // Writes config file.
        #[derive(Serialize)]
        struct Context {
            data_home: String,
        }
        write_config_file(&template_path, &Context { data_home }, &output_config_path)
            .await
            .unwrap();

        let args = vec![
            "standalone".to_string(),
            "start".to_string(),
            format!("--config-file={output_config_path}"),
        ];
        let process_manager = ProcessManager::new();
        let (tx, rx) = watch::channel(ProcessState::NotSpawn);
        let unstable_process_controller = Arc::new(UnstableProcessController {
            binary_path: variables.binary_path,
            args,
            root_dir,
            seed: input.seed,
            process_manager,
            health_check: Box::new(HttpHealthChecker {
                url: DEFAULT_HTTP_HEALTH_URL.to_string(),
            }),
            sender: tx,
            running: Arc::new(AtomicBool::new(false)),
        });

        execute_unstable_create_table(unstable_process_controller, rx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
