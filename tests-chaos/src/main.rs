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

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bare::process::{Pid, ProcessManager};
use common_telemetry::{info, warn};
use nix::sys::signal::Signal;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::Serialize;
use snafu::{ensure, ResultExt};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};
use tests_fuzz::context::TableContext;
use tests_fuzz::ir::AlterTableOperation;
use tests_fuzz::translator::mysql::alter_expr::AlterTableExprTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::validator;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use utils::generate_alter_table_expr;
mod bare;
mod error;
mod utils;

use crate::error::Result;
use crate::utils::{generate_create_table_expr, get_conf_path, path_to_stdio, render_config_file};
const DEFAULT_LOG_LEVEL: &str = "--log-level=debug,hyper=warn,tower=warn,datafusion=warn,reqwest=warn,sqlparser=warn,h2=info,opendal=info";

#[tokio::main]
async fn main() {
    common_telemetry::init_default_ut_logging();

    let state = Arc::new(TestState {
        killed: AtomicBool::new(false),
    });

    let moved_state = state.clone();
    tokio::spawn(async move {
        let mut rng = ChaChaRng::seed_from_u64(0);
        loop {
            warn!("Staring");
            let pid = start_database().await.expect("Failed to start database");
            let secs = rng.gen_range(100..300);
            moved_state.killed.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(secs)).await;
            warn!("After {secs}ms, Killing pid: {pid}");
            moved_state.killed.store(true, Ordering::Relaxed);
            ProcessManager::kill(pid, Signal::SIGKILL).expect("Failed to kill");
        }
    });
    let mut rng = ChaChaRng::seed_from_u64(0);
    let client = connect_db("127.0.0.1:4002").await;
    let mut created_table = HashSet::new();
    loop {
        run_test(&client, &mut created_table, &state, &mut rng)
            .await
            .unwrap();
    }
}

async fn connect_db(addr: &str) -> Pool<MySql> {
    MySqlPoolOptions::new()
        .connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap()
}

struct TestState {
    killed: AtomicBool,
}

async fn run_test<R: Rng + 'static>(
    client: &Pool<MySql>,
    created_table: &mut HashSet<String>,
    state: &Arc<TestState>,
    rng: &mut R,
) -> Result<()> {
    let expr = generate_create_table_expr(rng);
    let table_name = expr.table_name.to_string();
    if created_table.contains(&table_name) {
        warn!("ignores same name table: {table_name}");
        // ignores.
        return Ok(());
    }

    let mut table_ctx = Arc::new(TableContext::from(&expr));
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&expr).unwrap();
    let result = sqlx::query(&sql).execute(client).await;
    match result {
        Ok(result) => {
            created_table.insert(table_name);
            validate_mysql(client, state, &table_ctx).await;
            info!("Create table: {sql}, result: {result:?}");
        }
        Err(err) => {
            ensure!(
                state.killed.load(Ordering::Relaxed),
                error::UnexpectedSnafu {
                    err_msg: err.to_string(),
                }
            );
            created_table.insert(table_name);
        }
    }

    let actions = rng.gen_range(1..20);

    for _ in 0..actions {
        let expr = generate_alter_table_expr(table_ctx.clone(), rng);
        if let AlterTableOperation::RenameTable { new_table_name } = &expr.alter_options {
            let table_name = new_table_name.to_string();
            if created_table.contains(&table_name) {
                warn!("ignores altering to same name table: {table_name}");
                continue;
            }
        };

        let translator = AlterTableExprTranslator;
        let sql = translator.translate(&expr).unwrap();
        let result = sqlx::query(&sql).execute(client).await;
        match result {
            Ok(result) => {
                info!("alter table: {sql}, result: {result:?}");
                let table_name = table_ctx.name.to_string();
                created_table.remove(&table_name);
                table_ctx = Arc::new(Arc::unwrap_or_clone(table_ctx).alter(expr).unwrap());
                validate_mysql(client, state, &table_ctx).await;
                let table_name = table_ctx.name.to_string();
                created_table.insert(table_name);
            }
            Err(err) => {
                ensure!(
                    state.killed.load(Ordering::Relaxed),
                    error::UnexpectedSnafu {
                        err_msg: err.to_string(),
                    }
                );
                break;
            }
        }
    }

    Ok(())
}

async fn validate_mysql(client: &Pool<MySql>, _state: &Arc<TestState>, table_ctx: &TableContext) {
    loop {
        match validator::column::fetch_columns_via_mysql(
            client,
            "public".into(),
            table_ctx.name.clone(),
        )
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

async fn start_database() -> Result<Pid> {
    let binary_path = "/home/weny/Projects/greptimedb-cuckoo/target/debug/greptime";
    let test_dir = "/tmp/greptimedb-cuckoo/";
    let template_filename = "standalone-v0.3.2.toml.template";
    let health_url = "http://127.0.0.1:4000/health";

    let process_manager = ProcessManager::new();
    for _ in 0..3 {
        let pid = start_process(&process_manager, binary_path, test_dir, template_filename)
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(10), health_check(health_url)).await {
            Ok(_) => {
                info!("GreptimeDB started, pid: {pid}");
                return Ok(pid);
            }
            Err(_) => {
                ensure!(
                    process_manager.get(pid).unwrap().exited,
                    error::UnexpectedSnafu {
                        err_msg: format!("Failed to start database: pid: {pid}")
                    }
                );
                // retry alter
                warn!("Wait for staring timeout, retry later...");
            }
        };
    }
    error::UnexpectedSnafu {
        err_msg: "Failed to start datanode",
    }
    .fail()
}

async fn start_process(
    process_manager: &ProcessManager,
    binary: &str,
    test_dir: &str,
    template_filename: &str,
) -> Result<Pid> {
    tokio::fs::create_dir_all(test_dir)
        .await
        .context(error::CreateDirAllSnafu)?;

    let data_home = format!("{test_dir}data_home");
    info!("data home: {}", data_home);

    // Prepares the config file
    let mut conf_path = get_conf_path();
    conf_path.push(template_filename);
    let template_path = conf_path.to_str().unwrap().to_string();

    let conf_path = format!("{test_dir}config.toml");
    info!("conf path: {}", conf_path);
    #[derive(Serialize)]
    struct Context {
        data_home: String,
    }
    let conf_content = render_config_file(&template_path, &Context { data_home });
    let mut config_file = File::create(&conf_path)
        .await
        .context(error::CreateFileSnafu { path: &conf_path })?;
    config_file
        .write_all(conf_content.as_bytes())
        .await
        .context(error::WriteFileSnafu { path: &conf_path })?;

    let args = vec![
        DEFAULT_LOG_LEVEL.to_string(),
        "standalone".to_string(),
        "start".to_string(),
        format!("--config-file={conf_path}"),
    ];

    let now = common_time::util::current_time_millis();
    let stdout = format!("{test_dir}stdout-{}", now);
    let stderr = format!("{test_dir}stderr-{}", now);
    info!("stdout: {}, stderr: {}", stdout, stderr);
    let stdout = path_to_stdio(&stdout).await?;
    let stderr = path_to_stdio(&stderr).await?;

    let on_exit = move |pid, result| {
        info!("The pid: {pid} exited, result: {result:?}");
    };

    process_manager.spawn(binary, &args, stdout, stderr, on_exit)
}

async fn health_check(url: &str) {
    loop {
        match reqwest::get(url).await {
            Ok(resp) => {
                if resp.status() == 200 {
                    info!("health checked!");
                    return;
                }
                info!("failed to health, status: {}", resp.status());
            }
            Err(err) => {
                info!("failed to health, err: {err:?}");
            }
        }

        info!("checking health later...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
