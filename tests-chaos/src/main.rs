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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::Router;
use bare::process::{Pid, ProcessManager};
use common_telemetry::{info, warn};
use mysql::prelude::Queryable;
use nix::sys::signal::Signal;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::Serialize;
use snafu::{ensure, ResultExt};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};
use tests_fuzz::context::{Rows, TableContext};
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::select_expr::{Direction, SelectExpr};
use tests_fuzz::ir::AlterTableOperation;
use tests_fuzz::translator::mysql::alter_expr::AlterTableExprTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::mysql::select_expr::SelectExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::validator;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use utils::generate_alter_table_expr;
mod bare;
mod error;
mod utils;

use axum::routing::get;
use prometheus::{register_int_counter, Encoder, IntCounter, TextEncoder};

use crate::error::{Error, RequestMysqlSnafu, Result};
use crate::utils::{generate_create_table_expr, get_conf_path, path_to_stdio, render_config_file};
const DEFAULT_LOG_LEVEL: &str = "--log-level=debug,hyper=warn,tower=warn,datafusion=warn,reqwest=warn,sqlparser=warn,h2=info,opendal=info";

#[derive(Copy, Clone)]
pub struct MetricsHandler;

impl MetricsHandler {
    pub fn render(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        // Gather the metrics.
        let metric_families = prometheus::gather();
        // Encode them to send.
        match encoder.encode(&metric_families, &mut buffer) {
            Ok(_) => match String::from_utf8(buffer) {
                Ok(s) => s,
                Err(e) => e.to_string(),
            },
            Err(e) => e.to_string(),
        }
    }
}

#[axum_macros::debug_handler]
pub async fn metrics(
    State(state): State<MetricsHandler>,
    Query(_params): Query<HashMap<String, String>>,
) -> String {
    state.render()
}

lazy_static::lazy_static! {
    static ref UP_COUNTER: IntCounter = register_int_counter!("up", "up counter").unwrap();
}

// cargo run --package tests-chaos --bin tests-chaos
#[tokio::main]
async fn main() {
    common_telemetry::init_default_ut_logging();

    tokio::spawn(async move {
        let app = Router::new()
            .route("/metric", get(metrics))
            .with_state(MetricsHandler);
        let addr = SocketAddr::from(([0, 0, 0, 0], 30000));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    let test_dir = "/home/lfc/test-cuckoo/";
    // Remove everything in the test directory, to make sure we have a clean start.
    match std::fs::remove_dir_all(test_dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => panic!("{e:?}"),
        _ => {}
    }
    std::fs::create_dir_all(test_dir).unwrap();

    let state = Arc::new(TestState {
        killed: AtomicBool::new(false),
    });

    let moved_state = state.clone();
    tokio::spawn(async move {
        let mut rng = ChaChaRng::seed_from_u64(0);
        loop {
            warn!("Staring");
            UP_COUNTER.inc();
            let pid = start_database(test_dir).await.unwrap();
            let secs = rng.gen_range(100..300);
            moved_state.killed.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(secs)).await;
            warn!("After {secs}ms, Killing pid: {pid}");
            moved_state.killed.store(true, Ordering::Relaxed);

            // Flush the database before restarting it. Because cuckoo does not enable WAL,
            // data may not survive the restart if not flush them.
            flush_db().await;

            ProcessManager::kill(pid, Signal::SIGKILL).expect("Failed to kill");
        }
    });
    let mut rng = ChaChaRng::seed_from_u64(0);
    let mut sqlx = sqlx_connections().await;
    let mut mysql = mysql_connections().await;
    let mut created_table = HashSet::new();

    // Runs maximum 10000 times.
    for _i in 0..10000 {
        if let Err(e) = run_test(&sqlx, &mysql, &mut created_table, &state, &mut rng).await {
            if matches!(e, Error::ExecuteQuery { .. } | Error::RequestMysql { .. })
                && state.killed.load(Ordering::Relaxed)
            {
                // If the query error is caused by restarting the database
                // (which is an intended action), reconnect.
                sqlx = sqlx_connections().await;
                mysql = mysql_connections().await;
            } else {
                panic!("{e:?}");
            }
        }
    }
    info!("Successfully runs DDL chaos testing for cuckoo!");
}

async fn flush_db() {
    info!("Start flushing the database ...");
    let _ = reqwest::get("http://127.0.0.1:4000/v1/admin/flush?db=public")
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
}

async fn mysql_connections() -> mysql::Pool {
    let mut max_retry = 10;
    loop {
        match mysql::Pool::new("mysql://127.0.0.1:4002/public") {
            Ok(x) => return x,
            Err(e) => {
                max_retry -= 1;
                if max_retry == 0 {
                    panic!("{e:?}")
                } else {
                    info!("GreptimeDB is not connectable, maybe during restart. Wait 1 second to retry");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

async fn sqlx_connections() -> Pool<MySql> {
    let mut max_retry = 10;
    loop {
        match MySqlPoolOptions::new()
            .connect("mysql://127.0.0.1:4002/public")
            .await
        {
            Ok(x) => return x,
            Err(e) => {
                max_retry -= 1;
                if max_retry == 0 {
                    panic!("{e:?}")
                } else {
                    info!("GreptimeDB is not connectable, maybe during restart. Wait 1 second to retry");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

struct TestState {
    killed: AtomicBool,
}

async fn run_test<R: Rng + 'static>(
    client: &Pool<MySql>,
    pool: &mysql::Pool,
    created_table: &mut HashSet<String>,
    state: &Arc<TestState>,
    rng: &mut R,
) -> Result<()> {
    let expr = generate_create_table_expr(rng);
    let table_name = expr.table_name.to_string();
    if !created_table.insert(table_name.clone()) {
        warn!("ignores same name table: {table_name}");
        // ignores.
        return Ok(());
    }

    let mut table_ctx = TableContext::from(&expr);

    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&expr).unwrap();
    let result = sqlx::query(&sql).execute(client).await;

    match result {
        Ok(result) => {
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
            return Ok(());
        }
    }

    let actions = rng.gen_range(1..20);

    for _ in 0..actions {
        let expr = generate_alter_table_expr(Arc::new(table_ctx.clone()), rng);
        if let AlterTableOperation::RenameTable { new_table_name } = &expr.alter_options {
            let table_name = new_table_name.to_string();
            if created_table.contains(&table_name) {
                warn!("ignores altering to same name table: {table_name}");
                continue;
            }
        };

        insert_rows(pool, &mut table_ctx, rng)?;

        let translator = AlterTableExprTranslator;
        let sql = translator.translate(&expr).unwrap();
        let result = sqlx::query(&sql).execute(client).await;
        match result {
            Ok(result) => {
                info!("alter table: {sql}, result: {result:?}");
                let table_name = table_ctx.name.to_string();
                created_table.remove(&table_name);
                table_ctx.alter(expr)?;
                validate_mysql(client, state, &table_ctx).await;
                let table_name = table_ctx.name.to_string();
                created_table.insert(table_name);
            }
            Err(err) => {
                table_ctx.alter(expr)?;
                let table_name = table_ctx.name.to_string();
                created_table.insert(table_name);
                ensure!(
                    state.killed.load(Ordering::Relaxed),
                    error::UnexpectedSnafu {
                        err_msg: err.to_string(),
                    }
                );
                break;
            }
        }

        let actual_rows = fetch_all_rows(&table_ctx, pool)?;
        info!("fetch all rows after alter: {actual_rows}");
        if actual_rows.empty() && state.killed.load(Ordering::Relaxed) {
            // Cuckoo does not have WAL enabled; therefore the data could be cleared across restart.
            // When that happened, clear the saved data that are used for comparison, too.
            table_ctx.clear_data();
        } else {
            assert_eq!(
                &table_ctx.rows, &actual_rows,
                r#"rows not equal:
expect: {}
actual: {}"#,
                &table_ctx.rows, &actual_rows
            )
        }
    }

    Ok(())
}

fn insert_rows<R: Rng + 'static>(
    pool: &mysql::Pool,
    table_ctx: &mut TableContext,
    rng: &mut R,
) -> Result<()> {
    let insert_expr = InsertExprGeneratorBuilder::default()
        .table_ctx(Arc::new(table_ctx.clone()))
        .build()
        .unwrap()
        .generate(rng)
        .unwrap();
    let sql = InsertIntoExprTranslator.translate(&insert_expr).unwrap();
    info!("executing insertion: {sql}");

    let mut conn = pool.get_conn().context(RequestMysqlSnafu {
        err_msg: "get connection",
    })?;
    conn.query_drop(&sql).with_context(|_| RequestMysqlSnafu {
        err_msg: format!("executing sql '{}'", sql),
    })?;

    if conn.affected_rows() > 0 {
        table_ctx.insert(insert_expr)?;
    }
    Ok(())
}

// Sqlx treats all queries as prepared, we have to switch to another mysql client library here.
// There's a error when trying to query GreptimeDB with prepared statement:
// "tried to use [50, 48, ..., 56] as MYSQL_TYPE_TIMESTAMP"
// Besides, sqlx is suited for cases where table schema is known (and representable in codes),
// definitely not here.
fn fetch_all_rows(table_ctx: &TableContext, pool: &mysql::Pool) -> Result<Rows> {
    let select_expr = SelectExpr {
        table_name: table_ctx.name.to_string(),
        columns: table_ctx.columns.clone(),
        order_by: vec![table_ctx
            .columns
            .iter()
            .find_map(|c| {
                if c.is_time_index() {
                    Some(c.name.to_string())
                } else {
                    None
                }
            })
            .unwrap()],
        direction: Direction::Asc,
        limit: usize::MAX,
    };
    let sql = SelectExprTranslator.translate(&select_expr).unwrap();
    info!("executing selection: {sql}");

    let mut conn = pool.get_conn().context(RequestMysqlSnafu {
        err_msg: "get connection",
    })?;
    let rows: Vec<mysql::Row> = conn.query(&sql).with_context(|_| RequestMysqlSnafu {
        err_msg: format!("executing sql: {}", sql),
    })?;

    Ok(Rows::fill(rows))
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

async fn start_database(test_dir: &str) -> Result<Pid> {
    let binary_path = "/home/lfc/greptimedb-cuckoo/target/debug/greptime";
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
