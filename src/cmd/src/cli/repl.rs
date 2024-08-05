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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cache::{
    build_fundamental_cache_registry, with_default_composite_cache_registry, TABLE_CACHE_NAME,
    TABLE_ROUTE_CACHE_NAME,
};
use catalog::kvbackend::{
    CachedMetaKvBackend, CachedMetaKvBackendBuilder, KvBackendCatalogManager, MetaKvBackend,
};
use client::{Client, Database, OutputData, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_base::Plugins;
use common_config::Mode;
use common_error::ext::ErrorExt;
use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::debug;
use either::Either;
use meta_client::client::MetaClientBuilder;
use query::datafusion::DatafusionQueryEngine;
use query::parser::QueryLanguageParser;
use query::plan::LogicalPlan;
use query::query_engine::{DefaultSerializer, QueryEngineState};
use query::QueryEngine;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::cli::cmd::ReplCommand;
use crate::cli::helper::RustylineHelper;
use crate::cli::AttachCommand;
use crate::error;
use crate::error::{
    CollectRecordBatchesSnafu, ParseSqlSnafu, PlanStatementSnafu, PrettyPrintRecordBatchesSnafu,
    ReadlineSnafu, ReplCreationSnafu, RequestDatabaseSnafu, Result, StartMetaClientSnafu,
    SubstraitEncodeLogicalPlanSnafu,
};

/// Captures the state of the repl, gathers commands and executes them one by one
pub struct Repl {
    /// Rustyline editor for interacting with user on command line
    rl: Editor<RustylineHelper>,

    /// Current prompt
    prompt: String,

    /// Client for interacting with GreptimeDB
    database: Database,

    query_engine: Option<DatafusionQueryEngine>,
}

#[allow(clippy::print_stdout)]
impl Repl {
    fn print_help(&self) {
        println!("{}", ReplCommand::help())
    }

    pub(crate) async fn try_new(cmd: &AttachCommand) -> Result<Self> {
        let mut rl = Editor::new().context(ReplCreationSnafu)?;

        if !cmd.disable_helper {
            rl.set_helper(Some(RustylineHelper::default()));

            let history_file = history_file();
            if let Err(e) = rl.load_history(&history_file) {
                debug!(
                    "failed to load history file on {}, error: {e}",
                    history_file.display()
                );
            }
        }

        let client = Client::with_urls([&cmd.grpc_addr]);
        let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);

        let query_engine = if let Some(meta_addr) = &cmd.meta_addr {
            create_query_engine(meta_addr).await.map(Some)?
        } else {
            None
        };

        Ok(Self {
            rl,
            prompt: "> ".to_string(),
            database,
            query_engine,
        })
    }

    /// Parse the next command
    fn next_command(&mut self) -> Result<ReplCommand> {
        match self.rl.readline(&self.prompt) {
            Ok(ref line) => {
                let request = line.trim();

                let _ = self.rl.add_history_entry(request.to_string());

                request.try_into()
            }
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => Ok(ReplCommand::Exit),
            // Some sort of real underlying error
            Err(e) => Err(e).context(ReadlineSnafu),
        }
    }

    /// Read Evaluate Print Loop (interactive command line) for GreptimeDB
    ///
    /// Inspired / based on repl.rs from InfluxDB IOX
    pub(crate) async fn run(&mut self) -> Result<()> {
        println!("Ready for commands. (Hint: try 'help')");

        loop {
            match self.next_command()? {
                ReplCommand::Help => {
                    self.print_help();
                }
                ReplCommand::UseDatabase { db_name } => {
                    if self.execute_sql(format!("USE {db_name}")).await {
                        println!("Using {db_name}");
                        self.database.set_schema(&db_name);
                        self.prompt = format!("[{db_name}] > ");
                    }
                }
                ReplCommand::Sql { sql } => {
                    let _ = self.execute_sql(sql).await;
                }
                ReplCommand::Exit => {
                    return Ok(());
                }
            }
        }
    }

    async fn execute_sql(&self, sql: String) -> bool {
        self.do_execute_sql(sql)
            .await
            .map_err(|e| {
                let status_code = e.status_code();
                let root_cause = e.output_msg();
                println!("Error: {}({status_code}), {root_cause}", status_code as u32)
            })
            .is_ok()
    }

    async fn do_execute_sql(&self, sql: String) -> Result<()> {
        let start = Instant::now();

        let output = if let Some(query_engine) = &self.query_engine {
            let query_ctx = Arc::new(QueryContext::with(
                self.database.catalog(),
                self.database.schema(),
            ));

            let stmt = QueryLanguageParser::parse_sql(&sql, &query_ctx)
                .with_context(|_| ParseSqlSnafu { sql: sql.clone() })?;

            let plan = query_engine
                .planner()
                .plan(stmt, query_ctx.clone())
                .await
                .context(PlanStatementSnafu)?;

            let LogicalPlan::DfPlan(plan) = query_engine
                .optimize(&query_engine.engine_context(query_ctx), &plan)
                .context(PlanStatementSnafu)?;

            let plan = DFLogicalSubstraitConvertor {}
                .encode(&plan, DefaultSerializer)
                .context(SubstraitEncodeLogicalPlanSnafu)?;

            self.database.logical_plan(plan.to_vec()).await
        } else {
            self.database.sql(&sql).await
        }
        .context(RequestDatabaseSnafu { sql: &sql })?;

        let either = match output.data {
            OutputData::Stream(s) => {
                let x = RecordBatches::try_collect(s)
                    .await
                    .context(CollectRecordBatchesSnafu)?;
                Either::Left(x)
            }
            OutputData::RecordBatches(x) => Either::Left(x),
            OutputData::AffectedRows(rows) => Either::Right(rows),
        };

        let end = Instant::now();

        match either {
            Either::Left(recordbatches) => {
                let total_rows: usize = recordbatches.iter().map(|x| x.num_rows()).sum();
                if total_rows > 0 {
                    println!(
                        "{}",
                        recordbatches
                            .pretty_print()
                            .context(PrettyPrintRecordBatchesSnafu)?
                    );
                }
                println!("Total Rows: {total_rows}")
            }
            Either::Right(rows) => println!("Affected Rows: {rows}"),
        };

        println!("Cost {} ms", (end - start).as_millis());
        Ok(())
    }
}

impl Drop for Repl {
    fn drop(&mut self) {
        if self.rl.helper().is_some() {
            let history_file = history_file();
            if let Err(e) = self.rl.save_history(&history_file) {
                debug!(
                    "failed to save history file on {}, error: {e}",
                    history_file.display()
                );
            }
        }
    }
}

/// Return the location of the history file (defaults to $HOME/".greptimedb_cli_history")
fn history_file() -> PathBuf {
    let mut buf = match std::env::var("HOME") {
        Ok(home) => PathBuf::from(home),
        Err(_) => PathBuf::new(),
    };
    buf.push(".greptimedb_cli_history");
    buf
}

async fn create_query_engine(meta_addr: &str) -> Result<DatafusionQueryEngine> {
    let mut meta_client = MetaClientBuilder::default().enable_store().build();
    meta_client
        .start([meta_addr])
        .await
        .context(StartMetaClientSnafu)?;
    let meta_client = Arc::new(meta_client);

    let cached_meta_backend =
        Arc::new(CachedMetaKvBackendBuilder::new(meta_client.clone()).build());
    let layered_cache_builder = LayeredCacheRegistryBuilder::default().add_cache_registry(
        CacheRegistryBuilder::default()
            .add_cache(cached_meta_backend.clone())
            .build(),
    );
    let fundamental_cache_registry =
        build_fundamental_cache_registry(Arc::new(MetaKvBackend::new(meta_client.clone())));
    let layered_cache_registry = Arc::new(
        with_default_composite_cache_registry(
            layered_cache_builder.add_cache_registry(fundamental_cache_registry),
        )
        .context(error::BuildCacheRegistrySnafu)?
        .build(),
    );

    let catalog_manager = KvBackendCatalogManager::new(
        Mode::Distributed,
        Some(meta_client.clone()),
        cached_meta_backend.clone(),
        layered_cache_registry,
    );
    let plugins: Plugins = Default::default();
    let state = Arc::new(QueryEngineState::new(
        catalog_manager,
        None,
        None,
        None,
        None,
        false,
        plugins.clone(),
    ));

    Ok(DatafusionQueryEngine::new(state, plugins))
}
