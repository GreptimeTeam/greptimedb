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
use std::time::Instant;

use client::{Client, Database};
use common_error::prelude::ErrorExt;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::logging;
use either::Either;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use snafu::{ErrorCompat, ResultExt};

use crate::cli::cmd::ReplCommand;
use crate::cli::helper::RustylineHelper;
use crate::cli::AttachCommand;
use crate::error::{
    CollectRecordBatchesSnafu, PrettyPrintRecordBatchesSnafu, ReadlineSnafu, ReplCreationSnafu,
    RequestDatabaseSnafu, Result,
};

/// Captures the state of the repl, gathers commands and executes them one by one
pub(crate) struct Repl {
    /// Rustyline editor for interacting with user on command line
    rl: Editor<RustylineHelper>,

    /// Current prompt
    prompt: String,

    /// Client for interacting with GreptimeDB
    database: Database,
}

#[allow(clippy::print_stdout)]
impl Repl {
    fn print_help(&self) {
        println!("{}", ReplCommand::help())
    }

    pub(crate) fn try_new(cmd: &AttachCommand) -> Result<Self> {
        let mut rl = Editor::new().context(ReplCreationSnafu)?;

        if !cmd.disable_helper {
            rl.set_helper(Some(RustylineHelper::default()));

            let history_file = history_file();
            if let Err(e) = rl.load_history(&history_file) {
                logging::debug!(
                    "failed to load history file on {}, error: {e}",
                    history_file.display()
                );
            }
        }

        let client = Client::with_urls([&cmd.grpc_addr]);
        let database = Database::with_client(client);

        Ok(Self {
            rl,
            prompt: "> ".to_string(),
            database,
        })
    }

    /// Parse the next command
    fn next_command(&mut self) -> Result<ReplCommand> {
        match self.rl.readline(&self.prompt) {
            Ok(ref line) => {
                let request = line.trim();

                self.rl.add_history_entry(request.to_string());

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
                    self.execute_sql(sql).await;
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
                let root_cause = e.iter_chain().last().unwrap();
                println!("Error: {}({status_code}), {root_cause}", status_code as u32)
            })
            .is_ok()
    }

    async fn do_execute_sql(&self, sql: String) -> Result<()> {
        let start = Instant::now();

        let output = self
            .database
            .sql(&sql)
            .await
            .context(RequestDatabaseSnafu { sql: &sql })?;

        let either = match output {
            Output::Stream(s) => {
                let x = RecordBatches::try_collect(s)
                    .await
                    .context(CollectRecordBatchesSnafu)?;
                Either::Left(x)
            }
            Output::RecordBatches(x) => Either::Left(x),
            Output::AffectedRows(rows) => Either::Right(rows),
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
                logging::debug!(
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
