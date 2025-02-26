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

use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

use crate::ir::create_expr::{CreateDatabaseExprBuilderError, CreateTableExprBuilderError};
#[cfg(feature = "unstable")]
use crate::utils::process::Pid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to create a file: {}", path))]
    CreateFile {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to write a file: {}", path))]
    WriteFile {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Unexpected, violated: {violated}"))]
    Unexpected {
        violated: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build create table expr"))]
    BuildCreateTableExpr {
        #[snafu(source)]
        error: CreateTableExprBuilderError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build create database expr"))]
    BuildCreateDatabaseExpr {
        #[snafu(source)]
        error: CreateDatabaseExprBuilderError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No droppable columns"))]
    DroppableColumns {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute query: {}", sql))]
    ExecuteQuery {
        sql: String,
        #[snafu(source)]
        error: sqlx::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to assert: {}", reason))]
    Assert {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Child process exited unexpected"))]
    UnexpectedExited {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to spawn a child process"))]
    SpawnChild {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[cfg(feature = "unstable")]
    #[snafu(display("Failed to kill a process, pid: {}", pid))]
    KillProcess {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: nix::Error,
        pid: Pid,
    },
}
