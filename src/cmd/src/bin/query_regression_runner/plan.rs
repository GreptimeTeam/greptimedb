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
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

use crate::query_regression_runner::Result;
use crate::query_regression_runner::model::{
    Layout, OtlpLoad, Query, RemoteWrite, Scenario, Table,
};

pub(super) fn load_plan(generator: &PathBuf, case_path: &PathBuf) -> Result<Value> {
    let output = Command::new(generator)
        .args(["plan", "--case"])
        .arg(case_path)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("query_perf_fixture plan failed: {:.2000}", stderr).into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

pub(super) fn normalize_scenario(scenario: Scenario) -> Result<(Vec<Table>, Vec<Query>)> {
    match scenario {
        Scenario::DirectReadableSst {
            tables,
            layout,
            queries,
        } => Ok((validate_direct_tables(tables, layout)?, queries)),
        Scenario::PromRemoteWriteThenQuery {
            remote_write,
            queries,
        } => Ok((
            vec![Table {
                database: remote_write.database,
                name: remote_write.metric,
                engine: "metric".to_string(),
                columns: vec![],
                primary_key: vec![],
                time_index: None,
                append_mode: None,
                sst_format: None,
                validate_show_create_engine: false,
            }],
            queries,
        )),
        Scenario::OtlpTraceLoad { .. } => {
            Err("measure requires a query scenario, not otlp_trace_load".into())
        }
        Scenario::WriteThroughput { .. } => {
            Err("measure requires a query scenario, not write_throughput".into())
        }
    }
}

pub(super) fn validate_direct_tables(tables: Vec<Table>, layout: Layout) -> Result<Vec<Table>> {
    if tables.is_empty() || layout.regions != 1 {
        return Err("runner supports one or more tables and exactly one region per table".into());
    }
    let mut pairs = HashSet::new();
    let mut names = HashSet::new();
    for table in &tables {
        if !pairs.insert((&table.database, &table.name)) {
            return Err("duplicate (database, name) table entries are not supported".into());
        }
        if !names.insert(&table.name) {
            return Err("duplicate table names are not supported".into());
        }
    }
    Ok(tables)
}

pub(super) fn normalized_remote_write(
    generator: &PathBuf,
    case: &Path,
) -> Result<(PathBuf, RemoteWrite)> {
    let case_path = case.canonicalize()?;
    let plan = load_plan(generator, &case_path)?;
    let scenario = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    match serde_json::from_value(scenario)? {
        Scenario::PromRemoteWriteThenQuery { remote_write, .. } => Ok((case_path, remote_write)),
        Scenario::DirectReadableSst { .. } => {
            Err("remote command requires scenario kind prom_remote_write_then_query".into())
        }
        Scenario::OtlpTraceLoad { .. } => {
            Err("remote command requires scenario kind prom_remote_write_then_query".into())
        }
        Scenario::WriteThroughput { .. } => {
            Err("remote command requires scenario kind prom_remote_write_then_query".into())
        }
    }
}

pub(super) fn normalized_otlp_load(
    generator: &PathBuf,
    case: &Path,
) -> Result<(PathBuf, OtlpLoad)> {
    let case_path = case.canonicalize()?;
    let plan = load_plan(generator, &case_path)?;
    let scenario = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    match serde_json::from_value(scenario)? {
        Scenario::OtlpTraceLoad { load } => Ok((case_path, load.load)),
        Scenario::DirectReadableSst { .. }
        | Scenario::PromRemoteWriteThenQuery { .. }
        | Scenario::WriteThroughput { .. } => {
            Err("OTLP command requires scenario kind otlp_trace_load".into())
        }
    }
}
