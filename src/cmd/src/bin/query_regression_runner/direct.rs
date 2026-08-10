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

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};

use crate::query_regression_runner::model::{Scenario, Table};
use crate::query_regression_runner::plan::{load_plan, validate_direct_tables};
use crate::query_regression_runner::sql::{
    extract_rows, http_post_sql, row_u64, row_value, sql_ident, sql_string, value_text,
};
use crate::query_regression_runner::{PrepareDirectArgs, Result};

#[derive(Clone, Debug, Serialize)]
struct Discovery {
    table: String,
    catalog: String,
    schema: String,
    table_id: u64,
    region_id: u64,
    region_seq: u64,
    table_dir: String,
    region_dir: String,
    peer_id: u64,
    peer_addr: Value,
    is_leader: Value,
    status: Value,
    discovery_queries: Value,
}

#[derive(Debug)]
struct PreparedTarget {
    creates: Vec<Value>,
    discoveries: Vec<Discovery>,
}

pub(super) async fn run_prepare_direct(args: PrepareDirectArgs) -> Result<()> {
    if !args.http_timeout.is_finite() || args.http_timeout < 0.0 {
        return Err("--http-timeout must be a non-negative finite number".into());
    }
    let case_path = args.case.canonicalize()?;
    let plan = load_plan(&args.fixture_generator, &case_path)?;
    let scenario_value = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    let scenario: Scenario = serde_json::from_value(scenario_value)?;
    let tables = direct_tables(scenario)?;
    let client = Client::builder()
        .timeout(Duration::from_secs_f64(args.http_timeout))
        .build()?;
    let base = prepare_target("base", args.base_http_port, &tables, &client).await?;
    let candidate = prepare_target("candidate", args.candidate_http_port, &tables, &client).await?;
    if base.discoveries.len() != candidate.discoveries.len()
        || base
            .discoveries
            .iter()
            .zip(&candidate.discoveries)
            .any(|(base, candidate)| {
                base.table != candidate.table
                    || base.region_id != candidate.region_id
                    || base.table_dir != candidate.table_dir
            })
    {
        return Err(format!(
            "base/candidate metadata mismatch: base={:?}, candidate={:?}",
            base.discoveries, candidate.discoveries
        )
        .into());
    }

    let mut fixtures = Vec::with_capacity(tables.len());
    for (index, (table, discovery)) in tables.iter().zip(&base.discoveries).enumerate() {
        let fixture_dir = table_fixture_dir(&args.fixture_dir, &tables, table, index);
        fixtures.push(generate_direct_fixture(
            &args.fixture_generator,
            &case_path,
            &fixture_dir,
            table,
            discovery,
            tables.len() > 1,
            args.allow_large_fixture,
        )?);
    }

    let report = json!({
        "case_path": case_path,
        "scenario": "direct_readable_sst",
        "base": { "create_table": base.creates, "discovery": base.discoveries },
        "candidate": { "create_table": candidate.creates, "discovery": candidate.discoveries },
        "fixtures": fixtures,
    });
    let text = format!("{}\n", serde_json::to_string_pretty(&report)?);
    if let Some(path) = args.output {
        fs::write(path, &text)?;
    }
    print!("{text}");
    Ok(())
}

fn direct_tables(scenario: Scenario) -> Result<Vec<Table>> {
    match scenario {
        Scenario::DirectReadableSst { tables, layout, .. } => {
            validate_direct_tables(tables, layout)
        }
        Scenario::PromRemoteWriteThenQuery { .. } => {
            Err("prepare-direct requires scenario kind direct_readable_sst".into())
        }
        Scenario::OtlpTraceLoad { .. } => {
            Err("prepare-direct requires scenario kind direct_readable_sst".into())
        }
        Scenario::WriteThroughput { .. } => {
            Err("prepare-direct requires scenario kind direct_readable_sst".into())
        }
    }
}

async fn prepare_target(
    name: &str,
    port: u16,
    tables: &[Table],
    client: &Client,
) -> Result<PreparedTarget> {
    let mut creates = Vec::with_capacity(tables.len());
    let mut discoveries = Vec::with_capacity(tables.len());
    for table in tables {
        let sql = create_table_sql(table)?;
        let result = http_post_sql(client, port, &sql, &table.database).await;
        if !result["ok"].as_bool().unwrap_or(false) {
            return Err(format!("CREATE TABLE {} failed for {name}: {result}", table.name).into());
        }
        creates.push(json!({ "table": table.name, "sql": sql, "result": result }));
        discoveries.push(discover_region(client, port, table).await?);
    }
    Ok(PreparedTarget {
        creates,
        discoveries,
    })
}

fn create_table_sql(table: &Table) -> Result<String> {
    if table.engine != "mito" {
        return Err("prepare-direct requires table engine mito".into());
    }
    let time_index = table
        .time_index
        .as_deref()
        .ok_or("direct-SST table requires time_index")?;
    if table.columns.is_empty() {
        return Err("direct-SST table requires columns".into());
    }
    let columns = table
        .columns
        .iter()
        .map(|column| format!("{} {}", sql_ident(&column.name), column.data_type))
        .collect::<Vec<_>>()
        .join(",\n  ");
    let primary_key = table
        .primary_key
        .iter()
        .map(|column| sql_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut options = Vec::new();
    if let Some(append_mode) = table.append_mode {
        options.push(("append_mode", append_mode.to_string()));
    }
    if let Some(sst_format) = table
        .sst_format
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        options.push(("sst_format", sst_format.to_string()));
    }
    let with = (!options.is_empty()).then(|| {
        format!(
            "\nWITH ({})",
            options
                .iter()
                .map(|(key, value)| format!("'{key}'='{value}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });
    Ok(format!(
        "CREATE TABLE {} (\n  {columns},\n  TIME INDEX ({}),\n  PRIMARY KEY ({primary_key})\n) ENGINE=mito{};",
        sql_ident(&table.name),
        sql_ident(time_index),
        with.unwrap_or_default()
    ))
}

async fn discover_region(client: &Client, port: u16, table: &Table) -> Result<Discovery> {
    let schema = sql_string(&table.database);
    let table_name = sql_string(&table.name);
    let table_sql = format!(
        "SELECT table_id FROM information_schema.tables WHERE table_schema = {schema} AND table_name = {table_name}"
    );
    let table_result = http_post_sql(client, port, &table_sql, &table.database).await;
    if !table_result["ok"].as_bool().unwrap_or(false) {
        return Err(format!("table_id discovery failed: {table_result}").into());
    }
    let table_rows = extract_rows(table_result.get("response").unwrap_or(&Value::Null));
    if table_rows.len() != 1 {
        return Err(format!(
            "expected one information_schema.tables row, got {}: {table_result}",
            table_rows.len()
        )
        .into());
    }
    let table_id = row_u64(&table_rows[0], 0, "table_id")?;

    let region_sql = format!(
        "SELECT region_id, peer_id, peer_addr, is_leader, status FROM information_schema.region_peers WHERE table_schema = {schema} AND table_name = {table_name}"
    );
    let region_result = http_post_sql(client, port, &region_sql, &table.database).await;
    if !region_result["ok"].as_bool().unwrap_or(false) {
        return Err(format!("region_peers discovery failed: {region_result}").into());
    }
    let region_rows = extract_rows(region_result.get("response").unwrap_or(&Value::Null));
    if region_rows.len() != 1 {
        return Err(format!(
            "expected one information_schema.region_peers row, got {}: {region_result}",
            region_rows.len()
        )
        .into());
    }
    let row = &region_rows[0];
    let region_id = row_u64(row, 0, "region_id")?;
    let peer_id = row_u64(row, 1, "peer_id")?;
    let peer_addr = row_value(row, 2, "peer_addr")
        .cloned()
        .unwrap_or(Value::Null);
    let is_leader = row_value(row, 3, "is_leader")
        .cloned()
        .unwrap_or(Value::Null);
    let status = row_value(row, 4, "status").cloned().unwrap_or(Value::Null);
    if !matches!(
        value_text(&is_leader).to_ascii_lowercase().as_str(),
        "yes" | "true"
    ) {
        return Err(format!(
            "expected leader region peer, got is_leader={is_leader}: {region_result}"
        )
        .into());
    }
    if !value_text(&status).eq_ignore_ascii_case("ALIVE") {
        return Err(
            format!("expected ALIVE region peer, got status={status}: {region_result}").into(),
        );
    }
    if peer_id != 0 {
        return Err(format!("expected region leader on datanode peer_id=0, got {peer_id}").into());
    }
    if region_id >> 32 != table_id {
        return Err(format!(
            "table_id mismatch: tables={table_id}, region_id-derived={}",
            region_id >> 32
        )
        .into());
    }
    let region_seq = region_id & u32::MAX as u64;
    let (table_dir, region_dir) = storage_paths(&table.database, table_id, region_seq);
    Ok(Discovery {
        table: table.name.clone(),
        catalog: "greptime".to_string(),
        schema: table.database.clone(),
        table_id,
        region_id,
        region_seq,
        table_dir: table_dir.clone(),
        region_dir,
        peer_id,
        peer_addr,
        is_leader,
        status,
        discovery_queries: json!({ "table": table_result, "region": region_result }),
    })
}

fn storage_paths(database: &str, table_id: u64, region_seq: u64) -> (String, String) {
    let table_dir = format!("data/greptime/{database}/{table_id}/");
    let region_dir = format!("{table_dir}{table_id}_{region_seq:010}");
    (table_dir, region_dir)
}

fn table_fixture_dir(root: &Path, tables: &[Table], table: &Table, index: usize) -> PathBuf {
    if tables.len() == 1 {
        root.to_path_buf()
    } else {
        root.join(fixture_subdir(table, index))
    }
}

fn fixture_subdir(table: &Table, index: usize) -> String {
    let raw = format!("{index:02}_{}_{}", table.database, table.name);
    let mut safe = String::new();
    let mut replaced = false;
    for character in raw.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '_' | '.' | '-') {
            safe.push(character);
            replaced = false;
        } else if !replaced {
            safe.push('_');
            replaced = true;
        }
    }
    let safe = safe.trim_matches(['.', '_', '-']);
    if safe.is_empty() {
        format!("table_{index:02}")
    } else {
        safe.to_string()
    }
}

fn generate_direct_fixture(
    generator: &Path,
    case_path: &Path,
    fixture_dir: &Path,
    table: &Table,
    discovery: &Discovery,
    multi_table: bool,
    allow_large_fixture: bool,
) -> Result<Value> {
    if fixture_dir.exists() {
        fs::remove_dir_all(fixture_dir)?;
    }
    fs::create_dir_all(fixture_dir)?;
    let mut command = vec![
        generator.to_string_lossy().to_string(),
        "direct-sst".to_string(),
        "--case".to_string(),
        case_path.to_string_lossy().to_string(),
        "--out-dir".to_string(),
        fixture_dir.to_string_lossy().to_string(),
    ];
    if multi_table {
        command.extend(["--table".to_string(), table.name.clone()]);
    }
    command.extend([
        "--region-id".to_string(),
        discovery.region_id.to_string(),
        "--table-dir".to_string(),
        discovery.table_dir.clone(),
    ]);
    if allow_large_fixture {
        command.push("--allow-large".to_string());
    }
    let started = Instant::now();
    let output = Command::new(generator).args(&command[1..]).output()?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        return Err(format!(
            "fixture generator failed: command={command:?}, returncode={:?}, elapsed_seconds={elapsed_seconds:.3}, stderr={:.2000}",
            output.status.code(), stderr
        )
        .into());
    }
    let summary: Value = serde_json::from_slice(&fs::read(fixture_dir.join("summary.json"))?)?;
    assert_fixture_summary(&summary, table, discovery)?;
    Ok(json!({
        "status": "ok",
        "fixture_dir": fixture_dir,
        "command": command,
        "returncode": output.status.code(),
        "elapsed_seconds": elapsed_seconds,
        "stdout": stdout,
        "stderr": stderr,
        "summary": summary,
    }))
}

fn assert_fixture_summary(summary: &Value, table: &Table, discovery: &Discovery) -> Result<()> {
    if summary.get("table").and_then(Value::as_str) != Some(&table.name) {
        return Err(format!(
            "fixture table mismatch: {:?} != {}",
            summary.get("table"),
            table.name
        )
        .into());
    }
    if summary.get("database").and_then(Value::as_str) != Some(&table.database) {
        return Err(format!(
            "fixture database mismatch: {:?} != {}",
            summary.get("database"),
            table.database
        )
        .into());
    }
    if summary.get("region_id").and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    }) != Some(discovery.region_id)
    {
        return Err(format!(
            "fixture region_id mismatch: {:?} != {}",
            summary.get("region_id"),
            discovery.region_id
        )
        .into());
    }
    if summary.get("table_dir").and_then(Value::as_str) != Some(&discovery.table_dir) {
        return Err(format!(
            "fixture table_dir mismatch: {:?} != {}",
            summary.get("table_dir"),
            discovery.table_dir
        )
        .into());
    }
    if summary
        .get("region_dir")
        .and_then(Value::as_str)
        .map(|value| value.trim_matches('/'))
        != Some(discovery.region_dir.trim_matches('/'))
    {
        return Err(format!(
            "fixture region_dir mismatch: {:?} != {}",
            summary.get("region_dir"),
            discovery.region_dir
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_regression_runner::model::Column;

    #[test]
    fn creates_exact_direct_sst_table_sql() {
        let table = Table {
            database: "public".to_string(),
            name: "metric\"name".to_string(),
            engine: "mito".to_string(),
            columns: vec![
                Column {
                    name: "host".to_string(),
                    data_type: "STRING".to_string(),
                },
                Column {
                    name: "ts".to_string(),
                    data_type: "TIMESTAMP(9)".to_string(),
                },
            ],
            primary_key: vec!["host".to_string()],
            time_index: Some("ts".to_string()),
            append_mode: Some(true),
            sst_format: Some("flat".to_string()),
            validate_show_create_engine: true,
        };
        assert_eq!(
            create_table_sql(&table).unwrap(),
            "CREATE TABLE \"metric\"\"name\" (\n  \"host\" STRING,\n  \"ts\" TIMESTAMP(9),\n  TIME INDEX (\"ts\"),\n  PRIMARY KEY (\"host\")\n) ENGINE=mito\nWITH ('append_mode'='true', 'sst_format'='flat');"
        );
    }

    #[test]
    fn extracts_named_and_positional_discovery_rows() {
        let rows = extract_rows(&json!({"output": [{"data": [{"TABLE_ID": "7"}]}]}));
        assert_eq!(rows, vec![json!({"TABLE_ID": "7"})]);
        assert_eq!(row_u64(&rows[0], 0, "table_id").unwrap(), 7);
        let rows = extract_rows(&json!({"data": [[30064771073_u64, 0, "addr", "YES", "ALIVE"]]}));
        assert_eq!(rows.len(), 1);
        assert_eq!(row_u64(&rows[0], 0, "region_id").unwrap(), 30_064_771_073);
        assert_eq!(row_u64(&rows[0], 1, "peer_id").unwrap(), 0);
        assert_eq!(
            value_text(row_value(&rows[0], 3, "is_leader").unwrap()),
            "YES"
        );
    }

    #[test]
    fn storage_paths_include_the_table_directory() {
        assert_eq!(
            storage_paths("public", 1024, 0),
            (
                "data/greptime/public/1024/".to_string(),
                "data/greptime/public/1024/1024_0000000000".to_string(),
            )
        );
    }
}
