// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use api::v1::*;
use client::{Client, Database};
use common_grpc::InsertBatch;

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let client = Client::with_urls(vec!["127.0.0.1:3001"]);
    let db = Database::new("greptime", client);

    let InsertBatch { columns, row_count } = insert_batches();

    let expr = InsertExpr {
        schema_name: "public".to_string(),
        table_name: "demo".to_string(),
        options: HashMap::default(),
        region_number: 0,
        columns,
        row_count,
    };
    db.insert(expr).await.unwrap();
}

fn insert_batches() -> InsertBatch {
    const SEMANTIC_TAG: i32 = 0;
    const SEMANTIC_FIELD: i32 = 1;
    const SEMANTIC_TS: i32 = 2;

    let row_count = 4;

    let host_vals = column::Values {
        string_values: vec![
            "host1".to_string(),
            "host2".to_string(),
            "host3".to_string(),
            "host4".to_string(),
        ],
        ..Default::default()
    };
    let host_column = Column {
        column_name: "host".to_string(),
        semantic_type: SEMANTIC_TAG,
        values: Some(host_vals),
        null_mask: vec![0],
        ..Default::default()
    };

    let cpu_vals = column::Values {
        f64_values: vec![0.31, 0.41, 0.2],
        ..Default::default()
    };
    let cpu_column = Column {
        column_name: "cpu".to_string(),
        semantic_type: SEMANTIC_FIELD,
        values: Some(cpu_vals),
        null_mask: vec![2],
        ..Default::default()
    };

    let mem_vals = column::Values {
        f64_values: vec![0.1, 0.2, 0.3],
        ..Default::default()
    };
    let mem_column = Column {
        column_name: "memory".to_string(),
        semantic_type: SEMANTIC_FIELD,
        values: Some(mem_vals),
        null_mask: vec![4],
        ..Default::default()
    };

    let ts_vals = column::Values {
        i64_values: vec![100, 101, 102, 103],
        ..Default::default()
    };
    let ts_column = Column {
        column_name: "ts".to_string(),
        semantic_type: SEMANTIC_TS,
        values: Some(ts_vals),
        null_mask: vec![0],
        ..Default::default()
    };

    InsertBatch {
        columns: vec![host_column, cpu_column, mem_column, ts_column],
        row_count,
    }
}
