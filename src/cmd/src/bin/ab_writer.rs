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

//! Fast gRPC row writer for the query runtime-size A/B test.

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema, CreateTableExpr, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType, Value,
};
use clap::Parser;
use client::{Client, Database};

const ROW_COUNT: usize = 100_000;
const BATCH_SIZE: usize = 5_000;
const PAYLOAD_LEN: usize = 200;

#[derive(Debug, Parser)]
#[command(about = "Write the query_rt_size A/B test fixture over gRPC")]
struct Args {
    /// Frontend gRPC endpoint.
    #[arg(long, default_value = "127.0.0.1:25799")]
    endpoint: String,

    #[arg(long, default_value = "t")]
    table: String,
}

fn schema() -> Vec<ColumnSchema> {
    vec![
        ColumnSchema {
            column_name: "a".to_string(),
            datatype: ColumnDataType::Int32 as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        },
        ColumnSchema {
            column_name: "v".to_string(),
            datatype: ColumnDataType::Int64 as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        },
        ColumnSchema {
            column_name: "ts".to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        },
        ColumnSchema {
            column_name: "payload".to_string(),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        },
    ]
}

fn row(n: usize, base_ts_ms: i64) -> Row {
    Row {
        values: vec![
            Value {
                value_data: Some(ValueData::I32Value((n % 100) as i32)),
            },
            Value {
                value_data: Some(ValueData::I64Value((n as i64) * 10)),
            },
            Value {
                value_data: Some(ValueData::TimestampMillisecondValue(
                    base_ts_ms + (n as i64) * 1_000,
                )),
            },
            Value {
                value_data: Some(ValueData::StringValue("x".repeat(PAYLOAD_LEN))),
            },
        ],
    }
}

async fn write(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::with_urls([args.endpoint]);
    let db = Database::new("greptime", "public", client);

    let base_ts_ms = 1_700_000_000_000i64;
    let columns = schema();
    let create = CreateTableExpr {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: args.table.clone(),
        column_defs: columns
            .iter()
            .map(|column| ColumnDef {
                name: column.column_name.clone(),
                data_type: column.datatype,
                is_nullable: false,
                semantic_type: column.semantic_type,
                ..Default::default()
            })
            .collect(),
        time_index: "ts".to_string(),
        primary_keys: vec!["a".to_string()],
        create_if_not_exists: true,
        engine: "mito".to_string(),
        ..Default::default()
    };
    db.create(create).await?;
    let started = std::time::Instant::now();
    let mut written = 0u64;

    for start in (0..ROW_COUNT).step_by(BATCH_SIZE) {
        let end = (start + BATCH_SIZE).min(ROW_COUNT);
        let rows = (start..end).map(|n| row(n, base_ts_ms)).collect();
        let request = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: args.table.clone(),
                rows: Some(Rows {
                    schema: columns.clone(),
                    rows,
                }),
            }],
        };
        written += db.row_inserts(request).await? as u64;
        eprintln!("batch {}-{}: {} rows", start, end, end - start);
    }

    let elapsed = started.elapsed();
    println!(
        "wrote {written} rows in {:.3}s ({:.0} rows/s)",
        elapsed.as_secs_f64(),
        written as f64 / elapsed.as_secs_f64()
    );
    if written != ROW_COUNT as u64 {
        return Err(format!("server reported {written} rows, expected {ROW_COUNT}").into());
    }
    println!("row count verification: {written}");
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = write(Args::parse()).await {
        eprintln!("ab_writer: {err}");
        std::process::exit(1);
    }
}
