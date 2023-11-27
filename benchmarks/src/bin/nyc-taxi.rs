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

//! Use the taxi trip records from New York City dataset to bench. You can download the dataset from
//! [here](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use arrow::array::{ArrayRef, PrimitiveArray, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use client::api::v1::column::Values;
use client::api::v1::{
    Column, ColumnDataType, ColumnDef, CreateTableExpr, InsertRequest, InsertRequests, SemanticType,
};
use client::{Client, Database, Output, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use futures_util::TryStreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::task::JoinSet;

const CATALOG_NAME: &str = "greptime";
const SCHEMA_NAME: &str = "public";

#[derive(Parser)]
#[command(name = "NYC benchmark runner")]
struct Args {
    /// Path to the dataset
    #[arg(short, long)]
    path: Option<String>,

    /// Batch size of insert request.
    #[arg(short = 's', long = "batch-size", default_value_t = 4096)]
    batch_size: usize,

    /// Number of client threads on write (parallel on file level)
    #[arg(short = 't', long = "thread-num", default_value_t = 4)]
    thread_num: usize,

    /// Number of query iteration
    #[arg(short = 'i', long = "iter-num", default_value_t = 3)]
    iter_num: usize,

    #[arg(long = "skip-write")]
    skip_write: bool,

    #[arg(long = "skip-read")]
    skip_read: bool,

    #[arg(short, long, default_value_t = String::from("127.0.0.1:4001"))]
    endpoint: String,
}

fn get_file_list<P: AsRef<Path>>(path: P) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|dir| dir.unwrap().path().canonicalize().unwrap())
        .collect()
}

fn new_table_name() -> String {
    format!("nyc_taxi_{}", chrono::Utc::now().timestamp())
}

async fn write_data(
    table_name: &str,
    batch_size: usize,
    db: &Database,
    path: PathBuf,
    mpb: MultiProgress,
    pb_style: ProgressStyle,
) -> u128 {
    let file = std::fs::File::open(&path).unwrap();
    let record_batch_reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let row_num = record_batch_reader_builder
        .metadata()
        .file_metadata()
        .num_rows();
    let record_batch_reader = record_batch_reader_builder
        .with_batch_size(batch_size)
        .build()
        .unwrap();
    let progress_bar = mpb.add(ProgressBar::new(row_num as _));
    progress_bar.set_style(pb_style);
    progress_bar.set_message(format!("{path:?}"));

    let mut total_rpc_elapsed_ms = 0;

    for record_batch in record_batch_reader {
        let record_batch = record_batch.unwrap();
        if !is_record_batch_full(&record_batch) {
            continue;
        }
        let (columns, row_count) = convert_record_batch(record_batch);
        let request = InsertRequest {
            table_name: table_name.to_string(),
            columns,
            row_count,
        };
        let requests = InsertRequests {
            inserts: vec![request],
        };

        let now = Instant::now();
        db.insert(requests).await.unwrap();
        let elapsed = now.elapsed();
        total_rpc_elapsed_ms += elapsed.as_millis();
        progress_bar.inc(row_count as _);
    }

    progress_bar.finish_with_message(format!("file {path:?} done in {total_rpc_elapsed_ms}ms",));
    total_rpc_elapsed_ms
}

fn convert_record_batch(record_batch: RecordBatch) -> (Vec<Column>, u32) {
    let schema = record_batch.schema();
    let fields = schema.fields();
    let row_count = record_batch.num_rows();
    let mut columns = vec![];

    for (array, field) in record_batch.columns().iter().zip(fields.iter()) {
        let (values, datatype) = build_values(array);
        let semantic_type = match field.name().as_str() {
            "VendorID" => SemanticType::Tag,
            "tpep_pickup_datetime" => SemanticType::Timestamp,
            _ => SemanticType::Field,
        };

        let column = Column {
            column_name: field.name().clone(),
            values: Some(values),
            null_mask: array
                .to_data()
                .nulls()
                .map(|bitmap| bitmap.buffer().as_slice().to_vec())
                .unwrap_or_default(),
            datatype: datatype.into(),
            semantic_type: semantic_type as i32,
            ..Default::default()
        };
        columns.push(column);
    }

    (columns, row_count as _)
}

fn build_values(column: &ArrayRef) -> (Values, ColumnDataType) {
    match column.data_type() {
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .unwrap();
            let values = array.values();
            (
                Values {
                    i64_values: values.to_vec(),
                    ..Default::default()
                },
                ColumnDataType::Int64,
            )
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()
                .unwrap();
            let values = array.values();
            (
                Values {
                    f64_values: values.to_vec(),
                    ..Default::default()
                },
                ColumnDataType::Float64,
            )
        }
        DataType::Timestamp(_, _) => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let values = array.values();
            (
                Values {
                    timestamp_microsecond_values: values.to_vec(),
                    ..Default::default()
                },
                ColumnDataType::TimestampMicrosecond,
            )
        }
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            let values = array.iter().filter_map(|s| s.map(String::from)).collect();
            (
                Values {
                    string_values: values,
                    ..Default::default()
                },
                ColumnDataType::String,
            )
        }
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::LargeUtf8
        | DataType::List(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::RunEndEncoded(_, _)
        | DataType::Map(_, _) => todo!(),
    }
}

fn is_record_batch_full(batch: &RecordBatch) -> bool {
    batch.columns().iter().all(|col| col.null_count() == 0)
}

fn create_table_expr(table_name: &str) -> CreateTableExpr {
    CreateTableExpr {
        catalog_name: CATALOG_NAME.to_string(),
        schema_name: SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        desc: "".to_string(),
        column_defs: vec![
            ColumnDef {
                name: "VendorID".to_string(),
                data_type: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Tag as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "tpep_pickup_datetime".to_string(),
                data_type: ColumnDataType::TimestampMicrosecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "tpep_dropoff_datetime".to_string(),
                data_type: ColumnDataType::TimestampMicrosecond as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "passenger_count".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "trip_distance".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "RatecodeID".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "store_and_fwd_flag".to_string(),
                data_type: ColumnDataType::String as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "PULocationID".to_string(),
                data_type: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "DOLocationID".to_string(),
                data_type: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "payment_type".to_string(),
                data_type: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "fare_amount".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "extra".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "mta_tax".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "tip_amount".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "tolls_amount".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "improvement_surcharge".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "total_amount".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "congestion_surcharge".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "airport_fee".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
        ],
        time_index: "tpep_pickup_datetime".to_string(),
        primary_keys: vec!["VendorID".to_string()],
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: None,
        engine: "mito".to_string(),
    }
}

fn query_set(table_name: &str) -> HashMap<String, String> {
    HashMap::from([
        (
            "count_all".to_string(), 
            format!("SELECT COUNT(*) FROM {table_name};"),
        ),
        (
            "fare_amt_by_passenger".to_string(),
            format!("SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM {table_name} GROUP BY passenger_count"),
        )
    ])
}

async fn do_write(args: &Args, db: &Database, table_name: &str) {
    let mut file_list = get_file_list(args.path.clone().expect("Specify data path in argument"));
    let mut write_jobs = JoinSet::new();

    let create_table_result = db.create(create_table_expr(table_name)).await;
    println!("Create table result: {create_table_result:?}");

    let progress_bar_style = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:60.cyan/blue} {pos:>7}/{len:7} {msg}",
    )
    .unwrap()
    .progress_chars("##-");
    let multi_progress_bar = MultiProgress::new();
    let file_progress = multi_progress_bar.add(ProgressBar::new(file_list.len() as _));
    file_progress.inc(0);

    let batch_size = args.batch_size;
    for _ in 0..args.thread_num {
        if let Some(path) = file_list.pop() {
            let db = db.clone();
            let mpb = multi_progress_bar.clone();
            let pb_style = progress_bar_style.clone();
            let table_name = table_name.to_string();
            let _ = write_jobs.spawn(async move {
                write_data(&table_name, batch_size, &db, path, mpb, pb_style).await
            });
        }
    }
    while write_jobs.join_next().await.is_some() {
        file_progress.inc(1);
        if let Some(path) = file_list.pop() {
            let db = db.clone();
            let mpb = multi_progress_bar.clone();
            let pb_style = progress_bar_style.clone();
            let table_name = table_name.to_string();
            let _ = write_jobs.spawn(async move {
                write_data(&table_name, batch_size, &db, path, mpb, pb_style).await
            });
        }
    }
}

async fn do_query(num_iter: usize, db: &Database, table_name: &str) {
    for (query_name, query) in query_set(table_name) {
        println!("Running query: {query}");
        for i in 0..num_iter {
            let now = Instant::now();
            let res = db.sql(&query).await.unwrap();
            match res {
                Output::AffectedRows(_) | Output::RecordBatches(_) => (),
                Output::Stream(stream) => {
                    stream.try_collect::<Vec<_>>().await.unwrap();
                }
            }
            let elapsed = now.elapsed();
            println!(
                "query {}, iteration {}: {}ms",
                query_name,
                i,
                elapsed.as_millis(),
            );
        }
    }
}

fn main() {
    let args = Args::parse();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.thread_num)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let client = Client::with_urls(vec![&args.endpoint]);
            let db = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
            let table_name = new_table_name();

            if !args.skip_write {
                do_write(&args, &db, &table_name).await;
            }

            if !args.skip_read {
                do_query(args.iter_num, &db, &table_name).await;
            }
        })
}
