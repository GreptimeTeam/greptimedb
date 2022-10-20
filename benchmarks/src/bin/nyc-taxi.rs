//! Use the taxi trip records from New York City dataset to bench. You can download the dataset from
//! [here](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

#![feature(once_cell)]
#![allow(clippy::print_stdout)]

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{ArrayRef, PrimitiveArray, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Float64Type, Int64Type},
    record_batch::RecordBatch,
};
use clap::Parser;
use client::{
    admin::Admin,
    api::v1::{
        codec::InsertBatch, column::Values, insert_expr, Column, ColumnDataType, ColumnDef,
        CreateExpr, InsertExpr,
    },
    Client, Database, Select,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
};
use tokio::task::JoinSet;

const DATABASE_NAME: &str = "greptime";
const CATALOG_NAME: &str = "greptime";
const SCHEMA_NAME: &str = "public";
const TABLE_NAME: &str = "nyc_taxi";

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

    #[arg(short, long, default_value_t = String::from("127.0.0.1:3001"))]
    endpoint: String,
}

fn get_file_list<P: AsRef<Path>>(path: P) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|dir| dir.unwrap().path().canonicalize().unwrap())
        .collect()
}

async fn write_data(
    batch_size: usize,
    db: &Database,
    path: PathBuf,
    mpb: MultiProgress,
    pb_style: ProgressStyle,
) -> u128 {
    let file = std::fs::File::open(&path).unwrap();
    let file_reader = Arc::new(SerializedFileReader::new(file).unwrap());
    let row_num = file_reader.metadata().file_metadata().num_rows();
    let record_batch_reader = ParquetFileArrowReader::new(file_reader)
        .get_record_reader(batch_size)
        .unwrap();
    let progress_bar = mpb.add(ProgressBar::new(row_num as _));
    progress_bar.set_style(pb_style);
    progress_bar.set_message(format!("{:?}", path));

    let mut total_rpc_elapsed_ms = 0;

    for record_batch in record_batch_reader {
        let record_batch = record_batch.unwrap();
        let row_count = record_batch.num_rows();
        let insert_batch = convert_record_batch(record_batch).into();
        let insert_expr = InsertExpr {
            table_name: TABLE_NAME.to_string(),
            expr: Some(insert_expr::Expr::Values(insert_expr::Values {
                values: vec![insert_batch],
            })),
            options: HashMap::default(),
        };
        let now = Instant::now();
        db.insert(insert_expr).await.unwrap();
        let elapsed = now.elapsed();
        total_rpc_elapsed_ms += elapsed.as_millis();
        progress_bar.inc(row_count as _);
    }

    progress_bar.finish_with_message(format!(
        "file {:?} done in {}ms",
        path, total_rpc_elapsed_ms
    ));
    total_rpc_elapsed_ms
}

fn convert_record_batch(record_batch: RecordBatch) -> InsertBatch {
    let schema = record_batch.schema();
    let fields = schema.fields();
    let row_count = record_batch.num_rows();
    let mut columns = vec![];

    for (array, field) in record_batch.columns().iter().zip(fields.iter()) {
        let values = build_values(array);
        let column = Column {
            column_name: field.name().to_owned(),
            values: Some(values),
            null_mask: vec![],
            // datatype and semantic_type are set to default
            ..Default::default()
        };
        columns.push(column);
    }

    InsertBatch {
        columns,
        row_count: row_count as _,
    }
}

fn build_values(column: &ArrayRef) -> Values {
    match column.data_type() {
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .unwrap();
            let values = array.values();
            Values {
                i64_values: values.to_vec(),
                ..Default::default()
            }
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()
                .unwrap();
            let values = array.values();
            Values {
                f64_values: values.to_vec(),
                ..Default::default()
            }
        }
        DataType::Timestamp(_, _) => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let values = array.values();
            Values {
                i64_values: values.to_vec(),
                ..Default::default()
            }
        }
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            let values = array.iter().filter_map(|s| s.map(String::from)).collect();
            Values {
                string_values: values,
                ..Default::default()
            }
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
        | DataType::Decimal(_, _)
        | DataType::Map(_, _) => todo!(),
    }
}

fn create_table_expr() -> CreateExpr {
    CreateExpr {
        catalog_name: Some(CATALOG_NAME.to_string()),
        schema_name: Some(SCHEMA_NAME.to_string()),
        table_name: TABLE_NAME.to_string(),
        desc: None,
        column_defs: vec![
            ColumnDef {
                name: "VendorID".to_string(),
                datatype: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "tpep_pickup_datetime".to_string(),
                datatype: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "tpep_dropoff_datetime".to_string(),
                datatype: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "passenger_count".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "trip_distance".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "RatecodeID".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "store_and_fwd_flag".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "PULocationID".to_string(),
                datatype: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "DOLocationID".to_string(),
                datatype: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "payment_type".to_string(),
                datatype: ColumnDataType::Int64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "fare_amount".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "extra".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "mta_tax".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "tip_amount".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "tolls_amount".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "improvement_surcharge".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "total_amount".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "congestion_surcharge".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "airport_fee".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
        ],
        time_index: "tpep_pickup_datetime".to_string(),
        primary_keys: vec!["VendorID".to_string()],
        create_if_not_exists: false,
        table_options: Default::default(),
        region_ids: vec![0],
        table_id: Some(0),
    }
}

fn query_set() -> HashMap<String, String> {
    let mut ret = HashMap::new();

    ret.insert(
        "count_all".to_string(),
        format!("SELECT COUNT(*) FROM {};", TABLE_NAME),
    );

    ret.insert(
        "fare_amt_by_passenger".to_string(),
        format!("SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM {} GROUP BY passenger_count",TABLE_NAME)
    );

    ret
}

async fn do_write(args: &Args, client: &Client) {
    let admin = Admin::new("admin", client.clone());

    let mut file_list = get_file_list(args.path.clone().expect("Specify data path in argument"));
    let mut write_jobs = JoinSet::new();

    let create_table_result = admin.create(create_table_expr()).await;
    println!("Create table result: {:?}", create_table_result);

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
            let db = Database::new(DATABASE_NAME, client.clone());
            let mpb = multi_progress_bar.clone();
            let pb_style = progress_bar_style.clone();
            write_jobs.spawn(async move { write_data(batch_size, &db, path, mpb, pb_style).await });
        }
    }
    while write_jobs.join_next().await.is_some() {
        file_progress.inc(1);
        if let Some(path) = file_list.pop() {
            let db = Database::new(DATABASE_NAME, client.clone());
            let mpb = multi_progress_bar.clone();
            let pb_style = progress_bar_style.clone();
            write_jobs.spawn(async move { write_data(batch_size, &db, path, mpb, pb_style).await });
        }
    }
}

async fn do_query(num_iter: usize, db: &Database) {
    for (query_name, query) in query_set() {
        println!("Running query: {}", query);
        for i in 0..num_iter {
            let now = Instant::now();
            let _res = db.select(Select::Sql(query.clone())).await.unwrap();
            let elapsed = now.elapsed();
            println!(
                "query {}, iteration {}: {}ms",
                query_name,
                i,
                elapsed.as_millis()
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

            if !args.skip_write {
                do_write(&args, &client).await;
            }

            if !args.skip_read {
                let db = Database::new(DATABASE_NAME, client.clone());
                do_query(args.iter_num, &db).await;
            }
        })
}
