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
    Client, Database,
};
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::serialized_reader::SerializedFileReader,
};

const CATALOG_NAME: &str = "greptime";
const SCHEMA_NAME: &str = "public";
const TABLE_NAME: &str = "nyc_taxi";

#[derive(Parser)]
#[command(name = "NYC benchmark runner")]
struct Args {
    #[arg(short, long)]
    path: String,

    #[arg(short = 's', long = "batch-size")]
    batch_size: usize,
}

fn get_file_list<P: AsRef<Path>>(path: P) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|dir| dir.unwrap().path().canonicalize().unwrap())
        .collect()
}

async fn write_data(batch_size: usize, db: &mut Database, path: PathBuf) -> u128 {
    let file = std::fs::File::open(path).unwrap();
    let mut record_batch_reader =
        ParquetFileArrowReader::new(Arc::new(SerializedFileReader::new(file).unwrap()))
            .get_record_reader(batch_size)
            .unwrap();

    let mut total_rpc_elapsed_ms = 0;

    let mut cnt = 0;
    while let Some(record_batch) = record_batch_reader.next() {
        println!("{:?}, {}", Instant::now(), cnt);
        let record_batch = record_batch.unwrap();
        cnt += record_batch.num_rows();
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
    }

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
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let file_list = get_file_list(args.path);

    let client = Client::connect("http://127.0.0.1:3001").await.unwrap();
    let admin = Admin::new("admin", client.clone());
    let mut db = Database::new("greptime", client);

    let create_table_result = admin.create(create_table_expr()).await;
    println!("Create table result: {:?}", create_table_result);

    for path in file_list {
        println!("going to write {:?}", path);
        let elapsed_ms = write_data(args.batch_size, &mut db, path).await;
        println!("Finished after {}ms", elapsed_ms);
    }
}
