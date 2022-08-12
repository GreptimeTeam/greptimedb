use api::v1::{codec::InsertBatch, *};
use client::{Client, Database};

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let client = Client::connect("http://127.0.0.1:3001").await.unwrap();
    let db = Database::new("greptime", client);

    db.insert("demo", insert_batches()).await.unwrap();
}

fn insert_batches() -> Vec<Vec<u8>> {
    const SEMANTIC_TAG: i32 = 0;
    const SEMANTIC_FEILD: i32 = 1;
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
    };

    let cpu_vals = column::Values {
        f64_values: vec![0.31, 0.41, 0.2],
        ..Default::default()
    };
    let cpu_column = Column {
        column_name: "cpu".to_string(),
        semantic_type: SEMANTIC_FEILD,
        values: Some(cpu_vals),
        null_mask: vec![2],
    };

    let mem_vals = column::Values {
        f64_values: vec![0.1, 0.2, 0.3],
        ..Default::default()
    };
    let mem_column = Column {
        column_name: "memory".to_string(),
        semantic_type: SEMANTIC_FEILD,
        values: Some(mem_vals),
        null_mask: vec![4],
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
    };

    let insert_batch = InsertBatch {
        columns: vec![host_column, cpu_column, mem_column, ts_column],
        row_count,
    };
    vec![insert_batch.into()]
}
