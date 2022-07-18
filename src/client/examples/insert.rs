use api::v1::*;
use client::{Client, Database};

#[tokio::main]
async fn main() {
    let url = "http://127.0.0.1:3001";
    let db_name = "db";
    let table_name = "demo";

    let client = Client::connect(url).await.unwrap();
    let db = Database::new(db_name, client);
    db.insert(table_name, insert_batches()).await.unwrap();
}

fn insert_batches() -> Vec<Vec<u8>> {
    const SEMANTIC_TAG: i32 = 0;
    const SEMANTIC_FEILD: i32 = 1;
    const SEMANTIC_TS: i32 = 2;

    let row_count = 4;

    let mut host_vals = column::Values::default();
    host_vals.string_values = vec!["host1".to_string(), "host2".to_string(), "host3".to_string(), "host4".to_string()];
    let host_null_mask = vec![0];
    let host_column = Column {
        column_name: "host".to_string(),
        semantic_type: SEMANTIC_TAG,
        values: Some(host_vals),
        null_mask: host_null_mask.clone(),
    };

    let mut cpu_vals = column::Values::default();
    cpu_vals.f64_values = vec![0.31, 0.41, 0.2];
    let cpu_null_mask = vec![2];
    let cpu_column = Column {
        column_name: "cpu".to_string(),
        semantic_type: SEMANTIC_FEILD,
        values: Some(cpu_vals),
        null_mask: cpu_null_mask.clone(),
    };

    let mut mem_vals = column::Values::default();
    mem_vals.f64_values = vec![0.1, 0.2, 0.3];
    let mem_null_mask = vec![4];
    let mem_column = Column {
        column_name: "memory".to_string(),
        semantic_type: SEMANTIC_FEILD,
        values: Some(mem_vals),
        null_mask: mem_null_mask.clone(),
    };

    let mut ts_vals = column::Values::default();
    ts_vals.i64_values = vec![100, 101, 102, 103];
    let ts_null_mask = vec![0];
    let ts_column = Column {
        column_name: "ts".to_string(),
        semantic_type: SEMANTIC_TS,
        values: Some(ts_vals),
        null_mask: ts_null_mask,
    };
    
    let insert_batch = InsertBatch {
        columns: vec![host_column, cpu_column, mem_column, ts_column],
        row_count,
    };
    vec![insert_batch.clone().into()]
}
