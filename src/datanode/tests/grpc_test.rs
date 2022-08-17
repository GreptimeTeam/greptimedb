mod test_util;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use api::v1::{codec::InsertBatch, column, select_expr, Column, SelectExpr};
use client::{Client, Database, ObjectResult};
use datanode::instance::Instance;
use servers::grpc::GrpcServer;
use servers::server::Server;

#[tokio::test]
async fn test_insert_and_select() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
    let instance = Arc::new(Instance::new(&opts).await.unwrap());
    instance.start().await.unwrap();

    test_util::create_test_table(&instance).await.unwrap();

    tokio::spawn(async move {
        let mut grpc_server = GrpcServer::new(instance);
        let addr = "127.0.0.1:3001".parse::<SocketAddr>().unwrap();
        grpc_server.start(addr).await.unwrap()
    });

    // wait for GRPC server to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let grpc_client = Client::connect("http://127.0.0.1:3001").await.unwrap();
    let db = Database::new("greptime", grpc_client);

    // testing data:
    let expected_host_col = Column {
        column_name: "host".to_string(),
        values: Some(column::Values {
            string_values: vec!["host1", "host2", "host3", "host4"]
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let expected_cpu_col = Column {
        column_name: "cpu".to_string(),
        values: Some(column::Values {
            f64_values: vec![0.31, 0.41, 0.2],
            ..Default::default()
        }),
        null_mask: vec![2],
        ..Default::default()
    };
    let expected_mem_col = Column {
        column_name: "memory".to_string(),
        values: Some(column::Values {
            f64_values: vec![0.1, 0.2, 0.3],
            ..Default::default()
        }),
        null_mask: vec![4],
        ..Default::default()
    };
    let expected_ts_col = Column {
        column_name: "ts".to_string(),
        values: Some(column::Values {
            i64_values: vec![100, 101, 102, 103],
            ..Default::default()
        }),
        ..Default::default()
    };

    // insert
    let values = vec![InsertBatch {
        columns: vec![
            expected_host_col.clone(),
            expected_cpu_col.clone(),
            expected_mem_col.clone(),
            expected_ts_col.clone(),
        ],
        row_count: 4,
    }
    .into()];
    let result = db.insert("demo", values).await;
    assert!(result.is_ok());

    // select
    let select_expr = SelectExpr {
        expr: Some(select_expr::Expr::Sql("select * from demo".to_string())),
    };
    let result = db.select(select_expr).await.unwrap();
    assert!(matches!(result, ObjectResult::Select(_)));
    match result {
        ObjectResult::Select(select_result) => {
            assert_eq!(4, select_result.row_count);
            let actual_columns = select_result.columns;
            assert_eq!(4, actual_columns.len());

            let expected_columns = vec![
                expected_ts_col,
                expected_host_col,
                expected_cpu_col,
                expected_mem_col,
            ];
            expected_columns
                .iter()
                .zip(actual_columns.iter())
                .for_each(|(x, y)| assert_eq!(x, y));
        }
        _ => unreachable!(),
    }
}
