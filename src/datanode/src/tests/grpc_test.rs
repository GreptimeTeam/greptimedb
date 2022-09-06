use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use api::v1::{
    admin_result, codec::InsertBatch, column, Column, ColumnDef, CreateExpr, MutateResult,
};
use client::admin::Admin;
use client::{Client, Database, ObjectResult};
use servers::grpc::GrpcServer;
use servers::server::Server;

use crate::instance::Instance;
use crate::tests::test_util;

#[tokio::test]
async fn test_insert_and_select() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
    let instance = Arc::new(Instance::new(&opts).await.unwrap());
    instance.start().await.unwrap();

    tokio::spawn(async move {
        let mut grpc_server = GrpcServer::new(instance.clone(), instance);
        let addr = "127.0.0.1:3001".parse::<SocketAddr>().unwrap();
        grpc_server.start(addr).await.unwrap()
    });

    // wait for GRPC server to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let grpc_client = Client::connect("http://127.0.0.1:3001").await.unwrap();
    let db = Database::new("greptime", grpc_client.clone());
    let admin = Admin::new("greptime", grpc_client);

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

    // create
    let expr = testing_create_expr();
    let result = admin.create(expr).await.unwrap();
    assert_matches!(
        result.result,
        Some(admin_result::Result::Mutate(MutateResult {
            success: 1,
            failure: 0
        }))
    );

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
    let result = db
        .select(client::Select::Sql("select * from demo".to_string()))
        .await
        .unwrap();
    assert!(matches!(result, ObjectResult::Select(_)));
    match result {
        ObjectResult::Select(select_result) => {
            assert_eq!(4, select_result.row_count);
            let actual_columns = select_result.columns;
            assert_eq!(4, actual_columns.len());

            // Respect the order in create table schema
            let expected_columns = vec![
                expected_host_col,
                expected_cpu_col,
                expected_mem_col,
                expected_ts_col,
            ];
            expected_columns
                .iter()
                .zip(actual_columns.iter())
                .for_each(|(x, y)| assert_eq!(x, y));
        }
        _ => unreachable!(),
    }
}

fn testing_create_expr() -> CreateExpr {
    let column_defs = vec![
        ColumnDef {
            name: "host".to_string(),
            data_type: 12, // string
            is_nullable: false,
        },
        ColumnDef {
            name: "cpu".to_string(),
            data_type: 10, // float64
            is_nullable: true,
        },
        ColumnDef {
            name: "memory".to_string(),
            data_type: 10, // float64
            is_nullable: true,
        },
        ColumnDef {
            name: "ts".to_string(),
            data_type: 4, // int64
            is_nullable: true,
        },
    ];
    CreateExpr {
        catalog_name: None,
        schema_name: None,
        table_name: "demo".to_string(),
        desc: Some("blabla".to_string()),
        column_defs,
        time_index: "ts".to_string(),
        primary_keys: vec!["ts".to_string(), "host".to_string()],
        create_if_not_exists: true,
        table_options: HashMap::new(),
    }
}
