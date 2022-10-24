use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use api::v1::ColumnDataType;
use api::v1::{
    admin_result, alter_expr::Kind, codec::InsertBatch, column, column::SemanticType, insert_expr,
    AddColumn, AlterExpr, Column, ColumnDef, CreateExpr, InsertExpr, MutateResult,
};
use client::admin::Admin;
use client::{Client, Database, ObjectResult};
use servers::grpc::GrpcServer;
use servers::server::Server;

use crate::instance::Instance;
use crate::tests::test_util;

async fn setup_grpc_server(port: usize) -> String {
    common_telemetry::init_default_ut_logging();

    let (mut opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
    let addr = format!("127.0.0.1:{}", port);
    opts.rpc_addr = addr.clone();
    let instance = Arc::new(Instance::new(&opts).await.unwrap());
    instance.start().await.unwrap();

    let addr_cloned = addr.clone();
    tokio::spawn(async move {
        let mut grpc_server = GrpcServer::new(instance.clone(), instance);
        let addr = addr_cloned.parse::<SocketAddr>().unwrap();
        grpc_server.start(addr).await.unwrap()
    });

    // wait for GRPC server to start
    tokio::time::sleep(Duration::from_secs(1)).await;
    addr
}

#[tokio::test]
async fn test_auto_create_table() {
    let addr = setup_grpc_server(3991).await;

    let mut db = Database::new("greptime", Client::new());
    db.start(vec![addr]);

    insert_and_assert(&db).await;
}

fn expect_data() -> (Column, Column, Column, Column) {
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
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::String as i32,
        ..Default::default()
    };
    let expected_cpu_col = Column {
        column_name: "cpu".to_string(),
        values: Some(column::Values {
            f64_values: vec![0.31, 0.41, 0.2],
            ..Default::default()
        }),
        null_mask: vec![2],
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
    };
    let expected_mem_col = Column {
        column_name: "memory".to_string(),
        values: Some(column::Values {
            f64_values: vec![0.1, 0.2, 0.3],
            ..Default::default()
        }),
        null_mask: vec![4],
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
    };
    let expected_ts_col = Column {
        column_name: "ts".to_string(),
        values: Some(column::Values {
            ts_millis_values: vec![100, 101, 102, 103],
            ..Default::default()
        }),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: ColumnDataType::Timestamp as i32,
        ..Default::default()
    };

    (
        expected_host_col,
        expected_cpu_col,
        expected_mem_col,
        expected_ts_col,
    )
}

#[tokio::test]
async fn test_insert_and_select() {
    let client = Client::new();
    let addr = setup_grpc_server(3990).await;
    client.start(vec![addr]);
    let db = Database::new("greptime", client.clone());
    let admin = Admin::new("greptime", client);

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

    //alter
    let add_column = ColumnDef {
        name: "test_column".to_string(),
        datatype: ColumnDataType::Int64.into(),
        is_nullable: true,
        default_constraint: None,
    };
    let kind = Kind::AddColumn(AddColumn {
        column_def: Some(add_column),
    });
    let expr = AlterExpr {
        table_name: "test_table".to_string(),
        catalog_name: None,
        schema_name: None,
        kind: Some(kind),
    };
    let result = admin.alter(expr).await.unwrap();
    assert_eq!(result.result, None);

    // insert
    insert_and_assert(&db).await;
}

async fn insert_and_assert(db: &Database) {
    // testing data:
    let (expected_host_col, expected_cpu_col, expected_mem_col, expected_ts_col) = expect_data();

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
    let expr = InsertExpr {
        table_name: "demo".to_string(),
        expr: Some(insert_expr::Expr::Values(insert_expr::Values { values })),
        options: HashMap::default(),
    };
    let result = db.insert(expr).await;
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
            datatype: ColumnDataType::String as i32,
            is_nullable: false,
            default_constraint: None,
        },
        ColumnDef {
            name: "cpu".to_string(),
            datatype: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: None,
        },
        ColumnDef {
            name: "memory".to_string(),
            datatype: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: None,
        },
        ColumnDef {
            name: "ts".to_string(),
            datatype: 15, // timestamp
            is_nullable: true,
            default_constraint: None,
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
