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
use api::v1::alter_expr::Kind;
use api::v1::column::SemanticType;
use api::v1::{
    admin_result, column, AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, ColumnDef,
    CreateTableExpr, InsertExpr, MutateResult, TableId,
};
use client::admin::Admin;
use client::{Client, Database, ObjectResult};
use common_catalog::consts::MIN_USER_TABLE_ID;
use common_grpc::flight::{flight_messages_to_recordbatches, FlightMessage};
use servers::server::Server;
use tests_integration::test_util::{setup_grpc_server, StorageType};

#[macro_export]
macro_rules! grpc_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<integration_grpc_ $service:lower _test>] {
                $(
                    #[tokio::test(flavor = "multi_thread")]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            let _ = $crate::grpc::$test(store_type).await;
                        }

                    }
                )*
            }
        }
    };
}

#[macro_export]
macro_rules! grpc_tests {
    ($($service:ident),*) => {
        $(
            grpc_test!(
                $service,

                test_auto_create_table,
                test_insert_and_select,
            );
        )*
    };
}

pub async fn test_auto_create_table(store_type: StorageType) {
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "auto_create_table").await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new("greptime", grpc_client);
    insert_and_assert(&db).await;
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
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
            ts_millisecond_values: vec![100, 101, 102, 103],
            ..Default::default()
        }),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: ColumnDataType::TimestampMillisecond as i32,
        ..Default::default()
    };

    (
        expected_host_col,
        expected_cpu_col,
        expected_mem_col,
        expected_ts_col,
    )
}

pub async fn test_insert_and_select(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "insert_and_select").await;

    let grpc_client = Client::with_urls(vec![addr]);

    let db = Database::new("greptime", grpc_client.clone());
    let admin = Admin::new("greptime", grpc_client);

    // create
    let expr = testing_create_expr();
    let result = admin.create(expr).await.unwrap();
    assert!(matches!(
        result.result,
        Some(admin_result::Result::Mutate(MutateResult {
            success: 1,
            failure: 0
        }))
    ));

    //alter
    let add_column = ColumnDef {
        name: "test_column".to_string(),
        datatype: ColumnDataType::Int64.into(),
        is_nullable: true,
        default_constraint: vec![],
    };
    let kind = Kind::AddColumns(AddColumns {
        add_columns: vec![AddColumn {
            column_def: Some(add_column),
            is_key: false,
        }],
    });
    let expr = AlterExpr {
        table_name: "test_table".to_string(),
        catalog_name: "".to_string(),
        schema_name: "".to_string(),
        kind: Some(kind),
    };
    let result = admin.alter(expr).await.unwrap();
    assert!(result.result.is_none());

    // insert
    insert_and_assert(&db).await;

    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

async fn insert_and_assert(db: &Database) {
    // testing data:
    let (expected_host_col, expected_cpu_col, expected_mem_col, expected_ts_col) = expect_data();

    let expr = InsertExpr {
        schema_name: "public".to_string(),
        table_name: "demo".to_string(),
        region_number: 0,
        columns: vec![
            expected_host_col.clone(),
            expected_cpu_col.clone(),
            expected_mem_col.clone(),
            expected_ts_col.clone(),
        ],
        row_count: 4,
    };
    let result = db.insert(expr).await;
    result.unwrap();

    let result = db
        .sql(
            "INSERT INTO demo(host, cpu, memory, ts) VALUES \
            ('host5', 66.6, 1024, 1672201027000),\
            ('host6', 88.8, 333.3, 1672201028000)",
        )
        .await
        .unwrap();
    assert!(matches!(result, ObjectResult::FlightData(_)));
    let ObjectResult::FlightData(mut messages) = result else { unreachable!() };
    assert_eq!(messages.len(), 1);
    assert!(matches!(messages.remove(0), FlightMessage::AffectedRows(2)));

    // select
    let result = db.sql("SELECT * FROM demo").await.unwrap();
    match result {
        ObjectResult::FlightData(flight_messages) => {
            let recordbatches = flight_messages_to_recordbatches(flight_messages).unwrap();
            let pretty = recordbatches.pretty_print().unwrap();
            let expected = "\
+-------+------+--------+-------------------------+
| host  | cpu  | memory | ts                      |
+-------+------+--------+-------------------------+
| host1 | 0.31 | 0.1    | 1970-01-01T00:00:00.100 |
| host2 |      | 0.2    | 1970-01-01T00:00:00.101 |
| host3 | 0.41 |        | 1970-01-01T00:00:00.102 |
| host4 | 0.2  | 0.3    | 1970-01-01T00:00:00.103 |
| host5 | 66.6 | 1024   | 2022-12-28T04:17:07     |
| host6 | 88.8 | 333.3  | 2022-12-28T04:17:08     |
+-------+------+--------+-------------------------+\
";
            assert_eq!(pretty, expected);
        }
        _ => unreachable!(),
    }
}

fn testing_create_expr() -> CreateTableExpr {
    let column_defs = vec![
        ColumnDef {
            name: "host".to_string(),
            datatype: ColumnDataType::String as i32,
            is_nullable: false,
            default_constraint: vec![],
        },
        ColumnDef {
            name: "cpu".to_string(),
            datatype: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: vec![],
        },
        ColumnDef {
            name: "memory".to_string(),
            datatype: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: vec![],
        },
        ColumnDef {
            name: "ts".to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32, // timestamp
            is_nullable: true,
            default_constraint: vec![],
        },
    ];
    CreateTableExpr {
        catalog_name: "".to_string(),
        schema_name: "".to_string(),
        table_name: "demo".to_string(),
        desc: "blabla little magic fairy".to_string(),
        column_defs,
        time_index: "ts".to_string(),
        primary_keys: vec!["host".to_string()],
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: Some(TableId {
            id: MIN_USER_TABLE_ID,
        }),
        region_ids: vec![0],
    }
}
