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

use std::sync::Arc;
#[cfg(feature = "enterprise")]
use std::time::Duration;

use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::value::ValueData;
use api::v1::{
    AddColumn, AddColumns, AlterTableExpr, ColumnDataType, ColumnDef, CreateTableExpr, DdlRequest,
    Row, RowInsertRequest, RowInsertRequests, SemanticType, alter_table_expr,
};
use client::OutputData;
use common_catalog::consts::MITO_ENGINE;
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PAYLOAD_COLUMN, PHYSICAL_TABLE_ID_COLUMN, PROCEDURE_ID_COLUMN,
    PROCEDURE_TRIGGER_COLUMN, SCHEMA_NAME_COLUMN, TABLE_ID_COLUMN, TABLE_NAME_COLUMN, TYPE_COLUMN,
};
use common_test_util::temp_dir::create_temp_dir;
use frontend::instance::Instance;
use meta_srv::gc::GcSchedulerOptions;
use mito2::gc::GcConfig;
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{Channel, QueryContext};
use tests_integration::cluster::GreptimeDbClusterBuilder;

use crate::event_recorder_test_util::{
    assert_eventually_eq, assert_single_event, find_eventually_string, find_eventually_u32,
};

const EVENTS_TABLE: &str = "greptime_private.events";
const TABLE: &str = "table_ddl_events";
const PHYSICAL_TABLE: &str = "table_ddl_events_phy";
const LOGICAL_TABLE: &str = "table_ddl_events_logical";
const AUTO_TABLE: &str = "table_ddl_events_auto";
const AUTO_INFLUX_TABLE: &str = "table_ddl_events_auto_influx";
const GRPC_TABLE: &str = "table_ddl_events_grpc";

#[tokio::test(flavor = "multi_thread")]
async fn test_table_ddl_procedure_events() {
    common_telemetry::init_default_ut_logging();

    // Arrange: use a distributed cluster to submit Undrop and Purge directly
    // through the metasrv DDL manager and exercise logical-table task grouping.
    let home_dir = create_temp_dir("table_ddl_procedure_events");
    #[cfg_attr(not(feature = "enterprise"), allow(unused_mut))]
    let mut gc_options = GcSchedulerOptions {
        enable: true,
        ..Default::default()
    };
    // Soft drop is enterprise-only; non-enterprise builds run this test with
    // plain GC and skip the undrop/purge assertions below.
    #[cfg(feature = "enterprise")]
    {
        gc_options.experimental_soft_drop.enable = true;
        gc_options.experimental_soft_drop.retention = Duration::from_secs(1);
    }
    let cluster = GreptimeDbClusterBuilder::new("table_ddl_procedure_events")
        .await
        .with_datanodes(1)
        .with_metasrv_gc_config(gc_options)
        .with_datanode_gc_config(GcConfig {
            enable: true,
            ..Default::default()
        })
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let frontend = cluster.fe_instance().clone();

    frontend
        .handle_row_inserts(
            RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: AUTO_TABLE.to_string(),
                    rows: Some(api::v1::Rows {
                        schema: vec![
                            api::v1::ColumnSchema {
                                column_name: "host".to_string(),
                                datatype: ColumnDataType::String as i32,
                                semantic_type: SemanticType::Tag as i32,
                                datatype_extension: None,
                                options: None,
                            },
                            api::v1::ColumnSchema {
                                column_name: "ts".to_string(),
                                datatype: ColumnDataType::TimestampMillisecond as i32,
                                semantic_type: SemanticType::Timestamp as i32,
                                datatype_extension: None,
                                options: None,
                            },
                            api::v1::ColumnSchema {
                                column_name: "val".to_string(),
                                datatype: ColumnDataType::Float64 as i32,
                                semantic_type: SemanticType::Field as i32,
                                datatype_extension: None,
                                options: None,
                            },
                        ],
                        rows: vec![Row {
                            values: vec![
                                ValueData::StringValue("host".to_string()).into(),
                                ValueData::TimestampMillisecondValue(0).into(),
                                ValueData::F64Value(1.0).into(),
                            ],
                        }],
                    }),
                }],
            },
            Arc::new(QueryContext::with_channel(
                "greptime",
                "public",
                Channel::Prometheus,
            )),
            false,
            false,
        )
        .await
        .unwrap();
    let auto_create_procedure_id =
        submitted_procedure_id(&frontend, "create_table", AUTO_TABLE).await;
    assert_trigger_context(
        &frontend,
        "create_table",
        &auto_create_procedure_id,
        "auto_create",
        "prometheus",
    )
    .await;

    frontend
        .handle_row_inserts(
            RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: AUTO_INFLUX_TABLE.to_string(),
                    rows: Some(api::v1::Rows {
                        schema: vec![
                            api::v1::ColumnSchema {
                                column_name: "host".to_string(),
                                datatype: ColumnDataType::String as i32,
                                semantic_type: SemanticType::Tag as i32,
                                datatype_extension: None,
                                options: None,
                            },
                            api::v1::ColumnSchema {
                                column_name: "ts".to_string(),
                                datatype: ColumnDataType::TimestampMillisecond as i32,
                                semantic_type: SemanticType::Timestamp as i32,
                                datatype_extension: None,
                                options: None,
                            },
                            api::v1::ColumnSchema {
                                column_name: "val".to_string(),
                                datatype: ColumnDataType::Float64 as i32,
                                semantic_type: SemanticType::Field as i32,
                                datatype_extension: None,
                                options: None,
                            },
                        ],
                        rows: vec![Row {
                            values: vec![
                                ValueData::StringValue("host".to_string()).into(),
                                ValueData::TimestampMillisecondValue(0).into(),
                                ValueData::F64Value(1.0).into(),
                            ],
                        }],
                    }),
                }],
            },
            Arc::new(QueryContext::with_channel(
                "greptime",
                "public",
                Channel::Influx,
            )),
            false,
            false,
        )
        .await
        .unwrap();
    let auto_influx_create_procedure_id =
        submitted_procedure_id(&frontend, "create_table", AUTO_INFLUX_TABLE).await;
    assert_trigger_context(
        &frontend,
        "create_table",
        &auto_influx_create_procedure_id,
        "auto_create",
        "influx",
    )
    .await;

    // gRPC DDL bypasses the SQL statement executor, so it must set its own
    // manual trigger reason before submitting the table procedure.
    GrpcQueryHandler::do_query(
        frontend.as_ref(),
        Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: GRPC_TABLE.to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "host".to_string(),
                        data_type: ColumnDataType::String as i32,
                        is_nullable: true,
                        semantic_type: SemanticType::Tag as i32,
                        ..Default::default()
                    },
                    ColumnDef {
                        name: "ts".to_string(),
                        data_type: ColumnDataType::TimestampMillisecond as i32,
                        is_nullable: false,
                        semantic_type: SemanticType::Timestamp as i32,
                        ..Default::default()
                    },
                ],
                time_index: "ts".to_string(),
                engine: MITO_ENGINE.to_string(),
                ..Default::default()
            })),
        }),
        Arc::new(QueryContext::with_channel(
            "greptime",
            "public",
            Channel::Grpc,
        )),
    )
    .await
    .unwrap();
    let grpc_create_procedure_id =
        submitted_procedure_id(&frontend, "create_table", GRPC_TABLE).await;
    assert_trigger_context(
        &frontend,
        "create_table",
        &grpc_create_procedure_id,
        "manual",
        "grpc",
    )
    .await;
    GrpcQueryHandler::do_query(
        frontend.as_ref(),
        Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::AlterTable(AlterTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: GRPC_TABLE.to_string(),
                kind: Some(alter_table_expr::Kind::AddColumns(AddColumns {
                    add_columns: vec![AddColumn {
                        column_def: Some(ColumnDef {
                            name: "value".to_string(),
                            data_type: ColumnDataType::Float64 as i32,
                            is_nullable: true,
                            semantic_type: SemanticType::Field as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                })),
            })),
        }),
        Arc::new(QueryContext::with_channel(
            "greptime",
            "public",
            Channel::Grpc,
        )),
    )
    .await
    .unwrap();
    let grpc_alter_procedure_id =
        submitted_procedure_id(&frontend, "alter_table", GRPC_TABLE).await;
    assert_trigger_context(
        &frontend,
        "alter_table",
        &grpc_alter_procedure_id,
        "manual",
        "grpc",
    )
    .await;

    // Act / Assert: Create Table retains its rich submitted row and records the
    // created table ID when it completes.
    run_sql_with_context(
        &frontend,
        &format!(
            "CREATE TABLE {TABLE} (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, val DOUBLE)"
        ),
        Arc::new(QueryContext::with_channel(
            "greptime",
            "public",
            Channel::HttpSql,
        )),
    )
    .await;
    let table_id = find_table_id(&frontend, TABLE).await;
    let create_table_procedure_id = submitted_procedure_id(&frontend, "create_table", TABLE).await;
    assert_trigger_context(
        &frontend,
        "create_table",
        &create_table_procedure_id,
        "manual",
        "httpsql",
    )
    .await;
    assert_named_submitted_event(
        &frontend,
        "create_table",
        &create_table_procedure_id,
        "\
+--------------+------------------+
| type         | name             |
+--------------+------------------+
| create_table | table_ddl_events |
+--------------+------------------+",
    )
    .await;
    assert_id_terminal_event(
        &frontend,
        "create_table",
        &create_table_procedure_id,
        table_id,
        "Succeeded",
        "\
+--------------+
| type         |
+--------------+
| create_table |
+--------------+",
    )
    .await;

    // Act / Assert: Alter and Truncate have a rich submitted row and lightweight
    // terminal lifecycle row.
    run_sql(
        &frontend,
        &format!("ALTER TABLE {TABLE} ADD COLUMN extra STRING"),
    )
    .await;
    let alter_table_procedure_id = submitted_procedure_id(&frontend, "alter_table", TABLE).await;
    assert_named_submitted_event(
        &frontend,
        "alter_table",
        &alter_table_procedure_id,
        "\
+-------------+------------------+
| type        | name             |
+-------------+------------------+
| alter_table | table_ddl_events |
+-------------+------------------+",
    )
    .await;
    assert_lightweight_terminal_event(
        &frontend,
        "alter_table",
        &alter_table_procedure_id,
        "Succeeded",
    )
    .await;
    run_sql(
        &frontend,
        &format!("INSERT INTO {TABLE} VALUES ('a', 0, 1, 'b')"),
    )
    .await;
    run_sql(&frontend, &format!("TRUNCATE TABLE {TABLE}")).await;
    let truncate_table_procedure_id =
        submitted_procedure_id(&frontend, "truncate_table", TABLE).await;
    assert_named_submitted_event(
        &frontend,
        "truncate_table",
        &truncate_table_procedure_id,
        "\
+----------------+------------------+
| type           | name             |
+----------------+------------------+
| truncate_table | table_ddl_events |
+----------------+------------------+",
    )
    .await;
    assert_lightweight_terminal_event(
        &frontend,
        "truncate_table",
        &truncate_table_procedure_id,
        "Succeeded",
    )
    .await;

    // Act / Assert: Drop records the table name, while Undrop and Purge use the
    // table ID as their submitted locator.
    run_sql(&frontend, &format!("DROP TABLE {TABLE}")).await;
    let drop_table_procedure_id = submitted_procedure_id(&frontend, "drop_table", TABLE).await;
    assert_named_submitted_event(
        &frontend,
        "drop_table",
        &drop_table_procedure_id,
        "\
+------------+------------------+
| type       | name             |
+------------+------------------+
| drop_table | table_ddl_events |
+------------+------------------+",
    )
    .await;
    assert_lightweight_terminal_event(
        &frontend,
        "drop_table",
        &drop_table_procedure_id,
        "Succeeded",
    )
    .await;
    #[cfg(feature = "enterprise")]
    {
        let (undrop_table_procedure_id, _) = cluster
            .metasrv
            .ddl_manager()
            .submit_undrop_table_task(common_meta::rpc::ddl::UndropTableTask { table_id })
            .await
            .unwrap();
        let undrop_table_procedure_id = undrop_table_procedure_id.to_string();
        assert_id_submitted_event(
            &frontend,
            "undrop_table",
            &undrop_table_procedure_id,
            "\
+--------------+
| type         |
+--------------+
| undrop_table |
+--------------+",
        )
        .await;
        assert_lightweight_terminal_event(
            &frontend,
            "undrop_table",
            &undrop_table_procedure_id,
            "Succeeded",
        )
        .await;
        run_sql(&frontend, &format!("DROP TABLE {TABLE}")).await;
        let (purge_table_procedure_id, _) = cluster
            .metasrv
            .ddl_manager()
            .submit_purge_dropped_table_task(common_meta::rpc::ddl::PurgeDroppedTableTask {
                table_id,
            })
            .await
            .unwrap();
        let purge_table_procedure_id = purge_table_procedure_id.to_string();
        assert_id_submitted_event(
            &frontend,
            "purge_dropped_table",
            &purge_table_procedure_id,
            "\
+---------------------+
| type                |
+---------------------+
| purge_dropped_table |
+---------------------+",
        )
        .await;
        assert_lightweight_terminal_event(
            &frontend,
            "purge_dropped_table",
            &purge_table_procedure_id,
            "Succeeded",
        )
        .await;
    }

    // Arrange / Act: metric logical-table DDL is executed through the distributed
    // frontend, which groups logical DDL tasks for the meta procedure manager.
    run_sql(
        &frontend,
        &format!(
            "CREATE TABLE {PHYSICAL_TABLE} (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH (\"physical_metric_table\" = \"\")"
        ),
    )
    .await;
    let physical_table_id = find_table_id(&frontend, PHYSICAL_TABLE).await;
    run_sql(
        &frontend,
        &format!(
            "CREATE TABLE {LOGICAL_TABLE} (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE=metric WITH (\"on_physical_table\" = \"{PHYSICAL_TABLE}\")"
        ),
    )
    .await;
    let logical_table_id = find_table_id(&frontend, LOGICAL_TABLE).await;
    let create_logical_tables_procedure_id =
        submitted_procedure_id(&frontend, "create_logical_tables", LOGICAL_TABLE).await;

    // Assert: logical Create emits one submitted row per logical table and
    // preserves the logical and physical table IDs on its terminal row.
    assert_named_submitted_event(
        &frontend,
        "create_logical_tables",
        &create_logical_tables_procedure_id,
        "\
+-----------------------+--------------------------+
| type                  | name                     |
+-----------------------+--------------------------+
| create_logical_tables | table_ddl_events_logical |
+-----------------------+--------------------------+",
    )
    .await;
    assert_logical_terminal_event(
        &frontend,
        "create_logical_tables",
        &create_logical_tables_procedure_id,
        logical_table_id,
        physical_table_id,
        "Succeeded",
        "\
+-----------------------+--------------------------+
| type                  | name                     |
+-----------------------+--------------------------+
| create_logical_tables | table_ddl_events_logical |
+-----------------------+--------------------------+",
    )
    .await;

    // Act / Assert: logical Alter preserves the one-row-per-logical-table submitted
    // contract and emits a lightweight terminal row.
    run_sql(
        &frontend,
        &format!("ALTER TABLE {LOGICAL_TABLE} ADD COLUMN rack STRING PRIMARY KEY"),
    )
    .await;
    let alter_logical_tables_procedure_id =
        submitted_procedure_id(&frontend, "alter_logical_tables", LOGICAL_TABLE).await;
    assert_named_submitted_event(
        &frontend,
        "alter_logical_tables",
        &alter_logical_tables_procedure_id,
        "\
+----------------------+--------------------------+
| type                 | name                     |
+----------------------+--------------------------+
| alter_logical_tables | table_ddl_events_logical |
+----------------------+--------------------------+",
    )
    .await;
    assert_lightweight_terminal_event(
        &frontend,
        "alter_logical_tables",
        &alter_logical_tables_procedure_id,
        "Succeeded",
    )
    .await;
}

async fn run_sql(instance: &Arc<Instance>, sql: &str) {
    run_sql_with_context(instance, sql, QueryContext::arc()).await;
}

async fn run_sql_with_context(
    instance: &Arc<Instance>,
    sql: &str,
    query_context: Arc<QueryContext>,
) {
    let output = SqlQueryHandler::do_query(instance.as_ref(), sql, query_context)
        .await
        .remove(0)
        .unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(_)), "{sql}");
}

async fn assert_trigger_context(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
    reason: &str,
    protocol: &str,
) {
    let query = format!(
        "SELECT count(*) AS event_count FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND json_path_match({}, '$.type == \"Submitted\"') AND json_path_match(trigger_context, '$.reason.type == \"{reason}\"') AND json_path_match(trigger_context, '$.protocol == \"{protocol}\"')",
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
    );
    assert_single_event(instance, &query).await;
}

async fn find_table_id(instance: &Arc<Instance>, table_name: &str) -> u32 {
    find_eventually_u32(
        instance,
        &format!(
            "SELECT table_id FROM information_schema.tables WHERE table_catalog = 'greptime' AND table_schema = 'public' AND table_name = '{table_name}'"
        ),
        "table_id",
    )
    .await
}

async fn submitted_procedure_id(
    instance: &Arc<Instance>,
    event_type: &str,
    table_name: &str,
) -> String {
    let query = format!(
        "SELECT {} FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{table_name}' AND json_path_match({}, '$.type == \"Submitted\"') AND json_path_match({}, '$.version == 1') ORDER BY timestamp DESC LIMIT 1",
        PROCEDURE_ID_COLUMN.name(),
        TYPE_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    find_eventually_string(instance, &query, PROCEDURE_ID_COLUMN.name()).await
}

async fn assert_named_submitted_event(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
    expected: &str,
) {
    let query = format!(
        "SELECT {}, {} AS name FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND json_path_match({}, '$.type == \"Submitted\"') AND json_path_match({}, '$.version == 1')",
        TYPE_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

#[cfg(feature = "enterprise")]
async fn assert_id_submitted_event(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
    expected: &str,
) {
    let query = format!(
        "SELECT {} FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND json_path_match({}, '$.type == \"Submitted\"') AND json_path_match({}, '$.version == 1')",
        TYPE_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_id_terminal_event(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
    table_id: u32,
    terminal_trigger: &str,
    expected: &str,
) {
    let query = format!(
        "SELECT {} FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND json_path_match({}, '$.type == \"{terminal_trigger}\"') AND {} = {table_id} AND json_is_null({}) AND {} IS NULL AND {} IS NULL AND {} IS NULL",
        TYPE_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
        CATALOG_NAME_COLUMN.name(),
        SCHEMA_NAME_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_logical_terminal_event(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
    table_id: u32,
    physical_table_id: u32,
    terminal_trigger: &str,
    expected: &str,
) {
    let query = format!(
        "SELECT {}, {} AS name FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND json_path_match({}, '$.type == \"{terminal_trigger}\"') AND {} = {table_id} AND {} = {physical_table_id} AND json_is_null({})",
        TYPE_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
        PHYSICAL_TABLE_ID_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_lightweight_terminal_event(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
    terminal_trigger: &str,
) {
    let query = format!(
        "SELECT count(*) AS event_count FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND json_path_match({}, '$.type == \"{terminal_trigger}\"') AND json_is_null({}) AND {} IS NULL AND {} IS NULL AND {} IS NULL AND {} IS NULL",
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
        CATALOG_NAME_COLUMN.name(),
        SCHEMA_NAME_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
    );
    assert_single_event(instance, &query).await;
}
