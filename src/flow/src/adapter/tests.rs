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

//! Mock test for adapter module
//! TODO(discord9): write mock test

use api::v1::SemanticType;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::catalog::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion_common::TableReference;
use datafusion_expr::LogicalPlanBuilder;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema, SchemaBuilder};
use store_api::storage::{ConcreteDataType, TableId};
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder};

use super::*;

#[test]
fn stateless_output_aliases_are_matched_by_position() {
    let output = vec![ColumnSchema::new(
        "output_alias",
        ConcreteDataType::int32_datatype(),
        false,
    )];
    let sink = vec![ColumnSchema::new(
        "sink_column",
        ConcreteDataType::int32_datatype(),
        false,
    )];
    assert!(validate_sink_layout(&output, &sink).is_ok());
    let proto = crate::adapter::util::column_schemas_to_proto(sink, &[]).unwrap();
    assert_eq!(proto[0].column_name, "sink_column");
}

#[test]
fn stateless_sink_schema_has_tag_and_timestamp_semantics() {
    let schema = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("ts", ConcreteDataType::timestamp_second_datatype(), false)
            .with_time_index(true),
    ];
    let proto =
        crate::adapter::util::column_schemas_to_proto(schema, &["host".to_string()]).unwrap();
    assert_eq!(proto[0].semantic_type, SemanticType::Tag as i32);
    assert_eq!(proto[1].semantic_type, SemanticType::Timestamp as i32);
}

#[test]
fn stateless_resolves_suffix_by_output_arity() {
    let ordinary = ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false);
    let update_at = ColumnSchema::new(
        AUTO_CREATED_UPDATE_AT_TS_COL,
        ConcreteDataType::timestamp_second_datatype(),
        true,
    );
    // Equal arity is an ordinary sink, despite the reserved-looking name.
    assert!(
        resolve_sink_layout(
            &[ordinary.clone(), update_at.clone()],
            &[ordinary.clone(), update_at.clone()]
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        resolve_sink_layout(
            std::slice::from_ref(&ordinary),
            &[ordinary.clone(), update_at]
        )
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn stateless_explicit_timestamp_compatibility_requires_default_and_lineage_absence() {
    let source = Arc::new(Schema::new(vec![
        ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
    ]));
    let output = vec![ColumnSchema::new(
        "value",
        ConcreteDataType::int32_datatype(),
        false,
    )];
    let sink_ts = ColumnSchema::new(
        "event_time",
        ConcreteDataType::timestamp_millisecond_datatype(),
        false,
    )
    .with_time_index(true)
    .with_default_constraint(Some(ColumnDefaultConstraint::Function("now()".into())))
    .unwrap();
    assert!(is_explicit_source_timestamp_compatibility(
        &output,
        &[Some(0)],
        &[output[0].clone(), sink_ts.clone()],
        &source,
    ));
    assert!(!is_explicit_source_timestamp_compatibility(
        &output,
        &[Some(1)],
        &[output[0].clone(), sink_ts],
        &source,
    ));
    let sink_ts_without_default = ColumnSchema::new(
        "event_time",
        ConcreteDataType::timestamp_millisecond_datatype(),
        false,
    )
    .with_time_index(true);
    assert!(!is_explicit_source_timestamp_compatibility(
        &output,
        &[Some(0)],
        &[output[0].clone(), sink_ts_without_default],
        &source,
    ));
}

#[test]
fn stateless_rejects_reserved_auto_names_for_auto_sink() {
    assert!(
        validate_auto_column_names(&[ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::int32_datatype(),
            true,
        )])
        .is_err()
    );
    assert!(
        validate_auto_column_names(&[ColumnSchema::new(
            AUTO_CREATED_PLACEHOLDER_TS_COL,
            ConcreteDataType::int32_datatype(),
            true,
        )])
        .is_err()
    );
}

#[test]
fn stateless_distinct_preserves_direct_column_lineage() {
    let source = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", ConcreteDataType::int32_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
    ]));
    let provider = MemTable::try_new(
        Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("number", ArrowDataType::Int32, false),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
        ])),
        vec![vec![]],
    )
    .unwrap();
    let plan = LogicalPlanBuilder::scan(
        TableReference::bare("source"),
        provider_as_source(Arc::new(provider)),
        None,
    )
    .unwrap()
    .project(vec![datafusion_expr::col("number").alias("dis")])
    .unwrap()
    .distinct()
    .unwrap()
    .build()
    .unwrap();

    let (output, lineage) = super::output_column_schemas(&plan, &source).unwrap();
    assert_eq!(output[0].name, "dis");
    assert_eq!(lineage, vec![Some(0)]);
    let relation = super::relation_desc_from_output(&output, &lineage, &[0]);
    assert_eq!(relation.typ.keys[0].column_indices, vec![0]);
}

#[test]
fn stateless_normalizes_dictionary_output_type() {
    let field = Field::new_dictionary("host", ArrowDataType::UInt32, ArrowDataType::Utf8, true);
    let arrow_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![field]));
    let provider = MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap();
    let plan = LogicalPlanBuilder::scan(
        TableReference::bare("source"),
        provider_as_source(Arc::new(provider)),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let source = Arc::new(Schema::new(vec![ColumnSchema::new(
        "host",
        ConcreteDataType::string_datatype(),
        true,
    )]));
    let (output, lineage) = super::output_column_schemas(&plan, &source).unwrap();
    assert_eq!(output[0].data_type, ConcreteDataType::string_datatype());
    assert_eq!(lineage, vec![Some(0)]);

    let relation = super::relation_desc_from_output(&output, &lineage, &[0]);
    assert_eq!(relation.typ.keys[0].column_indices, vec![0]);
}

#[test]
fn stateless_allows_only_trailing_auto_columns() {
    let ordinary = ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false);
    let update_at = ColumnSchema::new(
        AUTO_CREATED_UPDATE_AT_TS_COL,
        ConcreteDataType::timestamp_second_datatype(),
        true,
    );
    let placeholder = ColumnSchema::new(
        AUTO_CREATED_PLACEHOLDER_TS_COL,
        ConcreteDataType::timestamp_microsecond_datatype(),
        true,
    )
    .with_time_index(true);

    assert_eq!(
        sink_output_column_count(&[ordinary.clone(), update_at.clone()]).unwrap(),
        1
    );
    assert_eq!(
        sink_output_column_count(&[ordinary.clone(), update_at, placeholder]).unwrap(),
        1
    );
    assert!(
        validate_sink_layout(
            std::slice::from_ref(&ordinary),
            std::slice::from_ref(&ordinary)
        )
        .is_ok()
    );
    assert!(
        validate_sink_layout(
            &[ordinary],
            &[
                ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
                ColumnSchema::new("unexpected", ConcreteDataType::int32_datatype(), false),
            ]
        )
        .is_err()
    );
}

pub fn new_test_table_info_with_name<I: IntoIterator<Item = u32>>(
    table_id: TableId,
    table_name: &str,
    _region_numbers: I,
) -> TableInfo {
    let column_schemas = vec![
        ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
    ];
    let schema = SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .version(123)
        .build()
        .unwrap();

    let meta = TableMetaBuilder::empty()
        .schema(Arc::new(schema))
        .primary_key_indices(vec![0])
        .engine("engine")
        .next_column_id(3)
        .build()
        .unwrap();
    TableInfoBuilder::default()
        .table_id(table_id)
        .table_version(5)
        .name(table_name)
        .meta(meta)
        .build()
        .unwrap()
}

/// Create a mock harness for flow node manager
///
/// containing several default table info and schema
fn mock_harness_flow_node_manager() {}

#[test]
fn stateless_flow_slot_write_lease_fences_replacement() {
    let slot = Arc::new(super::StatelessFlowSlot {
        runtime: Arc::new(tokio::sync::RwLock::new(None)),
        active: std::sync::atomic::AtomicBool::new(true),
    });
    let guard = slot.runtime.try_read().unwrap();
    assert!(guard.is_none());
    // A writer cannot acquire the lease while an execution read lease is held.
    assert!(slot.runtime.try_write().is_err());
    drop(guard);
    assert!(slot.runtime.try_write().is_ok());
}

#[test]
fn stateless_captured_slot_rejects_inactive_or_detached_slot() {
    let slot = super::StatelessFlowSlot {
        runtime: Arc::new(tokio::sync::RwLock::new(None)),
        active: std::sync::atomic::AtomicBool::new(false),
    };

    assert!(super::validate_captured_slot(&slot, None, 1, 42).is_err());
}

#[test]
fn stateless_captured_slot_rejects_source_mismatch() {
    let slot = super::StatelessFlowSlot {
        runtime: Arc::new(tokio::sync::RwLock::new(None)),
        active: std::sync::atomic::AtomicBool::new(true),
    };

    assert!(super::validate_captured_slot(&slot, Some(2), 1, 42).is_err());
}

#[derive(Default)]
struct RecordingSink {
    inserts: std::sync::Mutex<Vec<api::v1::RowInsertRequest>>,
}

#[async_trait::async_trait]
impl crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError for RecordingSink {
    async fn do_query(
        &self,
        request: api::v1::greptime_request::Request,
        _: session::context::QueryContextRef,
    ) -> std::result::Result<common_query::Output, BoxedError> {
        let api::v1::greptime_request::Request::RowInserts(request) = request else {
            panic!("unexpected frontend request");
        };
        if request
            .inserts
            .iter()
            .any(|insert| insert.table_name == "failed_sink")
        {
            return Err(BoxedError::new(
                InvalidQuerySnafu {
                    reason: "injected sink failure",
                }
                .build(),
            ));
        }
        let count = request
            .inserts
            .iter()
            .map(|insert| insert.rows.as_ref().unwrap().rows.len())
            .sum();
        self.inserts.lock().unwrap().extend(request.inserts);
        Ok(common_query::Output::new_with_affected_rows(count))
    }
}

struct StreamingHarness {
    engine: StreamingEngine,
    metadata: TableMetadataManagerRef,
    catalog: Arc<catalog::memory::MemoryCatalogManager>,
    sink: Arc<RecordingSink>,
}

impl StreamingHarness {
    async fn new() -> Self {
        let metadata = Arc::new(common_meta::key::TableMetadataManager::new(Arc::new(
            common_meta::kv_backend::memory::MemoryKvBackend::new(),
        )));
        metadata.init().await.unwrap();
        let catalog = catalog::memory::new_memory_catalog_manager().unwrap();
        let query = query::QueryEngineFactory::new(
            catalog.clone(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();
        let sink = Arc::new(RecordingSink::default());
        let handler: Arc<
            dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError,
        > = sink.clone();
        let frontend =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());
        Self {
            engine: StreamingEngine::new(None, query, metadata.clone(), Arc::new(frontend)),
            metadata,
            catalog,
            sink,
        }
    }

    async fn table(&self, id: u32, name: &str) -> TableInfo {
        let info = new_test_table_info_with_name(id, name, []);
        self.metadata
            .create_table_metadata(
                info.clone(),
                common_meta::key::table_route::TableRouteValue::physical(vec![]),
                Default::default(),
            )
            .await
            .unwrap();
        self.register(&info);
        info
    }

    fn register(&self, info: &TableInfo) {
        self.catalog
            .register_table_sync(catalog::RegisterTableRequest {
                catalog: info.catalog_name.clone(),
                schema: info.schema_name.clone(),
                table_name: info.name.clone(),
                table_id: info.ident.table_id,
                table: table::test_util::EmptyTable::from_table_info(info),
            })
            .unwrap();
    }

    async fn flow(&self, id: FlowId, source: u32, sink: &str, sql: &str) {
        self.engine
            .create_flow_inner(CreateFlowArgs {
                flow_id: id,
                source_table_ids: vec![source],
                sink_table_name: ["greptime".into(), "public".into(), sink.into()],
                create_if_not_exists: false,
                or_replace: false,
                expire_after: None,
                eval_interval: None,
                comment: None,
                sql: sql.into(),
                flow_options: Default::default(),
                query_ctx: Some(session::context::QueryContext::arc().as_ref().clone()),
                eval_schedule: None,
            })
            .await
            .unwrap();
    }

    fn take_numbers(&self) -> Vec<(String, Vec<i32>)> {
        let inserts = std::mem::take(&mut *self.sink.inserts.lock().unwrap());
        inserts
            .into_iter()
            .map(|insert| {
                let mut values = insert
                    .rows
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|row| {
                        let Some(api::v1::value::ValueData::I32Value(value)) =
                            row.values[0].value_data
                        else {
                            panic!("expected int32 output");
                        };
                        value
                    })
                    .collect::<Vec<_>>();
                values.sort_unstable();
                (insert.table_name, values)
            })
            .collect()
    }
}

fn mirror_request(table: u32, region: u32, values: &[i32]) -> api::v1::region::InsertRequest {
    use api::v1::value::ValueData;
    api::v1::region::InsertRequest {
        region_id: RegionId::new(table, region).as_u64(),
        rows: Some(api::v1::Rows {
            schema: util::column_schemas_to_proto(
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
                &["number".into()],
            )
            .unwrap(),
            rows: values
                .iter()
                .map(|value| api::v1::Row {
                    values: vec![
                        api::v1::Value {
                            value_data: Some(ValueData::I32Value(*value)),
                        },
                        api::v1::Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(1)),
                        },
                    ],
                })
                .collect(),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn stateless_failed_flow_and_table_do_not_starve_healthy_sinks() {
    let h = StreamingHarness::new().await;
    h.table(1, "source_a").await;
    h.table(2, "source_b").await;
    h.table(3, "failed_sink").await;
    h.table(4, "healthy_a").await;
    h.table(5, "healthy_b").await;
    h.flow(1, 1, "failed_sink", "SELECT number, ts FROM source_a")
        .await;
    h.flow(2, 1, "healthy_a", "SELECT number, ts FROM source_a")
        .await;
    h.flow(3, 2, "healthy_b", "SELECT number, ts FROM source_b")
        .await;
    assert!(
        h.engine
            .handle_inserts_inner(api::v1::region::InsertRequests {
                requests: vec![mirror_request(1, 0, &[11]), mirror_request(2, 0, &[22])],
            })
            .await
            .is_err()
    );
    assert_eq!(
        h.take_numbers(),
        vec![
            ("healthy_a".into(), vec![11]),
            ("healthy_b".into(), vec![22])
        ]
    );

    let mut malformed = mirror_request(1, 1, &[99]);
    malformed.rows.as_mut().unwrap().schema.pop();
    assert!(
        h.engine
            .handle_inserts_inner(api::v1::region::InsertRequests {
                requests: vec![
                    mirror_request(1, 0, &[33]),
                    malformed,
                    mirror_request(1, 2, &[44]),
                    mirror_request(2, 0, &[55])
                ],
            })
            .await
            .is_err()
    );
    assert_eq!(h.take_numbers(), vec![("healthy_b".into(), vec![55])]);
    h.engine.remove_flow_inner(1).await.unwrap();
    h.engine
        .handle_inserts_inner(api::v1::region::InsertRequests {
            requests: vec![mirror_request(1, 0, &[66])],
        })
        .await
        .unwrap();
    assert_eq!(h.take_numbers(), vec![("healthy_a".into(), vec![66])]);
}

#[tokio::test]
async fn stateless_distinct_groups_regions_without_retaining_previous_envelope() {
    let h = StreamingHarness::new().await;
    h.table(1, "source").await;
    h.table(2, "sink").await;
    h.flow(
        1,
        1,
        "sink",
        "SELECT DISTINCT number AS value, ts FROM source",
    )
    .await;
    for _ in 0..2 {
        h.engine
            .handle_inserts_inner(api::v1::region::InsertRequests {
                requests: vec![mirror_request(1, 0, &[1, 2]), mirror_request(1, 1, &[2, 3])],
            })
            .await
            .unwrap();
        assert_eq!(h.take_numbers(), vec![("sink".into(), vec![1, 2, 3])]);
    }
}

#[tokio::test]
async fn stateless_schema_bump_rebuilds_for_current_and_subsequent_writes() {
    let h = StreamingHarness::new().await;
    let mut source = h.table(1, "source").await;
    h.table(2, "sink").await;
    h.flow(1, 1, "sink", "SELECT number, ts FROM source").await;
    let current = h
        .metadata
        .table_info_manager()
        .get(1)
        .await
        .unwrap()
        .unwrap();
    let mut columns = source.meta.schema.column_schemas().to_vec();
    columns.push(ColumnSchema::new(
        "extra",
        ConcreteDataType::int32_datatype(),
        true,
    ));
    source.meta.schema = Arc::new(
        SchemaBuilder::try_from(columns)
            .unwrap()
            .version(124)
            .build()
            .unwrap(),
    );
    h.metadata
        .update_table_info(&current, None, source.clone())
        .await
        .unwrap();
    // The metadata is newer than the catalog provider: do not publish a plan
    // carrying old column indices under the new version.
    assert!(
        h.engine
            .handle_inserts_inner(api::v1::region::InsertRequests {
                requests: vec![mirror_request(1, 0, &[99])],
            })
            .await
            .is_err()
    );
    assert!(h.take_numbers().is_empty());
    h.catalog
        .deregister_table_sync(catalog::DeregisterTableRequest {
            catalog: source.catalog_name.clone(),
            schema: source.schema_name.clone(),
            table_name: source.name.clone(),
        })
        .unwrap();
    h.register(&source);
    let slot = h.engine.flow_ids_for_table(1).await.pop().unwrap().1;
    assert_eq!(
        slot.runtime
            .read()
            .await
            .as_ref()
            .unwrap()
            .source_schema_version,
        123
    );
    let (left_rows, types, version) = h
        .engine
        .handle_insert_request(mirror_request(1, 0, &[5]))
        .await
        .unwrap();
    let (right_rows, _, _) = h
        .engine
        .handle_insert_request(mirror_request(1, 1, &[6]))
        .await
        .unwrap();
    // Queue both readers behind a writer. Releasing it grants both readers,
    // so neither rebuild can publish before both writes observe the old runtime.
    let lease = slot.runtime.write().await;
    let left = h
        .engine
        .execute_flow(1, slot.clone(), 1, left_rows, &types, version);
    let right = h
        .engine
        .execute_flow(1, slot.clone(), 1, right_rows, &types, version);
    tokio::pin!(left, right);
    assert!(futures::poll!(&mut left).is_pending());
    assert!(futures::poll!(&mut right).is_pending());
    drop(lease);
    let (left, right) = tokio::join!(left, right);
    left.unwrap();
    right.unwrap();
    let mut inserted = h.take_numbers();
    inserted.sort();
    assert_eq!(
        inserted,
        vec![("sink".into(), vec![5]), ("sink".into(), vec![6])]
    );
    assert_eq!(
        slot.runtime
            .read()
            .await
            .as_ref()
            .unwrap()
            .source_schema_version,
        124
    );
    for number in [7, 8] {
        h.engine
            .handle_inserts_inner(api::v1::region::InsertRequests {
                requests: vec![mirror_request(1, 0, &[number])],
            })
            .await
            .unwrap();
        assert_eq!(h.take_numbers(), vec![("sink".into(), vec![number])]);
    }
}

#[tokio::test]
async fn stateless_rejects_wrong_provider_identity_and_detached_lifecycle() {
    let h = StreamingHarness::new().await;
    let source = h.table(1, "source").await;
    h.table(2, "sink").await;
    h.flow(1, 1, "sink", "SELECT number, ts FROM source").await;
    let old_slot = h.engine.flow_ids_for_table(1).await.pop().unwrap().1;
    let (rows, types, version) = h
        .engine
        .handle_insert_request(mirror_request(1, 0, &[9]))
        .await
        .unwrap();
    h.engine.remove_flow_inner(1).await.unwrap();
    h.flow(1, 1, "sink", "SELECT number, ts FROM source").await;
    assert!(
        h.engine
            .execute_flow(1, old_slot, 1, rows, &types, version)
            .await
            .is_err()
    );
    assert!(h.take_numbers().is_empty());

    h.catalog
        .deregister_table_sync(catalog::DeregisterTableRequest {
            catalog: source.catalog_name.clone(),
            schema: source.schema_name.clone(),
            table_name: source.name.clone(),
        })
        .unwrap();
    let mut wrong = source.clone();
    wrong.ident.table_id = 99;
    h.register(&wrong);
    let args = h
        .engine
        .stateless_flows
        .read()
        .await
        .get(&1)
        .unwrap()
        .runtime
        .read()
        .await
        .as_ref()
        .unwrap()
        .create_args
        .clone();
    assert!(h.engine.build_stateless_flow(&args, false).await.is_err());
    assert!(h.take_numbers().is_empty());
}
