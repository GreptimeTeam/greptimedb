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
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field};
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
