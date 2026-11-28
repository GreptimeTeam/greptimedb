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

//! Tests for the plan-to-schema helpers shared with batching mode.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::catalog::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion_common::TableReference;
use datafusion_expr::{LogicalPlanBuilder, col};
use datatypes::schema::{ColumnSchema, Schema};
use store_api::storage::ConcreteDataType;

use super::output_column_schemas;

fn source_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("number", ConcreteDataType::int32_datatype(), false),
        ColumnSchema::new("other", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
    ]))
}

fn source_provider() -> Arc<MemTable> {
    Arc::new(
        MemTable::try_new(
            Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                Field::new("number", ArrowDataType::Int32, false),
                Field::new("other", ArrowDataType::Int32, true),
                Field::new(
                    "ts",
                    ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                    false,
                ),
            ])),
            vec![vec![]],
        )
        .unwrap(),
    )
}

#[test]
fn output_column_schemas_keeps_direct_column_lineage() {
    let source = source_schema();
    let plan = LogicalPlanBuilder::scan(
        TableReference::bare("source"),
        provider_as_source(source_provider()),
        None,
    )
    .unwrap()
    .project(vec![col("number").alias("dis")])
    .unwrap()
    .distinct()
    .unwrap()
    .build()
    .unwrap();

    let (output, lineage) = output_column_schemas(&plan, &source).unwrap();
    // The output alias is kept, while the source column semantics and lineage survive.
    assert_eq!(output[0].name, "dis");
    assert_eq!(lineage, vec![Some(0)]);
}

#[test]
fn output_column_schemas_marks_computed_expressions_without_lineage() {
    let source = source_schema();
    let plan = LogicalPlanBuilder::scan(
        TableReference::bare("source"),
        provider_as_source(source_provider()),
        None,
    )
    .unwrap()
    .project(vec![
        col("number"),
        (col("number") + col("other")).alias("computed"),
    ])
    .unwrap()
    .build()
    .unwrap();

    let (output, lineage) = output_column_schemas(&plan, &source).unwrap();
    assert_eq!(lineage, vec![Some(0), None]);
    // A computed expression must not inherit the source timestamp semantics.
    assert!(!output[1].is_time_index());
}

#[test]
fn output_column_schemas_normalizes_dictionary_output_type() {
    let field = Field::new_dictionary("host", ArrowDataType::UInt32, ArrowDataType::Utf8, true);
    let arrow_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![field]));
    let provider = MemTable::try_new(arrow_schema, vec![vec![]]).unwrap();
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

    let (output, lineage) = output_column_schemas(&plan, &source).unwrap();
    assert_eq!(output[0].data_type, ConcreteDataType::string_datatype());
    assert_eq!(lineage, vec![Some(0)]);
}
