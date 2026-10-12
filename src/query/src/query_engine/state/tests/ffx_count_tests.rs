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

use catalog::RegisterTableRequest;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_recordbatch::RecordBatch;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::options::QueryOptions;
use crate::parser::QueryLanguageParser;
use crate::query_engine::QueryEngineFactory;

fn engine(enabled: bool) -> crate::query_engine::QueryEngineRef {
    let catalog = catalog::memory::new_memory_catalog_manager().unwrap();
    let fact = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0])) as _,
            Arc::new(StringVector::from(vec![Some("a")])) as _,
            Arc::new(Float64Vector::from(vec![Some(2.0)])) as _,
        ],
    )
    .unwrap();
    let dim = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("area", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("weight", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0])) as _,
            Arc::new(StringVector::from(vec![Some("a")])) as _,
            Arc::new(StringVector::from(vec![Some("west")])) as _,
            Arc::new(Float64Vector::from(vec![Some(3.0)])) as _,
        ],
    )
    .unwrap();
    for (name, table_id, batch) in [("fact", 101, fact), ("dim", 102, dim)] {
        let table = MemTable::new_with_catalog(
            name,
            batch,
            table_id,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: name.to_string(),
                table_id,
                table,
            })
            .unwrap();
    }
    QueryEngineFactory::new(
        catalog,
        None,
        None,
        None,
        None,
        false,
        QueryOptions {
            experimental_enable_factorized_join_agg: enabled,
            ..Default::default()
        },
    )
    .query_engine()
}

async fn optimized_plan(engine: &crate::query_engine::QueryEngineRef) -> String {
    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(
        "SELECT f.host, d.area, AVG(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area",
        &context,
    )
    .unwrap();
    let plan = engine.planner().plan(&statement, context).await.unwrap();
    let plan = engine.engine_state().optimize_logical_plan(plan).unwrap();
    plan.display_indent().to_string()
}

#[tokio::test]
async fn factorized_join_agg_is_opt_in() {
    let disabled_plan = optimized_plan(&engine(false)).await;
    let enabled_plan = optimized_plan(&engine(true)).await;
    assert!(disabled_plan.contains("Aggregate"), "{disabled_plan}");
    assert!(enabled_plan.contains("Aggregate"), "{enabled_plan}");
    assert_ne!(disabled_plan, enabled_plan);
}
