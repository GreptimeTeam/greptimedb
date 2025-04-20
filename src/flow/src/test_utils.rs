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
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::prelude::*;
use datatypes::schema::Schema;
use datatypes::timestamp::TimestampMillisecond;
use datatypes::vectors::{TimestampMillisecondVectorBuilder, VectorRef};
use itertools::Itertools;
use prost::Message;
use query::options::QueryOptions;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use query::QueryEngine;
use session::context::QueryContext;
/// note here we are using the `substrait_proto_df` crate from the `substrait` module and
/// rename it to `substrait_proto`
use substrait::substrait_proto_df as substrait_proto;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use substrait_proto::proto;
use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};
use table::test_util::MemTable;

use crate::adapter::node_context::IdToNameMap;
use crate::adapter::table_source::test::FlowDummyTableSource;
use crate::adapter::FlownodeContext;
use crate::df_optimizer::apply_df_optimizer;
use crate::expr::GlobalId;
use crate::transform::register_function_to_query_engine;

pub fn create_test_ctx() -> FlownodeContext {
    let mut tri_map = IdToNameMap::new();
    {
        let gid = GlobalId::User(0);
        let name = [
            "greptime".to_string(),
            "public".to_string(),
            "numbers".to_string(),
        ];
        tri_map.insert(Some(name.clone()), Some(1024), gid);
    }

    {
        let gid = GlobalId::User(1);
        let name = [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ];
        tri_map.insert(Some(name.clone()), Some(1025), gid);
    }

    let dummy_source = FlowDummyTableSource::default();

    let mut ctx = FlownodeContext::new(Box::new(dummy_source));
    ctx.table_repr = tri_map;
    ctx.query_context = Some(Arc::new(QueryContext::with("greptime", "public")));

    ctx
}

pub fn create_test_query_engine() -> Arc<dyn QueryEngine> {
    let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
    let req = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: NUMBERS_TABLE_NAME.to_string(),
        table_id: NUMBERS_TABLE_ID,
        table: NumbersTable::table(NUMBERS_TABLE_ID),
    };
    catalog_list.register_table_sync(req).unwrap();

    let schema = vec![
        datatypes::schema::ColumnSchema::new("number", CDT::uint32_datatype(), false),
        datatypes::schema::ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ];
    let mut columns = vec![];
    let numbers = (1..=10).collect_vec();
    let column: VectorRef = Arc::new(<u32 as Scalar>::VectorType::from_vec(numbers));
    columns.push(column);

    let ts = (1..=10).collect_vec();
    let mut builder = TimestampMillisecondVectorBuilder::with_capacity(10);
    ts.into_iter()
        .map(|v| builder.push(Some(TimestampMillisecond::new(v))))
        .count();
    let column: VectorRef = builder.to_vector_cloned();
    columns.push(column);

    let schema = Arc::new(Schema::new(schema));
    let recordbatch = common_recordbatch::RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table("numbers_with_ts", recordbatch);

    let req_with_ts = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: "numbers_with_ts".to_string(),
        table_id: 1024,
        table,
    };
    catalog_list.register_table_sync(req_with_ts).unwrap();

    let schema = vec![
        datatypes::schema::ColumnSchema::new("NUMBER", CDT::uint32_datatype(), false),
        datatypes::schema::ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ];
    let mut columns = vec![];
    let numbers = (1..=10).collect_vec();
    let column: VectorRef = Arc::new(<u32 as Scalar>::VectorType::from_vec(numbers));
    columns.push(column);

    let ts = (1..=10).collect_vec();
    let mut builder = TimestampMillisecondVectorBuilder::with_capacity(10);
    ts.into_iter()
        .map(|v| builder.push(Some(TimestampMillisecond::new(v))))
        .count();
    let column: VectorRef = builder.to_vector_cloned();
    columns.push(column);

    let schema = Arc::new(Schema::new(schema));
    let recordbatch = common_recordbatch::RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table("UPPERCASE_NUMBERS_WITH_TS", recordbatch);

    let req_with_ts = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: "UPPERCASE_NUMBERS_WITH_TS".to_string(),
        table_id: 1025,
        table,
    };
    catalog_list.register_table_sync(req_with_ts).unwrap();

    let factory = query::QueryEngineFactory::new(
        catalog_list,
        None,
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    );

    let engine = factory.query_engine();
    register_function_to_query_engine(&engine);

    assert_eq!("datafusion", engine.name());
    engine
}

pub async fn sql_to_substrait(engine: Arc<dyn QueryEngine>, sql: &str) -> proto::Plan {
    // let engine = create_test_query_engine();
    let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
    let plan = engine
        .planner()
        .plan(&stmt, QueryContext::arc())
        .await
        .unwrap();
    let plan = apply_df_optimizer(plan).await.unwrap();

    // encode then decode so to rely on the impl of conversion from logical plan to substrait plan
    let bytes = DFLogicalSubstraitConvertor {}
        .encode(&plan, DefaultSerializer)
        .unwrap();

    proto::Plan::decode(bytes).unwrap()
}
