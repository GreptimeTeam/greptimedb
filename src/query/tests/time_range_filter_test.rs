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

use std::any::Any;
use std::sync::Arc;

use catalog::local::{new_memory_catalog_list, MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{CatalogList, CatalogProvider, SchemaProvider};
use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use common_recordbatch::RecordBatch;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{Int64Vector, TimestampMillisecondVector};
use query::QueryEngineRef;
use session::context::QueryContext;
use table::metadata::{FilterPushDownType, TableInfoRef};
use table::predicate::TimeRangePredicateBuilder;
use table::test_util::MemTable;
use table::Table;
use tokio::sync::RwLock;

struct MemTableWrapper {
    inner: MemTable,
    filter: RwLock<Vec<Expr>>,
}

impl MemTableWrapper {
    pub async fn get_filters(&self) -> Vec<Expr> {
        self.filter.write().await.drain(..).collect()
    }
}

#[async_trait::async_trait]
impl Table for MemTableWrapper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_info(&self) -> TableInfoRef {
        self.inner.table_info()
    }

    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> table::Result<PhysicalPlanRef> {
        *self.filter.write().await = filters.to_vec();
        self.inner.scan(projection, filters, limit).await
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> table::Result<FilterPushDownType> {
        Ok(FilterPushDownType::Exact)
    }
}

fn create_test_engine() -> TimeRangeTester {
    let schema = Schema::try_new(vec![
        ColumnSchema::new("v".to_string(), ConcreteDataType::int64_datatype(), false),
        ColumnSchema::new(
            "ts".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
    ])
    .unwrap();

    let table = Arc::new(MemTableWrapper {
        inner: MemTable::new(
            "m",
            RecordBatch::new(
                Arc::new(schema),
                vec![
                    Arc::new(Int64Vector::from_slice((0..1000).collect::<Vec<i64>>())) as Arc<_>,
                    Arc::new(TimestampMillisecondVector::from_slice(
                        (0..1000).collect::<Vec<i64>>(),
                    )) as Arc<_>,
                ],
            )
            .unwrap(),
        ),
        filter: Default::default(),
    });

    let catalog_list = new_memory_catalog_list().unwrap();

    let default_schema = Arc::new(MemorySchemaProvider::new());
    MemorySchemaProvider::register_table(&default_schema, "m".to_string(), table.clone()).unwrap();

    let default_catalog = Arc::new(MemoryCatalogProvider::new());
    default_catalog
        .register_schema("public".to_string(), default_schema)
        .unwrap();
    catalog_list
        .register_catalog("greptime".to_string(), default_catalog)
        .unwrap();

    let engine = query::QueryEngineFactory::new(catalog_list).query_engine();
    TimeRangeTester { engine, table }
}

struct TimeRangeTester {
    engine: QueryEngineRef,
    table: Arc<MemTableWrapper>,
}

impl TimeRangeTester {
    async fn check(&self, sql: &str, expect: TimestampRange) {
        let stmt = query::parser::QueryLanguageParser::parse_sql(sql).unwrap();
        let _ = self
            .engine
            .execute(
                &self
                    .engine
                    .statement_to_plan(stmt, Arc::new(QueryContext::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let filters = self.table.get_filters().await;

        let range = TimeRangePredicateBuilder::new("ts", &filters).build();
        assert_eq!(expect, range);
    }
}

#[tokio::test]
async fn test_range_filter() {
    let tester = create_test_engine();
    tester
        .check(
            "select * from m where ts >= 990;",
            TimestampRange::from_start(Timestamp::new(990, TimeUnit::Millisecond)),
        )
        .await;

    tester
        .check(
            "select * from m where ts <=1000;",
            TimestampRange::until_end(Timestamp::new(1000, TimeUnit::Millisecond), true),
        )
        .await;

    tester
        .check(
            "select * from m where ts > 1000;",
            TimestampRange::from_start(Timestamp::new(1000, TimeUnit::Millisecond)),
        )
        .await;

    tester
        .check(
            "select * from m where ts < 1000;",
            TimestampRange::until_end(Timestamp::new(1000, TimeUnit::Millisecond), false),
        )
        .await;

    tester
        .check(
            "select * from m where ts > 1000 and ts < 2000;",
            TimestampRange::with_unit(1000, 2000, TimeUnit::Millisecond).unwrap(),
        )
        .await;

    tester
        .check(
            "select * from m where ts >= 1000 or ts < 0;",
            TimestampRange::min_to_max(),
        )
        .await;

    tester
        .check(
            "select * from m where (ts >= 1000 and ts < 2000) or (ts>=3000 and ts<4000);",
            TimestampRange::with_unit(1000, 4000, TimeUnit::Millisecond).unwrap(),
        )
        .await;

    // sql's between is inclusive in both ends
    tester
        .check(
            "select * from m where ts between 1000 and 2000",
            TimestampRange::with_unit(1000, 2001, TimeUnit::Millisecond).unwrap(),
        )
        .await;

    tester
        .check(
            "select * from m where ts in (10, 20, 30, 40)",
            TimestampRange::with_unit(10, 41, TimeUnit::Millisecond).unwrap(),
        )
        .await;

    tester
        .check(
            "select * from m where ts=1000",
            TimestampRange::with_unit(1000, 1001, TimeUnit::Millisecond).unwrap(),
        )
        .await;

    tester
        .check(
            "select * from m where ts=1000 or ts=2000",
            TimestampRange::with_unit(1000, 2001, TimeUnit::Millisecond).unwrap(),
        )
        .await;

    tester
        .check(
            "select * from m where ts>='2023-01-16 17:01:57+08:00'",
            TimestampRange::from_start(Timestamp::new(1673859717000, TimeUnit::Millisecond)),
        )
        .await;

    tester
        .check(
            "select * from m where ts > 10 and ts < 9",
            TimestampRange::empty(),
        )
        .await;
}
