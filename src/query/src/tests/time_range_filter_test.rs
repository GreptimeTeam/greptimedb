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

use std::sync::{Arc, RwLock};

use catalog::memory::new_memory_catalog_manager;
use catalog::RegisterTableRequest;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_expr::expr::Expr;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Int64Vector, TimestampMillisecondVector};
use store_api::data_source::{DataSource, DataSourceRef};
use store_api::storage::ScanRequest;
use table::metadata::FilterPushDownType;
use table::predicate::build_time_range_predicate;
use table::test_util::MemTable;
use table::{Table, TableRef};

use crate::tests::exec_selection;
use crate::{QueryEngineFactory, QueryEngineRef};

struct MemTableWrapper;

impl MemTableWrapper {
    pub fn table(table: TableRef, filter: Arc<RwLock<Vec<Expr>>>) -> TableRef {
        let table_info = table.table_info();
        let data_source = table.data_source();
        let data_source = Arc::new(DataSourceWrapper {
            inner: data_source,
            filter,
        });
        let table = Table::new(table_info, FilterPushDownType::Exact, data_source);
        Arc::new(table)
    }
}

struct DataSourceWrapper {
    inner: DataSourceRef,
    filter: Arc<RwLock<Vec<Expr>>>,
}

impl DataSource for DataSourceWrapper {
    fn get_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream, BoxedError> {
        self.filter.write().unwrap().clone_from(&request.filters);
        self.inner.get_stream(request)
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

    let filter = Arc::new(RwLock::new(vec![]));
    let table = MemTableWrapper::table(
        MemTable::table(
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
        filter.clone(),
    );

    let catalog_manager = new_memory_catalog_manager().unwrap();
    let req = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: "m".to_string(),
        table_id: table.table_info().ident.table_id,
        table: table.clone(),
    };
    let _ = catalog_manager.register_table_sync(req).unwrap();

    let engine =
        QueryEngineFactory::new(catalog_manager, None, None, None, None, false).query_engine();
    TimeRangeTester { engine, filter }
}

struct TimeRangeTester {
    engine: QueryEngineRef,
    filter: Arc<RwLock<Vec<Expr>>>,
}

impl TimeRangeTester {
    async fn check(&self, sql: &str, expect: TimestampRange) {
        let _ = exec_selection(self.engine.clone(), sql).await;
        let mut filters = self.take_filters();

        let range = build_time_range_predicate("ts", TimeUnit::Millisecond, &mut filters);
        assert_eq!(expect, range);
    }

    fn take_filters(&self) -> Vec<Expr> {
        std::mem::take(&mut self.filter.write().unwrap())
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
            TimestampRange::from_start(Timestamp::new(1001, TimeUnit::Millisecond)),
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
            TimestampRange::with_unit(1001, 2000, TimeUnit::Millisecond).unwrap(),
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
