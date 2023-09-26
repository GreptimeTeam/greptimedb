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

use catalog::memory::MemoryCatalogManager;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use session::context::QueryContext;
use table::TableRef;

use crate::parser::QueryLanguageParser;
use crate::{QueryEngineFactory, QueryEngineRef};

mod argmax_test;
mod argmin_test;
mod mean_test;
mod my_sum_udaf_example;
mod percentile_test;
mod polyval_test;
mod query_engine_test;
mod scipy_stats_norm_cdf_test;
mod scipy_stats_norm_pdf;
mod time_range_filter_test;

mod function;
mod pow;

async fn exec_selection(engine: QueryEngineRef, sql: &str) -> Vec<RecordBatch> {
    let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
    let plan = engine
        .planner()
        .plan(stmt, QueryContext::arc())
        .await
        .unwrap();
    let Output::Stream(stream) = engine.execute(plan, QueryContext::arc()).await.unwrap() else {
        unreachable!()
    };
    util::collect(stream).await.unwrap()
}

pub fn new_query_engine_with_table(table: TableRef) -> QueryEngineRef {
    let catalog_manager = MemoryCatalogManager::new_with_table(table);

    QueryEngineFactory::new(catalog_manager, None, None, false).query_engine()
}
