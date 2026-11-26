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

use arrow::array::{ArrayRef, UInt32Array};
use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use common_base::Plugins;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_recordbatch::{RecordBatch, util};
use datafusion::datasource::DefaultTableSource;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::UInt32Vector;
use session::context::QueryContext;
use snafu::ResultExt;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};
use table::test_util::MemTable;

use crate::error::{QueryExecutionSnafu, Result};
use crate::options::QueryOptions as QueryOptionsNew;
use crate::parser::QueryLanguageParser;
use crate::query_engine::QueryEngineFactory;
use crate::query_engine::options::QueryOptions;

#[tokio::test]
async fn test_datafusion_query_engine() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog::memory::new_memory_catalog_manager()
        .map_err(BoxedError::new)
        .context(QueryExecutionSnafu)?;
    let factory = QueryEngineFactory::new(
        catalog_list,
        None,
        None,
        None,
        None,
        false,
        QueryOptionsNew::default(),
    );
    let engine = factory.query_engine();

    let column_schemas = vec![ColumnSchema::new(
        "number",
        ConcreteDataType::uint32_datatype(),
        false,
    )];
    let schema = Arc::new(Schema::new(column_schemas));
    let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice(
        (0..100).collect::<Vec<_>>(),
    ))];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table("numbers", recordbatch);

    let limit = 10;
    let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));
    let plan = LogicalPlanBuilder::scan(
        "numbers",
        Arc::new(DefaultTableSource { table_provider }),
        None,
    )
    .unwrap()
    .limit(0, Some(limit))
    .unwrap()
    .build()
    .unwrap();

    let output = engine.execute(plan, QueryContext::arc()).await?;

    let recordbatch = match output.data {
        OutputData::Stream(recordbatch) => recordbatch,
        _ => unreachable!(),
    };

    let numbers = util::collect(recordbatch).await.unwrap();

    assert_eq!(1, numbers.len());
    assert_eq!(numbers[0].num_columns(), 1);
    assert_eq!(1, numbers[0].schema.num_columns());
    assert_eq!("number", numbers[0].schema.column_schemas()[0].name);

    let batch = &numbers[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(batch.column(0).len(), limit);
    let expected = Arc::new(UInt32Array::from_iter_values(
        (0u32..limit as u32).collect::<Vec<_>>(),
    )) as ArrayRef;
    assert_eq!(batch.column(0), &expected);

    Ok(())
}

fn catalog_manager() -> Result<Arc<MemoryCatalogManager>> {
    let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
    let req = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: NUMBERS_TABLE_NAME.to_string(),
        table_id: NUMBERS_TABLE_ID,
        table: NumbersTable::table(NUMBERS_TABLE_ID),
    };
    let _ = catalog_manager.register_table_sync(req).unwrap();

    Ok(catalog_manager)
}

#[tokio::test]
async fn test_query_validate() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog_manager()?;

    // set plugins
    let plugins = Plugins::new();
    plugins.insert(QueryOptions {
        disallow_cross_catalog_query: true,
    });

    let factory = QueryEngineFactory::new_with_plugins(
        catalog_list,
        None,
        None,
        None,
        None,
        None,
        false,
        plugins,
        QueryOptionsNew::default(),
    );
    let engine = factory.query_engine();

    let stmt =
        QueryLanguageParser::parse_sql("select number from public.numbers", &QueryContext::arc())
            .unwrap();
    assert!(
        engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .is_ok()
    );

    let stmt = QueryLanguageParser::parse_sql(
        "select number from wrongschema.numbers",
        &QueryContext::arc(),
    )
    .unwrap();
    assert!(
        engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .is_err()
    );
    Ok(())
}

/// The `SAMPLE SERIES (<fraction>)` surface of a metric table: the clause is
/// accepted by the planner and the fraction reaches the scan request, while a
/// table that is not a metric-engine table is rejected instead of being planned
/// as an unsampled scan.
#[tokio::test]
async fn test_series_sample_clause() -> Result<()> {
    use common_catalog::consts::METRIC_ENGINE;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_expr::LogicalPlan;
    use datatypes::vectors::Float64Vector;
    use store_api::storage::{SeriesSample, TimeSeriesDistribution};
    use table::Table;
    use table::TableRef;
    use table::metadata::{
        FilterPushDownType, TableId, TableInfoBuilder, TableMetaBuilder, TableType, TableVersion,
    };

    fn metric_table(table_name: &str, table_id: TableId) -> TableRef {
        let column_schemas = vec![ColumnSchema::new(
            "v",
            ConcreteDataType::float64_datatype(),
            true,
        )];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![Arc::new(Float64Vector::from_slice([1.0]))];
        let recordbatch = RecordBatch::new(schema, columns).unwrap();
        let meta = TableMetaBuilder::empty()
            .schema(recordbatch.schema.clone())
            .primary_key_indices(vec![])
            .value_indices(vec![0])
            .engine(METRIC_ENGINE.to_string())
            .next_column_id(1)
            .options(Default::default())
            .created_on(Default::default())
            .build()
            .unwrap();
        let info = Arc::new(
            TableInfoBuilder::default()
                .table_id(table_id)
                .table_version(0 as TableVersion)
                .name(table_name.to_string())
                .schema_name(DEFAULT_SCHEMA_NAME.to_string())
                .catalog_name(DEFAULT_CATALOG_NAME.to_string())
                .desc(None)
                .table_type(TableType::Base)
                .meta(meta)
                .build()
                .unwrap(),
        );
        Arc::new(Table::new(
            info,
            FilterPushDownType::Unsupported,
            Arc::new(table::dist_table::DummyDataSource),
        ))
    }

    /// Returns the scan request of the single table scan of `plan`.
    fn scan_request(plan: &LogicalPlan) -> store_api::storage::ScanRequest {
        let mut requests = vec![];
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Some(source) = scan.source.as_ref().downcast_ref::<DefaultTableSource>()
                && let Some(adapter) = source
                    .table_provider
                    .downcast_ref::<DfTableProviderAdapter>()
            {
                requests.push(adapter.get_scan_req());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one table scan");
        requests.pop().unwrap()
    }

    common_telemetry::init_default_ut_logging();
    let catalog_manager = catalog_manager()?;
    let _ = catalog_manager
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "metrics".to_string(),
            table_id: 2048,
            table: metric_table("metrics", 2048),
        })
        .unwrap();
    let factory = QueryEngineFactory::new(
        catalog_manager,
        None,
        None,
        None,
        None,
        false,
        QueryOptionsNew::default(),
    );
    let engine = factory.query_engine();

    // A fraction reaches the scan request together with the distribution sampling needs.
    let stmt = QueryLanguageParser::parse_sql(
        "SELECT v FROM metrics SAMPLE SERIES (1/16)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let request = scan_request(&plan);
    assert_eq!(request.sample, Some(SeriesSample { num: 1, den: 16 }));
    assert_eq!(
        request.distribution,
        Some(TimeSeriesDistribution::PerSeries)
    );

    // `0.0625` is the same fraction, not a row percentage.
    let stmt = QueryLanguageParser::parse_sql(
        "SELECT v FROM metrics SAMPLE SERIES (0.0625)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    assert_eq!(
        scan_request(&plan).sample,
        Some(SeriesSample { num: 1, den: 16 })
    );

    // A query without the clause asks for nothing.
    let stmt =
        QueryLanguageParser::parse_sql("SELECT v FROM metrics", &QueryContext::arc()).unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let request = scan_request(&plan);
    assert_eq!(request.sample, None);
    assert_eq!(request.distribution, None);

    // An invalid fraction is rejected while parsing the clause.
    let stmt = QueryLanguageParser::parse_sql(
        "SELECT v FROM metrics SAMPLE SERIES (1/1)",
        &QueryContext::arc(),
    )
    .unwrap();
    let error = engine
        .planner()
        .plan(&stmt, QueryContext::arc())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("the fraction must be less than 1"),
        "{error}"
    );

    // A non metric-engine table cannot be sampled, and says so.
    let stmt = QueryLanguageParser::parse_sql(
        "SELECT number FROM numbers SAMPLE SERIES (1/16)",
        &QueryContext::arc(),
    )
    .unwrap();
    let error = engine
        .planner()
        .plan(&stmt, QueryContext::arc())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("SAMPLE SERIES is not supported on table `numbers`"),
        "{error}"
    );

    // Row-level `TABLESAMPLE` keeps its previous (unsampled) meaning.
    let stmt = QueryLanguageParser::parse_sql(
        "SELECT number FROM numbers TABLESAMPLE (10 PERCENT)",
        &QueryContext::arc(),
    )
    .unwrap();
    assert!(
        engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .is_ok()
    );

    Ok(())
}
