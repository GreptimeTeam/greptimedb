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
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, FILE_ENGINE, METRIC_ENGINE, MITO_ENGINE,
    NUMBERS_TABLE_ID,
};
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
use store_api::storage::{PkSample, ScanRequest, SeriesSample};
use table::TableRef;
use table::metadata::{TableId, TableVersion};
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};
use table::test_util::MemTable;

use crate::error::{QueryExecutionSnafu, Result};
use crate::options::QueryOptions as QueryOptionsNew;
use crate::parser::QueryLanguageParser;
use crate::query_engine::options::QueryOptions;
use crate::query_engine::{QueryEngineFactory, QueryEngineRef};
use crate::series_sample::scan::{SampleMethod, SeriesSampleScan};

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

/// A base table of `engine` with a single `v` column.
///
/// The engine decides what a `SAMPLE SERIES` clause can prune: series for the
/// metric engine, a primary-key range for the mito engine, nothing for the rest.
fn engine_table(table_name: &str, table_id: TableId, engine: &str) -> TableRef {
    use datatypes::vectors::Float64Vector;
    use table::Table;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder, TableType};

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
        .engine(engine.to_string())
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

/// The samples of every `SeriesSampleScan` of `plan`, in plan order.
fn sampled_scans(plan: &datafusion_expr::LogicalPlan) -> Vec<SampleMethod> {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_expr::LogicalPlan;

    let mut methods = vec![];
    plan.apply(|node| {
        if let LogicalPlan::Extension(extension) = node
            && let Some(sample) = extension.node.as_any().downcast_ref::<SeriesSampleScan>()
        {
            methods.push(sample.method());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    methods
}

/// Builds a query engine over the tables a `SAMPLE SERIES` clause can meet: the
/// metric-engine base table `metrics`, the mito-engine (classic) table `classic`,
/// and the file-engine table `files`, which keeps no key a sample could prune.
fn series_sample_engine() -> Result<QueryEngineRef> {
    let catalog_manager = catalog_manager()?;
    for (table_name, table_id, engine) in [
        ("metrics", 2048, METRIC_ENGINE),
        ("classic", 2049, MITO_ENGINE),
        ("files", 2050, FILE_ENGINE),
    ] {
        let _ = catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: table_name.to_string(),
                table_id,
                table: engine_table(table_name, table_id, engine),
            })
            .unwrap();
    }
    let factory = QueryEngineFactory::new(
        catalog_manager,
        None,
        None,
        None,
        None,
        false,
        QueryOptionsNew::default(),
    );
    Ok(factory.query_engine())
}

/// The `SAMPLE SERIES (<fraction>)` surface of a metric table: the clause is
/// accepted by the planner and becomes a `SeriesSampleScan` node over the sampled
/// scan, which is what `EXPLAIN` shows and what the region lowers into its scan
/// request. A table of an engine that keeps no key to prune is rejected instead
/// of being planned as an unsampled scan.
#[tokio::test]
async fn test_series_sample_clause() -> Result<()> {
    use datafusion_expr::LogicalPlan;

    common_telemetry::init_default_ut_logging();
    let engine = series_sample_engine()?;

    // The fraction is part of the plan, so `EXPLAIN` shows it.
    let stmt = QueryLanguageParser::parse_sql(
        "EXPLAIN SELECT v FROM metrics SAMPLE SERIES (1/16)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let LogicalPlan::Explain(explain) = &plan else {
        panic!("expected an explain plan, got: {plan}");
    };
    let display = explain.plan.display_indent().to_string();
    assert!(
        display.contains("SeriesSampleScan: series 1/16"),
        "{display}"
    );

    // `0.0625` is the same fraction, not a row percentage.
    let stmt = QueryLanguageParser::parse_sql(
        "SELECT v FROM metrics SAMPLE SERIES (0.0625)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    assert_eq!(vec![SampleMethod::series(1, 16)], sampled_scans(&plan));

    // A query without the clause asks for nothing.
    let stmt =
        QueryLanguageParser::parse_sql("SELECT v FROM metrics", &QueryContext::arc()).unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    assert!(sampled_scans(&plan).is_empty(), "{plan}");

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

    // An engine that keeps no key to prune cannot be sampled, and says so.
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

/// An alias yields a distinct plan node; a self-join reads one table twice with a
/// fraction of its own each, so the two scans keep their own fractions and the
/// query plans without an error.
#[tokio::test]
async fn test_two_series_samples_of_one_table_are_two_nodes() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = series_sample_engine()?;

    let stmt = QueryLanguageParser::parse_sql(
        "SELECT a.v FROM metrics AS a SAMPLE SERIES (1/16) \
         JOIN metrics AS b SAMPLE SERIES (1/4) ON a.v = b.v",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let methods = sampled_scans(&plan);
    assert_eq!(2, methods.len(), "{plan}");
    assert!(
        methods.contains(&SampleMethod::series(1, 16)),
        "{methods:?}"
    );
    assert!(methods.contains(&SampleMethod::series(1, 4)), "{methods:?}");

    Ok(())
}

/// Lowers a sample the way a region does, and returns the scan request it becomes.
///
/// A region reads a `DummyTableProvider` scan, so the plan lowered here is such a
/// scan carrying `method` - the method the planner chose for the sampled table.
fn region_scan_request(method: SampleMethod) -> ScanRequest {
    use datafusion_expr::LogicalPlan;
    use store_api::storage::RegionId;

    use crate::dummy_catalog::DummyTableProvider;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::series_sample::scan::SeriesSampleScanRule;

    let provider = Arc::new(mock_table_provider(RegionId::new(1024, 1)));
    let scan =
        LogicalPlanBuilder::scan("metrics", Arc::new(DefaultTableSource::new(provider)), None)
            .unwrap()
            .build()
            .unwrap();
    let plan = SeriesSampleScan::new(scan, method).into_logical_plan();

    // `new(false)` is the region-side engine: only it lowers the node.
    let lowered = SeriesSampleScanRule::new(false).lower(plan).unwrap().data;
    let LogicalPlan::TableScan(scan) = lowered else {
        panic!("expected a lowered table scan, got: {lowered}");
    };
    let source = scan
        .source
        .downcast_ref::<DefaultTableSource>()
        .expect("expected the dummy catalog source");
    source
        .table_provider
        .downcast_ref::<DummyTableProvider>()
        .expect("expected a dummy table provider")
        .scan_request()
}

/// The fraction is applied to the dimension the engine can prune: a classic
/// (mito-engine) table stores its rows in encoded-primary-key order, so it is
/// sampled by a primary-key range, while a metric-engine table is sampled by
/// series. Each choice is what `EXPLAIN` shows and what the region turns into its
/// scan request, and a scan samples one dimension only.
#[tokio::test]
async fn test_sample_method_follows_the_engine() -> Result<()> {
    use datafusion_expr::LogicalPlan;

    common_telemetry::init_default_ut_logging();
    let engine = series_sample_engine()?;

    // A classic table: the sample is a primary-key range, never a series sample.
    let stmt = QueryLanguageParser::parse_sql(
        "EXPLAIN SELECT v FROM classic SAMPLE SERIES (1/4)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let LogicalPlan::Explain(explain) = &plan else {
        panic!("expected an explain plan, got: {plan}");
    };
    let display = explain.plan.display_indent().to_string();
    assert!(
        display.contains("SeriesSampleScan: primary-key 1/4"),
        "{display}"
    );

    let stmt = QueryLanguageParser::parse_sql(
        "SELECT v FROM classic SAMPLE SERIES (1/4)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let methods = sampled_scans(&plan);
    assert_eq!(vec![SampleMethod::PrimaryKey { num: 1, den: 4 }], methods);
    let request = region_scan_request(methods[0]);
    assert_eq!(Some(PkSample { num: 1, den: 4 }), request.pk_sample);
    assert_eq!(None, request.sample);

    // A metric table: the same clause becomes a series sample, and the region
    // reads every row of a kept series instead of a key range.
    let stmt = QueryLanguageParser::parse_sql(
        "EXPLAIN SELECT v FROM metrics SAMPLE SERIES (1/4)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let LogicalPlan::Explain(explain) = &plan else {
        panic!("expected an explain plan, got: {plan}");
    };
    let display = explain.plan.display_indent().to_string();
    assert!(
        display.contains("SeriesSampleScan: series 1/4"),
        "{display}"
    );

    let stmt = QueryLanguageParser::parse_sql(
        "SELECT v FROM metrics SAMPLE SERIES (1/4)",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let methods = sampled_scans(&plan);
    assert_eq!(vec![SampleMethod::Series { num: 1, den: 4 }], methods);
    let request = region_scan_request(methods[0]);
    assert_eq!(Some(SeriesSample { num: 1, den: 4 }), request.sample);
    assert_eq!(None, request.pk_sample);

    Ok(())
}

/// An engine that keeps neither a series index nor an ordered primary key has
/// nothing a sample could prune, so the clause is rejected while planning and the
/// error names the table and its engine.
#[tokio::test]
async fn test_sample_on_an_engine_without_a_key_is_rejected() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = series_sample_engine()?;

    let stmt = QueryLanguageParser::parse_sql(
        "SELECT v FROM files SAMPLE SERIES (1/4)",
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
        error.contains("SAMPLE SERIES is not supported on table `files`"),
        "{error}"
    );
    assert!(error.contains(&format!("`{FILE_ENGINE}`")), "{error}");

    Ok(())
}

/// One query may sample a metric table and a classic table at once: each scan
/// keeps the method of its own engine and its own fraction, and neither takes the
/// fraction of the other.
#[tokio::test]
async fn test_two_tables_sample_by_their_own_engine() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = series_sample_engine()?;

    let stmt = QueryLanguageParser::parse_sql(
        "SELECT m.v FROM metrics AS m SAMPLE SERIES (1/16) \
         JOIN classic AS c SAMPLE SERIES (1/4) ON m.v = c.v",
        &QueryContext::arc(),
    )
    .unwrap();
    let plan = engine.planner().plan(&stmt, QueryContext::arc()).await?;
    let methods = sampled_scans(&plan);
    assert_eq!(2, methods.len(), "{plan}");
    assert!(
        methods.contains(&SampleMethod::Series { num: 1, den: 16 }),
        "{methods:?}"
    );
    assert!(
        methods.contains(&SampleMethod::PrimaryKey { num: 1, den: 4 }),
        "{methods:?}"
    );

    Ok(())
}
