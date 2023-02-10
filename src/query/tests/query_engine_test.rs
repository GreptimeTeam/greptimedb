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

mod pow;
// This is used to suppress the warning: function `create_query_engine` is never used.
// FIXME(yingwen): We finally need to refactor these tests and move them to `query/src`
// so tests can share codes with other mods.
#[allow(unused)]
mod function;

use std::sync::Arc;

use catalog::local::{MemoryCatalogManager, MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{CatalogList, CatalogProvider, SchemaProvider};
use common_base::Plugins;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::BoxedError;
use common_query::prelude::{create_udf, make_scalar_function, Volatility};
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use datafusion::datasource::DefaultTableSource;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::UInt32Vector;
use query::error::{QueryExecutionSnafu, Result};
use query::parser::QueryLanguageParser;
use query::plan::LogicalPlan;
use query::query_engine::options::QueryOptions;
use query::query_engine::QueryEngineFactory;
use session::context::QueryContext;
use snafu::ResultExt;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;
use table::test_util::MemTable;

use crate::pow::pow;

#[tokio::test]
async fn test_datafusion_query_engine() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog::local::new_memory_catalog_list()
        .map_err(BoxedError::new)
        .context(QueryExecutionSnafu)?;
    let factory = QueryEngineFactory::new(catalog_list);
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
    let table = Arc::new(MemTable::new("numbers", recordbatch));

    let limit = 10;
    let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));
    let plan = LogicalPlan::DfPlan(
        LogicalPlanBuilder::scan(
            "numbers",
            Arc::new(DefaultTableSource { table_provider }),
            None,
        )
        .unwrap()
        .limit(0, Some(limit))
        .unwrap()
        .build()
        .unwrap(),
    );

    let output = engine.execute(&plan).await?;

    let recordbatch = match output {
        Output::Stream(recordbatch) => recordbatch,
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
    let expected: Vec<u32> = (0u32..limit as u32).collect();
    assert_eq!(
        *batch.column(0),
        Arc::new(UInt32Vector::from_slice(&expected)) as VectorRef
    );

    Ok(())
}

fn catalog_list() -> Result<Arc<MemoryCatalogManager>> {
    let catalog_list = catalog::local::new_memory_catalog_list()
        .map_err(BoxedError::new)
        .context(QueryExecutionSnafu)?;

    let default_schema = Arc::new(MemorySchemaProvider::new());
    default_schema
        .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
        .unwrap();
    let default_catalog = Arc::new(MemoryCatalogProvider::new());
    default_catalog
        .register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema)
        .unwrap();
    catalog_list
        .register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog)
        .unwrap();
    Ok(catalog_list)
}

#[test]
fn test_query_validate() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog_list()?;

    // set plugins
    let mut plugins = Plugins::new();
    plugins.insert(QueryOptions {
        disallow_cross_schema_query: true,
    });
    let plugins = Arc::new(plugins);

    let factory = QueryEngineFactory::new_with_plugins(catalog_list, plugins);
    let engine = factory.query_engine();

    let stmt = QueryLanguageParser::parse_sql("select number from public.numbers").unwrap();
    let re = engine.statement_to_plan(stmt, Arc::new(QueryContext::new()));
    assert!(re.is_ok());

    let stmt = QueryLanguageParser::parse_sql("select number from wrongschema.numbers").unwrap();
    let re = engine.statement_to_plan(stmt, Arc::new(QueryContext::new()));
    assert!(re.is_err());

    Ok(())
}

#[tokio::test]
async fn test_udf() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog_list()?;

    let factory = QueryEngineFactory::new(catalog_list);
    let engine = factory.query_engine();

    let pow = make_scalar_function(pow);

    let udf = create_udf(
        // datafusion already supports pow, so we use a different name.
        "my_pow",
        vec![
            ConcreteDataType::uint32_datatype(),
            ConcreteDataType::uint32_datatype(),
        ],
        Arc::new(ConcreteDataType::uint32_datatype()),
        Volatility::Immutable,
        pow,
    );

    engine.register_udf(udf);

    let stmt =
        QueryLanguageParser::parse_sql("select my_pow(number, number) as p from numbers limit 10")
            .unwrap();
    let plan = engine
        .statement_to_plan(stmt, Arc::new(QueryContext::new()))
        .unwrap();

    let output = engine.execute(&plan).await?;
    let recordbatch = match output {
        Output::Stream(recordbatch) => recordbatch,
        _ => unreachable!(),
    };

    let numbers = util::collect(recordbatch).await.unwrap();
    assert_eq!(1, numbers.len());
    assert_eq!(numbers[0].num_columns(), 1);
    assert_eq!(1, numbers[0].schema.num_columns());
    assert_eq!("p", numbers[0].schema.column_schemas()[0].name);

    let batch = &numbers[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(batch.column(0).len(), 10);
    let expected: Vec<u32> = vec![1, 1, 4, 27, 256, 3125, 46656, 823543, 16777216, 387420489];
    assert_eq!(
        *batch.column(0),
        Arc::new(UInt32Vector::from_slice(&expected)) as VectorRef
    );

    Ok(())
}
