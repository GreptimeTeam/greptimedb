use std::sync::Arc;

use arrow::array::UInt32Array;
use common_recordbatch::util;
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datafusion::logical_plan::LogicalPlanBuilder;
use query::catalog::memory::MemoryCatalogList;
use query::error::Result;
use query::plan::LogicalPlan;
use query::query_engine::QueryEngineFactory;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;

#[tokio::test]
async fn test_datafusion_query_engine() -> Result<()> {
    let catalog_list = Arc::new(MemoryCatalogList::default());
    let factory = QueryEngineFactory::new(catalog_list);
    let engine = factory.query_engine();

    let limit = 10;
    let table = Arc::new(NumbersTable::default());
    let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));
    let plan = LogicalPlan::DfPlan(
        LogicalPlanBuilder::scan("numbers", table_provider, None)
            .unwrap()
            .limit(limit)
            .unwrap()
            .build()
            .unwrap(),
    );

    let ret = engine.execute(&plan).await;

    let numbers = util::collect(ret.unwrap()).await.unwrap();

    assert_eq!(1, numbers.len());
    assert_eq!(numbers[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, numbers[0].schema.arrow_schema().fields().len());
    assert_eq!("number", numbers[0].schema.arrow_schema().field(0).name());

    let columns = numbers[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), limit);
    let expected: Vec<u32> = (0u32..limit as u32).collect();
    assert_eq!(
        *columns[0].as_any().downcast_ref::<UInt32Array>().unwrap(),
        UInt32Array::from_slice(&expected)
    );

    Ok(())
}
