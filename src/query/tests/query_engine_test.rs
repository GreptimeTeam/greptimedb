mod interp;
mod pow;

use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::array::UInt32Array;
use common_query::prelude::{create_udf, make_scalar_function, Volatility};
use common_recordbatch::util;
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datafusion::logical_plan::LogicalPlanBuilder;
use datatypes::data_type::ConcreteDataType;
use query::catalog::memory;
use query::error::Result;
use query::plan::LogicalPlan;
use query::query_engine::{Output, QueryEngineFactory};
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;

use crate::interp::interp;
use crate::pow::pow;

#[tokio::test]
async fn test_datafusion_query_engine() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = memory::new_memory_catalog_list()?;
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

    let output = engine.execute(&plan).await?;

    let recordbatch = match output {
        Output::RecordBatch(recordbach) => recordbach,
        _ => unreachable!(),
    };

    let numbers = util::collect(recordbatch).await.unwrap();

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

#[tokio::test]
async fn test_udf() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = memory::new_memory_catalog_list()?;
    let factory = QueryEngineFactory::new(catalog_list);
    let engine = factory.query_engine();

    let pow = make_scalar_function(pow);

    let udf = create_udf(
        "pow",
        vec![
            ConcreteDataType::uint32_datatype(),
            ConcreteDataType::uint32_datatype(),
        ],
        Arc::new(ConcreteDataType::uint32_datatype()),
        Volatility::Immutable,
        pow,
    );

    engine.register_udf(udf);

    let plan = engine.sql_to_plan("select pow(number, number) as p from numbers limit 10")?;

    let output = engine.execute(&plan).await?;
    let recordbatch = match output {
        Output::RecordBatch(recordbach) => recordbach,
        _ => unreachable!(),
    };

    let numbers = util::collect(recordbatch).await.unwrap();
    assert_eq!(1, numbers.len());
    assert_eq!(numbers[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, numbers[0].schema.arrow_schema().fields().len());
    assert_eq!("p", numbers[0].schema.arrow_schema().field(0).name());

    let columns = numbers[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), 10);
    let expected: Vec<u32> = vec![1, 1, 4, 27, 256, 3125, 46656, 823543, 16777216, 387420489];
    assert_eq!(
        *columns[0].as_any().downcast_ref::<UInt32Array>().unwrap(),
        UInt32Array::from_slice(&expected)
    );

    Ok(())
}

#[tokio::test]
async fn test_udf_interp() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = memory::new_memory_catalog_list()?;
    let factory = QueryEngineFactory::new(catalog_list);
    let engine = factory.query_engine();

    let interp = make_scalar_function(interp);

    let udf = create_udf(
        "interp",
        vec![
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::float64_datatype(),
        ],
        Arc::new(ConcreteDataType::float64_datatype()),
        Volatility::Immutable,
        interp,
    );

    engine.register_udf(udf);

    let plan =
        engine.sql_to_plan("select interp(number, number,number) as p from numbers limit 10")?;

    let output = engine.execute(&plan).await?;
    let recordbatch = match output {
        Output::RecordBatch(recordbach) => recordbach,
        _ => unreachable!(),
    };

    let numbers = util::collect(recordbatch).await.unwrap();

    assert_eq!(1, numbers.len());
    assert_eq!(numbers[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, numbers[0].schema.arrow_schema().fields().len());
    assert_eq!("p", numbers[0].schema.arrow_schema().field(0).name());
    let columns = numbers[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), 10);
    let expected: Vec<f64> = vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
    assert_eq!(
        *columns[0].as_any().downcast_ref::<Float64Array>().unwrap(),
        Float64Array::from_slice(&expected)
    );

    Ok(())
}
