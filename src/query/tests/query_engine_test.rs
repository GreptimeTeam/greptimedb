mod pow;
mod testing_table;

use std::sync::Arc;

use arrow::array::UInt32Array;
use common_function::scalars::MedianAccumulatorCreator;
use common_query::logical_plan::create_aggregate_function;
use common_query::prelude::{create_udf, make_scalar_function, Volatility};
use common_recordbatch::error::Result as RecordResult;
use common_recordbatch::{util, RecordBatch};
use datafusion::arrow_print;
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datafusion::logical_plan::LogicalPlanBuilder;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::types::DataTypeBuilder;
use datatypes::types::Primitive;
use datatypes::vectors::PrimitiveVector;
use num::NumCast;
use query::catalog::memory;
use query::error::Result;
use query::plan::LogicalPlan;
use query::query_engine::{Output, QueryEngineFactory};
use rand::Rng;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;

use crate::pow::pow;
use crate::testing_table::TestingTable;

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
        Output::RecordBatch(recordbatch) => recordbatch,
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
        Output::RecordBatch(recordbatch) => recordbatch,
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
async fn test_median_udaf() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    test_median_udaf_failed(vec![1.0f32, 2.0f32, 3.0f32]).await?;
    test_median_udaf_failed(vec![1.0f64, 2.0f64, 3.0f64]).await?;

    let mut rng = rand::thread_rng();
    test_median_udaf_success(
        (1..=100)
            .map(|_| rng.gen_range(0..100))
            .collect::<Vec<u16>>(),
    )
    .await?;
    test_median_udaf_success(
        (1..100)
            .map(|_| rng.gen_range(0..100))
            .collect::<Vec<u32>>(),
    )
    .await?;
    test_median_udaf_success(
        (1..100)
            .map(|_| rng.gen_range(-100..100))
            .collect::<Vec<i32>>(),
    )
    .await?;
    test_median_udaf_success(
        (1..=100)
            .map(|_| rng.gen_range(-100..100))
            .collect::<Vec<i64>>(),
    )
    .await?;
    Ok(())
}

async fn test_median_udaf_success<T>(numbers: Vec<T>) -> Result<()>
where
    T: Primitive + DataTypeBuilder + Ord,
{
    let result = execute_udaf(&numbers).await;

    let recordbatch = result.unwrap();
    let df_recordbatch = recordbatch
        .into_iter()
        .map(|r| r.df_recordbatch)
        .collect::<Vec<DfRecordBatch>>();

    let pretty_print = arrow_print::write(&df_recordbatch);
    let pretty_print = pretty_print.lines().collect::<Vec<&str>>();

    let numbers = &mut { numbers };
    numbers.sort();
    let len = numbers.len();
    let expected_median = if len % 2 == 1 {
        numbers[len / 2]
    } else {
        let a: f64 = NumCast::from(numbers[len / 2 - 1]).unwrap();
        let b: f64 = NumCast::from(numbers[len / 2]).unwrap();
        NumCast::from(a / 2.0 + b / 2.0).unwrap()
    };
    let expected_median = format!("| {:<6} |", expected_median);
    let expected = vec![
        "+--------+",
        "| median |",
        "+--------+",
        &expected_median,
        "+--------+",
    ];
    assert_eq!(expected, pretty_print);
    Ok(())
}

async fn test_median_udaf_failed<T>(numbers: Vec<T>) -> Result<()>
where
    T: Primitive + DataTypeBuilder,
{
    let result = execute_udaf(&numbers).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains(&format!(
        "\"MEDIAN\" aggregate function not support date type {}",
        T::type_name()
    )));
    Ok(())
}

async fn execute_udaf<T>(numbers: &[T]) -> RecordResult<Vec<RecordBatch>>
where
    T: Primitive + DataTypeBuilder,
{
    let table_name = format!("{}_numbers", std::any::type_name::<T>());
    let column_name = format!("{}_number", std::any::type_name::<T>());

    let testing_table = Arc::new(TestingTable::new(
        &column_name,
        Arc::new(PrimitiveVector::<T>::from_vec(numbers.to_vec())),
    ));

    let factory = testing_table::new_query_engine_factory(table_name.clone(), testing_table);
    let engine = factory.query_engine();

    let median_udaf =
        create_aggregate_function("median", Arc::new(MedianAccumulatorCreator::default()));
    engine.register_udaf(median_udaf);

    let sql = format!(
        "select MEDIAN({}) as median from {}",
        column_name, table_name
    );
    let plan = engine.sql_to_plan(&sql).unwrap();

    let output = engine.execute(&plan).await.unwrap();
    let recordbatch_stream = match output {
        Output::RecordBatch(batch) => batch,
        _ => unreachable!(),
    };
    util::collect(recordbatch_stream).await
}
