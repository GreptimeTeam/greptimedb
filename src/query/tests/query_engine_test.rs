mod median;
mod pow;
mod testing_table;

use std::sync::Arc;

use arrow::array::UInt32Array;
use common_query::logical_plan::create_udaf;
use common_query::prelude::{create_udf, make_scalar_function, Volatility};
use common_recordbatch::util;
use datafusion::arrow_print;
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datafusion::logical_plan::LogicalPlanBuilder;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::types::DataTypeBuilder;
use datatypes::types::Primitive;
use datatypes::vectors::PrimitiveVector;
use query::catalog::memory;
use query::error::Result;
use query::plan::LogicalPlan;
use query::query_engine::{Output, QueryEngineFactory};
use rand::Rng;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;

use crate::median::Median;
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
async fn test_median_udaf() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let mut rng = rand::thread_rng();
    test_median_udaf_with(
        (1..100)
            .map(|_| rng.gen_range(0..100))
            .collect::<Vec<u16>>(),
    )
    .await?;
    test_median_udaf_with(
        (1..100)
            .map(|_| rng.gen_range(0..100))
            .collect::<Vec<u32>>(),
    )
    .await?;
    // FIXME(LFC) wouldn't run
    test_median_udaf_with(
        (1..100)
            .map(|_| rng.gen_range(-100..100))
            .collect::<Vec<i32>>(),
    )
    .await?;
    Ok(())
}

async fn test_median_udaf_with<T>(numbers: Vec<T>) -> Result<()>
where
    T: Primitive + DataTypeBuilder + Ord,
{
    let table_name = format!("{}_numbers", std::any::type_name::<T>());
    let column_name = format!("{}_number", std::any::type_name::<T>());

    let testing_table = Arc::new(TestingTable::new(
        &column_name,
        Arc::new(PrimitiveVector::<T>::from_vec(numbers.clone())),
    ));

    let factory = testing_table::new_query_engine_factory(table_name.clone(), testing_table);
    let engine = factory.query_engine();

    let median_udaf = create_udaf(
        "median",
        ConcreteDataType::uint32_datatype(),
        Arc::new(ConcreteDataType::uint32_datatype()),
        Volatility::Immutable,
        Arc::new(|| Ok(Box::new(Median::new()))),
        Arc::new(vec![ConcreteDataType::string_datatype()]),
    );
    engine.register_udaf(median_udaf);

    let sql = format!(
        "select MEDIAN({}) as median from {}",
        column_name, table_name
    );
    let plan = engine.sql_to_plan(&sql)?;

    let output = engine.execute(&plan).await?;
    let recordbatch_stream = match output {
        Output::RecordBatch(batch) => batch,
        _ => unreachable!(),
    };
    let recordbatch = util::collect(recordbatch_stream).await.unwrap();
    let df_recordbatch = recordbatch
        .into_iter()
        .map(|r| r.df_recordbatch)
        .collect::<Vec<DfRecordBatch>>();

    let pretty_print = arrow_print::write(&df_recordbatch);
    let pretty_print = pretty_print.lines().collect::<Vec<&str>>();

    let numbers = &mut { numbers };
    numbers.sort();
    let expected_median = numbers[numbers.len() / 2];
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
