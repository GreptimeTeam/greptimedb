mod pow;
mod testing_table;

use std::sync::Arc;

use arrow::array::UInt32Array;
use common_query::prelude::{create_udf, make_scalar_function, Volatility};
use common_recordbatch::error::Result as RecordResult;
use common_recordbatch::{util, RecordBatch};
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datafusion::logical_plan::LogicalPlanBuilder;
use datatypes::for_all_ordered_primitive_types;
use datatypes::prelude::*;
use datatypes::types::DataTypeBuilder;
use datatypes::vectors::PrimitiveVector;
use num::NumCast;
use query::catalog::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use query::catalog::schema::SchemaProvider;
use query::catalog::{memory, CatalogList, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use query::error::Result;
use query::plan::LogicalPlan;
use query::query_engine::{Output, QueryEngineFactory};
use query::QueryEngine;
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

fn create_query_engine() -> Arc<dyn QueryEngine> {
    let schema_provider = Arc::new(MemorySchemaProvider::new());
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_list = Arc::new(MemoryCatalogList::default());

    macro_rules! create_testing_table {
        ([], $( { $T:ty } ),*) => {
            $(
                let mut rng = rand::thread_rng();

                let table_name = format!("{}_number_even", std::any::type_name::<$T>());
                let column_name = table_name.clone();
                let numbers = (1..=100).map(|_| rng.gen_range(<$T>::MIN..<$T>::MAX)).collect::<Vec<$T>>();
                let table = Arc::new(TestingTable::new(
                    &column_name,
                    Arc::new(PrimitiveVector::<$T>::from_vec(numbers.to_vec())),
                ));
                schema_provider.register_table(table_name, table).unwrap();

                let table_name = format!("{}_number_odd", std::any::type_name::<$T>());
                let column_name = table_name.clone();
                let numbers = (1..=99).map(|_| rng.gen_range(<$T>::MIN..<$T>::MAX)).collect::<Vec<$T>>();
                let table = Arc::new(TestingTable::new(
                    &column_name,
                    Arc::new(PrimitiveVector::<$T>::from_vec(numbers.to_vec())),
                ));
                schema_provider.register_table(table_name, table).unwrap();
            )*
        }
    }
    for_all_ordered_primitive_types! { create_testing_table }

    let table = Arc::new(TestingTable::new(
        "f32_number",
        Arc::new(PrimitiveVector::<f32>::from_vec(vec![1.0f32, 2.0, 3.0])),
    ));
    schema_provider
        .register_table("f32_number".to_string(), table)
        .unwrap();

    let table = Arc::new(TestingTable::new(
        "f64_number",
        Arc::new(PrimitiveVector::<f64>::from_vec(vec![1.0f64, 2.0, 3.0])),
    ));
    schema_provider
        .register_table("f64_number".to_string(), table)
        .unwrap();

    catalog_provider.register_schema(DEFAULT_SCHEMA_NAME, schema_provider);
    catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider);

    let factory = QueryEngineFactory::new(catalog_list);
    factory.query_engine().clone()
}

async fn get_numbers_from_table<T: Primitive + DataTypeBuilder>(
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Vec<T> {
    let column_name = table_name;
    let sql = format!("SELECT {} FROM {}", column_name, table_name);
    let plan = engine.sql_to_plan(&sql).unwrap();

    let output = engine.execute(&plan).await.unwrap();
    let recordbatch_stream = match output {
        Output::RecordBatch(batch) => batch,
        _ => unreachable!(),
    };
    let numbers = util::collect(recordbatch_stream).await.unwrap();

    let columns = numbers[0].df_recordbatch.columns();
    let column = VectorHelper::try_into_vector(&columns[0]).unwrap();
    let v = column
        .as_any()
        .downcast_ref::<PrimitiveVector<T>>()
        .unwrap();
    (0..v.len()).map(|i| v.get_primitive(i)).collect::<Vec<T>>()
}

#[tokio::test]
async fn test_median_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let engine = create_query_engine();

    test_median_failed::<f32>("f32_number", engine.clone()).await?;
    test_median_failed::<f64>("f64_number", engine.clone()).await?;

    macro_rules! test_median {
        ([], $( { $T:ty } ),*) => {
            $(
                let table_name = format!("{}_number_even", std::any::type_name::<$T>());
                test_median_success::<$T>(&table_name, engine.clone()).await?;

                let table_name = format!("{}_number_odd", std::any::type_name::<$T>());
                test_median_success::<$T>(&table_name, engine.clone()).await?;
            )*
        }
    }
    for_all_ordered_primitive_types! { test_median }
    Ok(())
}

async fn test_median_success<T>(table_name: &str, engine: Arc<dyn QueryEngine>) -> Result<()>
where
    T: Primitive + Ord + DataTypeBuilder,
{
    let result = execute_median(table_name, engine.clone()).await.unwrap();
    assert_eq!(1, result.len());
    assert_eq!(result[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, result[0].schema.arrow_schema().fields().len());
    assert_eq!("median", result[0].schema.arrow_schema().field(0).name());

    let columns = result[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), 1);
    let v = VectorHelper::try_into_vector(&columns[0]).unwrap();
    assert_eq!(1, v.len());
    let median = v.get(0);

    let mut numbers = get_numbers_from_table::<T>(table_name, engine.clone()).await;
    numbers.sort();
    let len = numbers.len();
    let expected_median: Value = if len % 2 == 1 {
        numbers[len / 2]
    } else {
        let a: f64 = NumCast::from(numbers[len / 2 - 1]).unwrap();
        let b: f64 = NumCast::from(numbers[len / 2]).unwrap();
        NumCast::from(a / 2.0 + b / 2.0).unwrap()
    }
    .into();
    assert_eq!(expected_median, median);
    Ok(())
}

async fn test_median_failed<T>(table_name: &str, engine: Arc<dyn QueryEngine>) -> Result<()>
where
    T: Primitive + DataTypeBuilder,
{
    let result = execute_median(table_name, engine).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains(&format!(
        "\"MEDIAN\" aggregate function not support date type {}",
        T::type_name()
    )));
    Ok(())
}

async fn execute_median(
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> RecordResult<Vec<RecordBatch>> {
    let column_name = table_name;
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
