mod pow;

use std::sync::Arc;

use arrow::array::UInt32Array;
use catalog::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{
    CatalogList, CatalogProvider, SchemaProvider, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use common_query::prelude::{create_udf, make_scalar_function, Volatility};
use common_recordbatch::error::Result as RecordResult;
use common_recordbatch::{util, RecordBatch};
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datafusion::logical_plan::LogicalPlanBuilder;
use datatypes::for_all_ordered_primitive_types;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::types::PrimitiveElement;
use datatypes::vectors::{Float32Vector, Float64Vector, PrimitiveVector, UInt32Vector};
use num::NumCast;
use query::error::Result;
use query::plan::LogicalPlan;
use query::query_engine::{Output, QueryEngineFactory};
use query::QueryEngine;
use rand::Rng;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;
use test_util::MemTable;

use crate::pow::pow;

#[tokio::test]
async fn test_datafusion_query_engine() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog::memory::new_memory_catalog_list()?;
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
    let catalog_list = catalog::memory::new_memory_catalog_list()?;

    let default_schema = Arc::new(MemorySchemaProvider::new());
    default_schema
        .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
        .unwrap();
    let default_catalog = Arc::new(MemoryCatalogProvider::new());
    default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema);
    catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog);

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

    // create table with ordered primitives, and all columns' length are even
    let mut column_schemas = vec![];
    let mut columns = vec![];
    macro_rules! create_even_number_table {
        ([], $( { $T:ty } ),*) => {
            $(
                let mut rng = rand::thread_rng();

                let column_name = format!("{}_number_even", std::any::type_name::<$T>());
                let column_schema = ColumnSchema::new(column_name, Value::from(<$T>::default()).data_type(), true);
                column_schemas.push(column_schema);

                let numbers = (1..=100).map(|_| rng.gen_range(<$T>::MIN..<$T>::MAX)).collect::<Vec<$T>>();
                let column: VectorRef = Arc::new(PrimitiveVector::<$T>::from_vec(numbers.to_vec()));
                columns.push(column);
            )*
        }
    }
    for_all_ordered_primitive_types! { create_even_number_table }

    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let even_number_table = Arc::new(MemTable::new("even_numbers", recordbatch));
    schema_provider
        .register_table(
            even_number_table.table_name().to_string(),
            even_number_table,
        )
        .unwrap();

    // create table with ordered primitives, and all columns' length are odd
    let mut column_schemas = vec![];
    let mut columns = vec![];
    macro_rules! create_odd_number_table {
        ([], $( { $T:ty } ),*) => {
            $(
                let mut rng = rand::thread_rng();

                let column_name = format!("{}_number_odd", std::any::type_name::<$T>());
                let column_schema = ColumnSchema::new(column_name, Value::from(<$T>::default()).data_type(), true);
                column_schemas.push(column_schema);

                let numbers = (1..=99).map(|_| rng.gen_range(<$T>::MIN..<$T>::MAX)).collect::<Vec<$T>>();
                let column: VectorRef = Arc::new(PrimitiveVector::<$T>::from_vec(numbers.to_vec()));
                columns.push(column);
            )*
        }
    }
    for_all_ordered_primitive_types! { create_odd_number_table }

    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let odd_number_table = Arc::new(MemTable::new("odd_numbers", recordbatch));
    schema_provider
        .register_table(odd_number_table.table_name().to_string(), odd_number_table)
        .unwrap();

    // create table with floating numbers
    let column_schemas = vec![
        ColumnSchema::new("f32_number", ConcreteDataType::float32_datatype(), true),
        ColumnSchema::new("f64_number", ConcreteDataType::float64_datatype(), true),
    ];
    let f32_numbers: VectorRef = Arc::new(Float32Vector::from_vec(vec![1.0f32, 2.0, 3.0]));
    let f64_numbers: VectorRef = Arc::new(Float64Vector::from_vec(vec![1.0f64, 2.0, 3.0]));
    let columns = vec![f32_numbers, f64_numbers];
    let schema = Arc::new(Schema::new(column_schemas));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let float_number_table = Arc::new(MemTable::new("float_numbers", recordbatch));
    schema_provider
        .register_table(
            float_number_table.table_name().to_string(),
            float_number_table,
        )
        .unwrap();

    catalog_provider.register_schema(DEFAULT_SCHEMA_NAME.to_string(), schema_provider);
    catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider);

    let factory = QueryEngineFactory::new(catalog_list);
    factory.query_engine().clone()
}

async fn get_numbers_from_table<'s, T>(
    column_name: &'s str,
    table_name: &'s str,
    engine: Arc<dyn QueryEngine>,
) -> Vec<T>
where
    T: PrimitiveElement,
    for<'a> T: Scalar<RefType<'a> = T>,
{
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
    let column: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(&column) };
    column.iter_data().flatten().collect::<Vec<T>>()
}

#[tokio::test]
async fn test_median_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let engine = create_query_engine();

    test_median_failed::<f32>("f32_number", "float_numbers", engine.clone()).await?;
    test_median_failed::<f64>("f64_number", "float_numbers", engine.clone()).await?;

    macro_rules! test_median {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number_even", std::any::type_name::<$T>());
                test_median_success::<$T>(&column_name, "even_numbers", engine.clone()).await?;

                let column_name = format!("{}_number_odd", std::any::type_name::<$T>());
                test_median_success::<$T>(&column_name, "odd_numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_ordered_primitive_types! { test_median }
    Ok(())
}

async fn test_median_success<T>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: PrimitiveElement + Ord,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    let result = execute_median(column_name, table_name, engine.clone())
        .await
        .unwrap();
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

    let mut numbers = get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
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

async fn test_median_failed<T>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: PrimitiveElement,
{
    let result = execute_median(column_name, table_name, engine).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains(&format!(
        "Failed to create accumulator: \"MEDIAN\" aggregate function not support data type {}",
        T::type_name()
    )));
    Ok(())
}

async fn execute_median<'a>(
    column_name: &'a str,
    table_name: &'a str,
    engine: Arc<dyn QueryEngine>,
) -> RecordResult<Vec<RecordBatch>> {
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
