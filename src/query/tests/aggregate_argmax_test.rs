mod testing_table;

use std::sync::Arc;

use common_recordbatch::error::Result as RecordResult;
use common_recordbatch::{util, RecordBatch};
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datatypes::for_all_ordered_primitive_types;
use datatypes::prelude::*;
use datatypes::types::DataTypeBuilder;
use datatypes::vectors::PrimitiveVector;
use num::NumCast;
use query::catalog::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use query::catalog::schema::SchemaProvider;
use query::catalog::{CatalogList, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use query::error::Result;
use query::query_engine::{Output, QueryEngineFactory};
use query::QueryEngine;
use rand::Rng;

use crate::testing_table::TestingTable;

#[tokio::test]
async fn test_argmax_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let engine = create_query_engine();
    let table_name = "f32_number".to_string();
    test_argmax_success::<f32>(&table_name, engine.clone()).await?;

    let table_name = "f64_number".to_string();
    test_argmax_success::<f64>(&table_name, engine.clone()).await?;
    macro_rules! test_argmax {
        ([], $( { $T:ty } ),*) => {
            $(
                let table_name = format!("{}_number", std::any::type_name::<$T>());
                test_argmax_success::<$T>(&table_name, engine.clone()).await?;
            )*
        }
    }
    for_all_ordered_primitive_types! { test_argmax }
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

                let table_name = format!("{}_number", std::any::type_name::<$T>());
                let column_name = table_name.clone();
                let numbers = (1..=100).map(|_| rng.gen_range(<$T>::MIN..<$T>::MAX)).collect::<Vec<$T>>();
                let table = Arc::new(TestingTable::new(
                    &column_name,
                    Arc::new(PrimitiveVector::<$T>::from_vec(numbers.to_vec())),
                ));
                schema_provider.register_table(table_name, table).unwrap();
            )*
        }
    }

    macro_rules! create_testing_table_float {
        ($T:ty) => {
            let mut rng = rand::thread_rng();

            let table_name = format!("{}_number", std::any::type_name::<$T>());
            let column_name = table_name.clone();
            let numbers = (1..=100).map(|_| rng.gen::<$T>()).collect::<Vec<$T>>();
            let table = Arc::new(TestingTable::new(
                &column_name,
                Arc::new(PrimitiveVector::<$T>::from_vec(numbers.to_vec())),
            ));
            schema_provider.register_table(table_name, table).unwrap();
        };
    }
    for_all_ordered_primitive_types! { create_testing_table }
    create_testing_table_float!(f32);
    create_testing_table_float!(f64);

    catalog_provider.register_schema(DEFAULT_SCHEMA_NAME, schema_provider);
    catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider);

    let factory = QueryEngineFactory::new(catalog_list);
    factory.query_engine().clone()
}

async fn get_numbers_from_table<T>(table_name: &str, engine: Arc<dyn QueryEngine>) -> Vec<T>
where
    T: Primitive + DataTypeBuilder,
    for<'a> T: Scalar<RefType<'a> = T>,
{
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
    let column: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(&column) };
    column.iter_data().flatten().collect::<Vec<T>>()
}

async fn test_argmax_success<T>(table_name: &str, engine: Arc<dyn QueryEngine>) -> Result<()>
where
    T: Primitive + PartialOrd + DataTypeBuilder,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    let result = execute_argmax(table_name, engine.clone()).await.unwrap();
    // 我在执行的时候发现了不匹配？
    assert_eq!(1, result.len());
    assert_eq!(result[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, result[0].schema.arrow_schema().fields().len());
    assert_eq!("argmax", result[0].schema.arrow_schema().field(0).name());

    let columns = result[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), 1);
    let v = VectorHelper::try_into_vector(&columns[0]).unwrap();
    assert_eq!(1, v.len());
    let argmax = v.get(0);

    let numbers = get_numbers_from_table::<T>(table_name, engine.clone()).await;
    let len = numbers.len();
    let expected_argmax: Value = if len % 2 == 1 {
        numbers[len / 2]
    } else {
        let a: f64 = NumCast::from(numbers[len / 2 - 1]).unwrap();
        let b: f64 = NumCast::from(numbers[len / 2]).unwrap();
        NumCast::from(a / 2.0 + b / 2.0).unwrap()
    }
    .into();
    assert_eq!(expected_argmax, argmax);
    Ok(())
}

async fn execute_argmax(
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> RecordResult<Vec<RecordBatch>> {
    let column_name = table_name;
    let sql = format!(
        "select ARGMAX({}) as argmax from {}",
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
