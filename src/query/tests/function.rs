use std::sync::Arc;

use catalog::local::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{
    CatalogList, CatalogProvider, SchemaProvider, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use datatypes::for_all_primitive_types;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::types::PrimitiveElement;
use datatypes::vectors::PrimitiveVector;
use query::query_engine::QueryEngineFactory;
use query::QueryEngine;
use rand::Rng;
use test_util::MemTable;

pub fn create_query_engine() -> Arc<dyn QueryEngine> {
    let schema_provider = Arc::new(MemorySchemaProvider::new());
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_list = Arc::new(MemoryCatalogList::default());

    let mut column_schemas = vec![];
    let mut columns = vec![];
    macro_rules! create_number_table {
        ([], $( { $T:ty } ),*) => {
            $(
                let mut rng = rand::thread_rng();

                let column_name = format!("{}_number", std::any::type_name::<$T>());
                let column_schema = ColumnSchema::new(column_name, Value::from(<$T>::default()).data_type(), true);
                column_schemas.push(column_schema);

                let numbers = (1..=10).map(|_| rng.gen::<$T>()).collect::<Vec<$T>>();
                let column: VectorRef = Arc::new(PrimitiveVector::<$T>::from_vec(numbers.to_vec()));
                columns.push(column);
            )*
        }
    }
    for_all_primitive_types! { create_number_table }

    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let number_table = Arc::new(MemTable::new("numbers", recordbatch));
    schema_provider
        .register_table(number_table.table_name().to_string(), number_table)
        .unwrap();

    catalog_provider
        .register_schema(DEFAULT_SCHEMA_NAME.to_string(), schema_provider)
        .unwrap();
    catalog_list
        .register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider)
        .unwrap();

    let factory = QueryEngineFactory::new(catalog_list);
    factory.query_engine().clone()
}

pub async fn get_numbers_from_table<'s, T>(
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
        Output::Stream(batch) => batch,
        _ => unreachable!(),
    };
    let numbers = util::collect(recordbatch_stream).await.unwrap();

    let columns = numbers[0].df_recordbatch.columns();
    let column = VectorHelper::try_into_vector(&columns[0]).unwrap();
    let column: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(&column) };
    column.iter_data().flatten().collect::<Vec<T>>()
}
