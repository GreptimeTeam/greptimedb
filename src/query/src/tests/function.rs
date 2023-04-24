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

use catalog::local::{MemoryCatalogManager, MemoryCatalogProvider, MemorySchemaProvider};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_recordbatch::RecordBatch;
use datatypes::for_all_primitive_types;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::types::WrapperType;
use datatypes::vectors::Helper;
use rand::Rng;
use table::test_util::MemTable;

use crate::tests::exec_selection;
use crate::{QueryEngine, QueryEngineFactory};

pub fn create_query_engine() -> Arc<dyn QueryEngine> {
    let schema_provider = Arc::new(MemorySchemaProvider::new());
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_list = Arc::new(MemoryCatalogManager::default());

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
                let column: VectorRef = Arc::new(<$T as Scalar>::VectorType::from_vec(numbers.to_vec()));
                columns.push(column);
            )*
        }
    }
    for_all_primitive_types! { create_number_table }

    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let number_table = Arc::new(MemTable::new("numbers", recordbatch));
    schema_provider
        .register_table_sync(number_table.table_name().to_string(), number_table)
        .unwrap();

    catalog_provider
        .register_schema_sync(DEFAULT_SCHEMA_NAME.to_string(), schema_provider)
        .unwrap();
    catalog_list
        .register_catalog_sync(DEFAULT_CATALOG_NAME.to_string(), catalog_provider)
        .unwrap();

    QueryEngineFactory::new(catalog_list).query_engine()
}

pub async fn get_numbers_from_table<'s, T>(
    column_name: &'s str,
    table_name: &'s str,
    engine: Arc<dyn QueryEngine>,
) -> Vec<T>
where
    T: WrapperType,
{
    let sql = format!("SELECT {column_name} FROM {table_name}");
    let numbers = exec_selection(engine, &sql).await;

    let column = numbers[0].column(0);
    let column: &<T as Scalar>::VectorType = unsafe { Helper::static_cast(column) };
    column.iter_data().flatten().collect::<Vec<T>>()
}

pub fn get_value_from_batches(column_name: &str, batches: Vec<RecordBatch>) -> Value {
    assert_eq!(1, batches.len());
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(1, batches[0].schema.num_columns());
    assert_eq!(column_name, batches[0].schema.column_schemas()[0].name);

    let batch = &batches[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(batch.column(0).len(), 1);
    let v = batch.column(0);
    assert_eq!(1, v.len());
    v.get(0)
}
