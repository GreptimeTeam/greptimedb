use std::sync::Arc;

use query::catalog::CatalogListRef;
use query::query_engine::{Output, QueryEngineFactory, QueryEngineRef};
use snafu::ResultExt;

use crate::error::{QuerySnafu, Result};

// An abstraction to read/write services.
pub struct Instance {
    // Query service
    query_engine: QueryEngineRef,
    // Catalog list
    _catalog_list: CatalogListRef,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub fn new(catalog_list: CatalogListRef) -> Self {
        let factory = QueryEngineFactory::new(catalog_list.clone());
        let query_engine = factory.query_engine().clone();
        Self {
            query_engine,
            _catalog_list: catalog_list,
        }
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Output> {
        let logical_plan = self.query_engine.sql_to_plan(sql).context(QuerySnafu)?;

        self.query_engine
            .execute(&logical_plan)
            .await
            .context(QuerySnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query::catalog::memory;
    use common_recordbatch::util;
    use arrow::array::UInt64Array;

    #[tokio::test]
    async fn test_execute_sql() {
        let catalog_list = memory::new_memory_catalog_list().unwrap();

        let instance = Instance::new(catalog_list);

        let output = instance.execute_sql("select sum(number) from numbers limit 20").await.unwrap();

  match output {
            Output::RecordBatch(recordbatch) => {
                let numbers = util::collect(recordbatch).await.unwrap();
                let columns = numbers[0].df_recordbatch.columns();
                assert_eq!(1, columns.len());
                assert_eq!(columns[0].len(), 1);

                assert_eq!(
                    *columns[0].as_any().downcast_ref::<UInt64Array>().unwrap(),
                    UInt64Array::from_slice(&[4950])
                );
            },
            _ => unreachable!(),
        }
    }
}
