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
