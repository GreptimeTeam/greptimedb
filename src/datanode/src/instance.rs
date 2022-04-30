use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;
use query::catalog::CatalogListRef;
use query::query_engine::{QueryEngineFactory, QueryEngineRef};

use crate::error::Result;

pub enum Output {
    AffectedRows(usize),
    RecordBatch(SendableRecordBatchStream),
}

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

    pub async fn execute_sql(&self, _sql: &str) -> Result<Output> {
        Ok(Output::AffectedRows(3))
    }
}
