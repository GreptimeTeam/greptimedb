use std::sync::Arc;

use async_trait::async_trait;
use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};

use crate::metadata::TableInfoBuilder;
use crate::metadata::TableInfoRef;
use crate::requests::InsertRequest;
use crate::Result;
use crate::{
    metadata::{TableMetaBuilder, TableType},
    requests::CreateTableRequest,
    Table,
};

pub struct EmptyTable {
    info: TableInfoRef,
}

impl EmptyTable {
    pub fn new(req: CreateTableRequest) -> Self {
        let table_meta = TableMetaBuilder::default()
            .schema(req.schema)
            .primary_key_indices(req.primary_key_indices)
            .next_column_id(0)
            .options(req.table_options)
            .build();
        let table_info = TableInfoBuilder::default()
            .catalog_name(req.catalog_name)
            .schema_name(req.schema_name)
            .name(req.table_name)
            .meta(table_meta.unwrap())
            .table_type(TableType::Temporary)
            .desc(req.desc)
            .build()
            .unwrap();

        Self {
            info: Arc::new(table_info),
        }
    }
}

#[async_trait]
impl Table for EmptyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self as _
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.info.meta.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        self.info.clone()
    }

    async fn insert(&self, _request: InsertRequest) -> Result<usize> {
        Ok(0)
    }

    async fn scan(
        &self,
        _partition: usize,
        _projection: &Option<Vec<usize>>,
        _filters: &[common_query::prelude::Expr],
        _limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())))
    }
}
