use std::any::Any;

use async_trait::async_trait;
use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use snafu::OptionExt;
use store_api::storage::SchemaRef;
use store_api::storage::{PutOperation, Region, WriteContext, WriteRequest};
use table::error::{Error as TableError, MissingColumnSnafu, Result as TableResult};
use table::requests::InsertRequest;
use table::{
    metadata::{TableInfo, TableType},
    table::Table,
};

/// [Table] implementation.
pub struct MitoTable<R: Region> {
    table_info: TableInfo,
    //TODO(dennis): a table contains multi regions
    region: R,
}

#[async_trait]
impl<R: Region + 'static> Table for MitoTable<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_info.meta.schema.clone()
    }

    async fn insert(&self, request: InsertRequest) -> TableResult<usize> {
        if request.columns_values.is_empty() {
            return Ok(0);
        }

        let mut write_request = R::WriteRequest::new(self.schema());

        //FIXME(boyan): we can only insert to demo table right now
        let mut put_op = <<R as Region>::WriteRequest as WriteRequest>::PutOp::new();
        let mut columns_values = request.columns_values;
        let key_columns = vec!["ts", "host"];
        let value_columns = vec!["cpu", "memory"];
        //Add row key and columns
        for name in key_columns {
            put_op
                .add_key_column(
                    name,
                    columns_values
                        .get(name)
                        .context(MissingColumnSnafu { name })?
                        .clone(),
                )
                .map_err(TableError::new)?;
        }
        // Add vaue columns
        let mut rows_num = 0;
        for name in value_columns {
            if let Some(v) = columns_values.remove(name) {
                rows_num = v.len();
                put_op.add_value_column(name, v).map_err(TableError::new)?;
            }
        }
        write_request.put(put_op).map_err(TableError::new)?;

        let _resp = self
            .region
            .write(&WriteContext::default(), write_request)
            .await
            .map_err(TableError::new)?;

        Ok(rows_num)
    }

    fn table_type(&self) -> TableType {
        self.table_info.table_type
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> TableResult<SendableRecordBatchStream> {
        unimplemented!();
    }
}

impl<R: Region> MitoTable<R> {
    pub fn new(table_info: TableInfo, region: R) -> Self {
        Self { table_info, region }
    }
}
