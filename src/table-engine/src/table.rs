#[cfg(test)]
pub mod test_util;

use std::any::Any;
use std::pin::Pin;

use async_trait::async_trait;
use common_query::logical_plan::Expr;
use common_recordbatch::error::{Error as RecordBatchError, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::logging;
use futures::task::{Context, Poll};
use futures::Stream;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::manifest::action::ProtocolAction;
use store_api::manifest::{self, Manifest, ManifestVersion, MetaActionIterator};
use store_api::storage::{
    ChunkReader, PutOperation, ReadContext, Region, ScanRequest, SchemaRef, Snapshot, WriteContext,
    WriteRequest,
};
use table::error::{Error as TableError, MissingColumnSnafu, Result as TableResult};
use table::requests::InsertRequest;
use table::{
    metadata::{TableInfo, TableType},
    table::Table,
};

use crate::error::{
    Result, ScanTableManifestSnafu, TableInfoNotFoundSnafu, UpdateTableManifestSnafu,
};
use crate::manifest::action::*;
use crate::manifest::TableManifest;

#[inline]
fn table_manifest_dir(table_name: &str) -> String {
    format!("{}/manifest/", table_name)
}

/// [Table] implementation.
pub struct MitoTable<R: Region> {
    _manifest: TableManifest,
    table_info: TableInfo,
    // TODO(dennis): a table contains multi regions
    region: R,
}

#[async_trait]
impl<R: Region> Table for MitoTable<R> {
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

        let mut write_request = self.region.write_request(self.schema());

        let mut put_op = write_request.put_op();
        let mut columns_values = request.columns_values;
        let key_columns = self.table_info.meta.row_key_column_names();
        let value_columns = self.table_info.meta.value_column_names();

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
        let read_ctx = ReadContext::default();
        let snapshot = self.region.snapshot(&read_ctx).map_err(TableError::new)?;

        let mut reader = snapshot
            .scan(&read_ctx, ScanRequest::default())
            .await
            .map_err(TableError::new)?
            .reader;

        let schema = reader.schema().clone();
        let stream_schema = schema.clone();

        let stream = Box::pin(async_stream::try_stream! {

            for chunk in reader.next_chunk()
                .await
                .map_err(RecordBatchError::new)?
            {
                yield RecordBatch::new(stream_schema.clone(), chunk.columns)?
            }
        });

        Ok(Box::pin(ChunkStream { schema, stream }))
    }
}

struct ChunkStream {
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = RecordBatchResult<RecordBatch>> + Send>>,
}

impl RecordBatchStream for ChunkStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ChunkStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(ctx)
    }
}

impl<R: Region> MitoTable<R> {
    fn new(table_info: TableInfo, region: R, manifest: TableManifest) -> Self {
        Self {
            table_info,
            region,
            _manifest: manifest,
        }
    }

    pub async fn create(
        table_name: &str,
        table_info: TableInfo,
        region: R,
        object_store: ObjectStore,
    ) -> Result<MitoTable<R>> {
        let manifest = TableManifest::new(&table_manifest_dir(table_name), object_store);

        // TODO(dennis): save  manifest version into catalog?
        let _manifest_version = manifest
            .update(TableMetaActionList::new(vec![
                TableMetaAction::Protocol(ProtocolAction::new()),
                TableMetaAction::Change(Box::new(TableChange {
                    table_info: table_info.clone(),
                })),
            ]))
            .await
            .context(UpdateTableManifestSnafu { table_name })?;

        Ok(MitoTable::new(table_info, region, manifest))
    }

    pub async fn open(
        table_name: &str,
        region: R,
        object_store: ObjectStore,
    ) -> Result<MitoTable<R>> {
        let manifest = TableManifest::new(&table_manifest_dir(table_name), object_store);

        let table_info = Self::recover_table_info(table_name, &manifest)
            .await?
            .context(TableInfoNotFoundSnafu { table_name })?;

        Ok(MitoTable::new(table_info, region, manifest))
    }

    async fn recover_table_info(
        table_name: &str,
        manifest: &TableManifest,
    ) -> Result<Option<TableInfo>> {
        let (start, end) = Self::manifest_scan_range();
        let mut iter = manifest
            .scan(start, end)
            .await
            .context(ScanTableManifestSnafu { table_name })?;

        let mut last_manifest_version = manifest::MIN_VERSION;
        let mut table_info = None;
        while let Some((manifest_version, action_list)) = iter
            .next_action()
            .await
            .context(ScanTableManifestSnafu { table_name })?
        {
            last_manifest_version = manifest_version;

            for action in action_list.actions {
                match action {
                    TableMetaAction::Change(c) => {
                        table_info = Some(c.table_info);
                    }
                    TableMetaAction::Protocol(_) => {}
                    _ => unimplemented!(),
                }
            }
        }

        if table_info.is_some() {
            // update manifest state after recovering
            let protocol = iter.last_protocol();
            manifest.update_state(last_manifest_version + 1, protocol.clone());
        }

        logging::debug!(
            "Recovered table info {:?} for table: {}",
            table_info,
            table_name
        );

        Ok(table_info)
    }

    #[inline]
    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn manifest_scan_range() -> (ManifestVersion, ManifestVersion) {
        // TODO(dennis): use manifest version in catalog ?
        (manifest::MIN_VERSION, manifest::MAX_VERSION)
    }
}
