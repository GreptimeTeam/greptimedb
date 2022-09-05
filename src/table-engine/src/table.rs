#[cfg(any(test, feature = "test"))]
pub mod test_util;

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_query::logical_plan::Expr;
use common_recordbatch::error::{Error as RecordBatchError, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::logging;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use futures::task::{Context, Poll};
use futures::Stream;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::manifest::action::ProtocolAction;
use store_api::manifest::{self, Manifest, ManifestVersion, MetaActionIterator};
use store_api::storage::RegionDescriptorBuilder;
use store_api::storage::{
    ChunkReader, PutOperation, ReadContext, Region, RegionMeta, ScanRequest, SchemaRef, Snapshot,
    WriteContext, WriteRequest,
};
use table::error::{Error as TableError, MissingColumnSnafu, Result as TableResult};
use table::metadata::{TableInfoRef, TableMetaBuilder};
use table::requests::{AlterKind, AlterTableRequest, InsertRequest};
use table::{
    metadata::{TableInfo, TableType},
    table::Table,
};
use tokio::sync::Mutex;

use crate::engine::{build_column_family, build_row_key_desc, INIT_COLUMN_ID};
use crate::error::{
    self, ProjectedColumnNotFoundSnafu, Result, ScanTableManifestSnafu, TableInfoNotFoundSnafu,
    UpdateTableManifestSnafu,
};
use crate::manifest::action::*;
use crate::manifest::TableManifest;

#[inline]
fn table_manifest_dir(table_name: &str) -> String {
    format!("{}/manifest/", table_name)
}

/// [Table] implementation.
pub struct MitoTable<R: Region> {
    manifest: TableManifest,
    table_info: ArcSwap<TableInfo>,
    // TODO(dennis): a table contains multi regions
    region: R,
    alter_lock: Mutex<()>,
}

#[async_trait]
impl<R: Region> Table for MitoTable<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_info().meta.schema.clone()
    }

    async fn insert(&self, request: InsertRequest) -> TableResult<usize> {
        if request.columns_values.is_empty() {
            return Ok(0);
        }

        let mut write_request = self.region.write_request();

        let mut put_op = write_request.put_op();
        let mut columns_values = request.columns_values;

        let table_info = self.table_info();
        let key_columns = table_info.meta.row_key_column_names();
        let value_columns = table_info.meta.value_column_names();

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
        self.table_info().table_type
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> TableResult<SendableRecordBatchStream> {
        let read_ctx = ReadContext::default();
        let snapshot = self.region.snapshot(&read_ctx).map_err(TableError::new)?;

        let projection = self.transform_projection(&self.region, projection.clone())?;

        let scan_request = ScanRequest {
            projection,
            ..Default::default()
        };
        let mut reader = snapshot
            .scan(&read_ctx, scan_request)
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

    // Alter table changes the schemas of the table. The altering happens as cloning a new schema,
    // change the new one, and swap the old. Though we can change the schema in place, considering
    // the complex interwinding of inner data representation of schema, I think it's safer to
    // change it like this to avoid partial inconsistent during the altering. For example, schema's
    // `name_to_index` field must changed with `column_schemas` synchronously. If we add or remove
    // columns from `column_schemas` *and then* update the `name_to_index`, there's a slightly time
    // window of an inconsistency of the two field, which might bring some hard to trace down
    // concurrency related bugs or failures. (Of course we could introduce some guards like readwrite
    // lock to protect the consistency of schema altering, but that would hurt the performance of
    // schema reads, and the reads are the dominant operation of schema. At last, altering is
    // performed far lesser frequent.)
    async fn alter(&self, req: AlterTableRequest) -> TableResult<()> {
        let _lock = self.alter_lock.lock().await;

        let table_info = self.table_info();
        let table_name = &table_info.name;
        let table_meta = &table_info.meta;
        let table_schema = match &req.alter_kind {
            AlterKind::AddColumn { new_column } => {
                build_table_schema_with_new_column(table_name, &table_meta.schema, new_column)?
            }
        };

        let primary_key_indices = &table_meta.primary_key_indices;
        let (next_column_id, default_cf) = build_column_family(
            INIT_COLUMN_ID,
            table_name,
            &table_schema,
            primary_key_indices,
        )?;
        let (next_column_id, row_key) = build_row_key_desc(
            next_column_id,
            table_name,
            &table_schema,
            primary_key_indices,
        )?;

        let new_meta = TableMetaBuilder::default()
            .schema(table_schema.clone())
            .engine(&table_meta.engine)
            .next_column_id(next_column_id)
            .primary_key_indices(primary_key_indices.clone())
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        let mut new_info = TableInfo::clone(&*table_info);
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;

        // first alter region
        let region = self.region();
        let region_descriptor = RegionDescriptorBuilder::default()
            .id(region.id())
            .name(region.name())
            .row_key(row_key)
            .default_cf(default_cf)
            .build()
            .context(error::BuildRegionDescriptorSnafu {
                table_name,
                region_name: region.name(),
            })?;
        region.alter(region_descriptor).map_err(TableError::new)?;

        // then alter table info
        self.manifest
            .update(TableMetaActionList::new(vec![
                TableMetaAction::Protocol(ProtocolAction::new()),
                TableMetaAction::Change(Box::new(TableChange {
                    table_info: new_info.clone(),
                })),
            ]))
            .await
            .context(UpdateTableManifestSnafu {
                table_name: &self.table_info().name,
            })?;
        self.set_table_info(new_info);

        // TODO(LFC): Think of a way to properly handle the metadata integrity between region and table.
        // Currently there are no "transactions" to alter the metadata of region and table together,
        // they are altered in sequence. That means there might be cases where the metadata of region
        // is altered while the table's is not. Then the metadata integrity between region and
        // table cannot be hold.
        Ok(())
    }
}

fn build_table_schema_with_new_column(
    table_name: &str,
    table_schema: &SchemaRef,
    new_column: &ColumnSchema,
) -> Result<SchemaRef> {
    if table_schema
        .column_schema_by_name(&new_column.name)
        .is_some()
    {
        return error::ColumnExistsSnafu {
            column_name: &new_column.name,
            table_name,
        }
        .fail()?;
    }

    let mut columns = table_schema.column_schemas().to_vec();
    columns.push(new_column.clone());

    // Right now we are not support adding the column
    // before or after some column, so just clone a new schema like this.
    // TODO(LFC): support adding column before or after some column
    let mut builder = SchemaBuilder::from_columns(columns).version(table_schema.version() + 1);

    if let Some(index) = table_schema.timestamp_index() {
        builder = builder.timestamp_index(index);
    }
    for (k, v) in table_schema.arrow_schema().metadata.iter() {
        builder = builder.add_metadata(k, v);
    }
    let new_schema = Arc::new(builder.build().with_context(|_| error::SchemaBuildSnafu {
        msg: format!("cannot add new column {:?}", new_column),
    })?);
    Ok(new_schema)
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

#[inline]
fn column_qualified_name(table_name: &str, region_name: &str, column_name: &str) -> String {
    format!("{}.{}.{}", table_name, region_name, column_name)
}

impl<R: Region> MitoTable<R> {
    fn new(table_info: TableInfo, region: R, manifest: TableManifest) -> Self {
        Self {
            table_info: ArcSwap::new(Arc::new(table_info)),
            region,
            manifest,
            alter_lock: Mutex::new(()),
        }
    }

    /// Transform projection which is based on table schema
    /// into projection based on region schema.
    fn transform_projection(
        &self,
        region: &R,
        projection: Option<Vec<usize>>,
    ) -> Result<Option<Vec<usize>>> {
        let table_info = self.table_info();
        let table_schema = &table_info.meta.schema;

        let region_meta = region.in_memory_metadata();
        let region_schema = region_meta.schema();

        if projection.is_none() {
            // In fact, datafusion always calls scan with not-none projection
            // generated by table schema right now, but to prevent future compatibility
            // issue, we process this case here.
            let projection: Result<Vec<_>> = table_schema
                .column_schemas()
                .iter()
                .map(|column_schema| &column_schema.name)
                .map(|name| {
                    region_schema.column_index_by_name(name).with_context(|| {
                        ProjectedColumnNotFoundSnafu {
                            column_qualified_name: column_qualified_name(
                                &table_info.name,
                                region.name(),
                                name,
                            ),
                        }
                    })
                })
                .collect();
            return Some(projection).transpose();
        }

        projection
            .map(|p| {
                p.iter()
                    .map(|idx| table_schema.column_name_by_index(*idx))
                    .map(|name| {
                        region_schema.column_index_by_name(name).with_context(|| {
                            ProjectedColumnNotFoundSnafu {
                                column_qualified_name: column_qualified_name(
                                    &table_info.name,
                                    region.name(),
                                    name,
                                ),
                            }
                        })
                    })
                    .collect()
            })
            .transpose()
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
    pub fn region(&self) -> &R {
        &self.region
    }

    #[inline]
    pub fn table_info(&self) -> TableInfoRef {
        Arc::clone(&self.table_info.load())
    }

    pub fn set_table_info(&self, table_info: TableInfo) {
        self.table_info.swap(Arc::new(table_info));
    }

    #[inline]
    pub fn manifest(&self) -> &TableManifest {
        &self.manifest
    }

    fn manifest_scan_range() -> (ManifestVersion, ManifestVersion) {
        // TODO(dennis): use manifest version in catalog ?
        (manifest::MIN_VERSION, manifest::MAX_VERSION)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;

    use super::*;

    #[test]
    fn test_table_manifest_dir() {
        assert_eq!("demo/manifest/", table_manifest_dir("demo"));
        assert_eq!("numbers/manifest/", table_manifest_dir("numbers"));
    }

    #[test]
    fn test_build_table_schema_with_new_column() {
        let table_info = test_util::build_test_table_info();
        let table_name = &table_info.name;
        let table_meta = &table_info.meta;
        let table_schema = &table_meta.schema;

        let new_column = ColumnSchema::new("host", ConcreteDataType::string_datatype(), true);
        let result = build_table_schema_with_new_column(table_name, table_schema, &new_column);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Column host already exists in table demo"));

        let new_column = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_schema =
            build_table_schema_with_new_column(table_name, table_schema, &new_column).unwrap();

        assert_eq!(new_schema.num_columns(), table_schema.num_columns() + 1);
        assert_eq!(
            new_schema.column_schemas().split_last().unwrap(),
            (&new_column, table_schema.column_schemas())
        );

        assert_eq!(
            new_schema.timestamp_column(),
            table_schema.timestamp_column()
        );
        assert_eq!(new_schema.version(), table_schema.version() + 1);
    }
}
