#[cfg(any(test, feature = "test"))]
pub mod test_util;

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_query::execution::ExecutionPlan;
use common_query::logical_plan::Expr;
use common_recordbatch::error::{Error as RecordBatchError, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream};
use common_telemetry::logging;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use datatypes::vectors::VectorRef;
use futures::task::{Context, Poll};
use futures::Stream;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::manifest::{self, Manifest, ManifestVersion, MetaActionIterator};
use store_api::storage::{
    AddColumn, AlterOperation, AlterRequest, ChunkReader, ColumnDescriptorBuilder, PutOperation,
    ReadContext, Region, RegionMeta, ScanRequest, SchemaRef, Snapshot, WriteContext, WriteRequest,
};
use table::error::{Error as TableError, MissingColumnSnafu, Result as TableResult};
use table::metadata::{FilterPushDownType, TableInfoRef, TableMetaBuilder};
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest, InsertRequest};
use table::table::scan::SimpleTableScan;
use table::{
    metadata::{TableInfo, TableType},
    table::Table,
};
use tokio::sync::Mutex;

use crate::error::{
    self, ColumnsNotExistSnafu, ProjectedColumnNotFoundSnafu, Result, ScanTableManifestSnafu,
    SchemaBuildSnafu, TableInfoNotFoundSnafu, UnsupportedDefaultConstraintSnafu,
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
    // guarded by `self.alter_lock`
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
        let schema = self.schema();
        let key_columns = table_info.meta.row_key_column_names();
        let value_columns = table_info.meta.value_column_names();
        // columns_values is not empty, it's safe to unwrap
        let rows_num = columns_values.values().next().unwrap().len();

        // Add row key columns
        for name in key_columns {
            let column_schema = schema
                .column_schema_by_name(name)
                .expect("column schema not found");

            let vector = match columns_values.remove(name) {
                Some(v) => v,
                None => Self::try_get_column_default_constraint_vector(column_schema, rows_num)?,
            };

            put_op
                .add_key_column(name, vector)
                .map_err(TableError::new)?;
        }

        // Add value columns
        for name in value_columns {
            let column_schema = schema
                .column_schema_by_name(name)
                .expect("column schema not found");

            let vector = match columns_values.remove(name) {
                Some(v) => v,
                None => Self::try_get_column_default_constraint_vector(column_schema, rows_num)?,
            };
            put_op
                .add_value_column(name, vector)
                .map_err(TableError::new)?;
        }

        ensure!(
            columns_values.is_empty(),
            ColumnsNotExistSnafu {
                table_name: &table_info.name,
                column_names: columns_values
                    .keys()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            }
        );

        logging::trace!(
            "Insert into table {} with put_op: {:?}",
            table_info.name,
            put_op
        );

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

    fn table_info(&self) -> TableInfoRef {
        self.table_info.load_full()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> TableResult<Arc<dyn ExecutionPlan>> {
        let read_ctx = ReadContext::default();
        let snapshot = self.region.snapshot(&read_ctx).map_err(TableError::new)?;

        let projection = self.transform_projection(&self.region, projection.clone())?;
        let filters = filters.into();
        let scan_request = ScanRequest {
            projection,
            filters,
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
            while let Some(chunk) = reader.next_chunk().await.map_err(RecordBatchError::new)? {
                yield RecordBatch::new(stream_schema.clone(), chunk.columns)?
            }
        });

        let stream = Box::pin(ChunkStream { schema, stream });
        Ok(Arc::new(SimpleTableScan::new(stream)))
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

        let (alter_op, table_schema, new_columns_num) = match &req.alter_kind {
            AlterKind::AddColumns {
                columns: new_columns,
            } => {
                let columns = new_columns
                    .iter()
                    .enumerate()
                    .map(|(i, add_column)| {
                        let new_column = &add_column.column_schema;

                        let desc = ColumnDescriptorBuilder::new(
                            table_meta.next_column_id + i as u32,
                            &new_column.name,
                            new_column.data_type.clone(),
                        )
                        .is_nullable(new_column.is_nullable())
                        .default_constraint(new_column.default_constraint().cloned())
                        .build()
                        .context(error::BuildColumnDescriptorSnafu {
                            table_name,
                            column_name: &new_column.name,
                        })?;

                        Ok(AddColumn {
                            desc,
                            is_key: add_column.is_key,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let alter_op = AlterOperation::AddColumns { columns };

                // TODO(yingwen): [alter] Better way to alter the schema struct. In fact the column order
                // in table schema could differ from the region schema, so we could just push this column
                // to the back of the schema (as last column).
                let table_schema = build_table_schema_with_new_columns(
                    table_name,
                    &table_meta.schema,
                    new_columns,
                )?;

                (alter_op, table_schema, new_columns.len() as u32)
            }
            // TODO(dennis): supports removing columns etc.
            _ => unimplemented!(),
        };

        let new_meta = TableMetaBuilder::default()
            .schema(table_schema.clone())
            .engine(&table_meta.engine)
            .next_column_id(table_meta.next_column_id + new_columns_num) // Bump next column id.
            .primary_key_indices(table_meta.primary_key_indices.clone())
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        let mut new_info = TableInfo::clone(&*table_info);
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;

        // FIXME(yingwen): [alter] Alter the region, now this is a temporary implementation.
        let region = self.region();
        let region_meta = region.in_memory_metadata();
        let alter_req = AlterRequest {
            operation: alter_op,
            version: region_meta.version(),
        };

        logging::debug!(
            "start altering region {} of table {}, with request {:?}",
            region.name(),
            table_name,
            alter_req,
        );
        region.alter(alter_req).await.map_err(TableError::new)?;

        // then alter table info
        logging::debug!(
            "start updating the manifest of table {} with new table info {:?}",
            table_name,
            new_info
        );
        self.manifest
            .update(TableMetaActionList::with_action(TableMetaAction::Change(
                Box::new(TableChange {
                    table_info: new_info.clone(),
                }),
            )))
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

    fn supports_filter_pushdown(&self, _filter: &Expr) -> table::error::Result<FilterPushDownType> {
        Ok(FilterPushDownType::Inexact)
    }
}

fn build_table_schema_with_new_columns(
    table_name: &str,
    table_schema: &SchemaRef,
    new_columns: &[AddColumnRequest],
) -> Result<SchemaRef> {
    let mut columns = table_schema.column_schemas().to_vec();

    for add_column in new_columns {
        let new_column = &add_column.column_schema;
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
        columns.push(new_column.clone());
    }

    // Right now we are not support adding the column
    // before or after some column, so just clone a new schema like this.
    // TODO(LFC): support adding column before or after some column
    let mut builder = SchemaBuilder::try_from_columns(columns)
        .context(SchemaBuildSnafu {
            msg: "Failed to convert column schemas into table schema",
        })?
        .version(table_schema.version() + 1);

    if let Some(index) = table_schema.timestamp_index() {
        builder = builder.timestamp_index(index);
    }
    for (k, v) in table_schema.arrow_schema().metadata.iter() {
        builder = builder.add_metadata(k, v);
    }
    let new_schema = Arc::new(builder.build().with_context(|_| error::SchemaBuildSnafu {
        msg: format!("cannot add new columns {:?}", new_columns),
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
            .update(TableMetaActionList::with_action(TableMetaAction::Change(
                Box::new(TableChange {
                    table_info: table_info.clone(),
                }),
            )))
            .await
            .context(UpdateTableManifestSnafu { table_name })?;

        Ok(MitoTable::new(table_info, region, manifest))
    }

    fn try_get_column_default_constraint_vector(
        column_schema: &ColumnSchema,
        rows_num: usize,
    ) -> TableResult<VectorRef> {
        // TODO(dennis): when we support altering schema, we should check the schemas difference between table and region
        let vector = column_schema
            .create_default_vector(rows_num)
            .context(UnsupportedDefaultConstraintSnafu)?
            .context(MissingColumnSnafu {
                name: &column_schema.name,
            })?;

        Ok(vector)
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
    use crate::table::test_util;

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

        let new_columns = vec![AddColumnRequest {
            column_schema: new_column,
            is_key: false,
        }];
        let result = build_table_schema_with_new_columns(table_name, table_schema, &new_columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Column host already exists in table demo"));

        let new_column = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_columns = vec![AddColumnRequest {
            column_schema: new_column.clone(),
            is_key: false,
        }];
        let new_schema =
            build_table_schema_with_new_columns(table_name, table_schema, &new_columns).unwrap();

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
