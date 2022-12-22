// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(any(test, feature = "test"))]
pub mod test_util;

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::logical_plan::Expr;
use common_query::physical_plan::PhysicalPlanRef;
use common_recordbatch::error::{ExternalSnafu, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream};
use common_telemetry::logging;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::VectorRef;
use futures::task::{Context, Poll};
use futures::Stream;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::manifest::{self, Manifest, ManifestVersion, MetaActionIterator};
use store_api::storage::{
    AddColumn, AlterOperation, AlterRequest, ChunkReader, PutOperation, ReadContext, Region,
    RegionMeta, ScanRequest, SchemaRef, Snapshot, WriteContext, WriteRequest,
};
use table::error::{Error as TableError, MissingColumnSnafu, Result as TableResult};
use table::metadata::{
    FilterPushDownType, RawTableInfo, TableInfo, TableInfoRef, TableMeta, TableType,
};
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest, InsertRequest};
use table::table::scan::SimpleTableScan;
use table::table::Table;
use tokio::sync::Mutex;

use crate::error::{
    self, ColumnsNotExistSnafu, ProjectedColumnNotFoundSnafu, Result, ScanTableManifestSnafu,
    TableInfoNotFoundSnafu, UnsupportedDefaultConstraintSnafu, UpdateTableManifestSnafu,
};
use crate::manifest::action::*;
use crate::manifest::TableManifest;

#[inline]
fn table_manifest_dir(table_dir: &str) -> String {
    format!("{table_dir}/manifest/")
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> TableResult<PhysicalPlanRef> {
        let read_ctx = ReadContext::default();
        let snapshot = self.region.snapshot(&read_ctx).map_err(TableError::new)?;

        let projection = self.transform_projection(&self.region, projection.cloned())?;
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
            while let Some(chunk) = reader.next_chunk().await.map_err(BoxedError::new).context(ExternalSnafu)? {
                yield RecordBatch::new(stream_schema.clone(), chunk.columns)?
            }
        });

        let stream = Box::pin(ChunkStream { schema, stream });
        Ok(Arc::new(SimpleTableScan::new(stream)))
    }

    /// Alter table changes the schemas of the table.
    async fn alter(&self, req: AlterTableRequest) -> TableResult<()> {
        let _lock = self.alter_lock.lock().await;

        let table_info = self.table_info();
        let table_name = &table_info.name;
        let table_meta = &table_info.meta;
        let mut new_meta = table_meta
            .builder_with_alter_kind(table_name, &req.alter_kind)?
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        let alter_op = create_alter_operation(table_name, &req.alter_kind, &mut new_meta)?;

        let mut new_info = TableInfo::clone(&*table_info);
        // Increase version of the table.
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;

        // Persist the alteration to the manifest.
        logging::debug!(
            "start updating the manifest of table {} with new table info {:?}",
            table_name,
            new_info
        );
        self.manifest
            .update(TableMetaActionList::with_action(TableMetaAction::Change(
                Box::new(TableChange {
                    table_info: RawTableInfo::from(new_info.clone()),
                }),
            )))
            .await
            .context(UpdateTableManifestSnafu {
                table_name: &self.table_info().name,
            })?;

        // TODO(yingwen): Error handling. Maybe the region need to provide a method to
        // validate the request first.
        let region = self.region();
        let region_meta = region.in_memory_metadata();
        let alter_req = AlterRequest {
            operation: alter_op,
            version: region_meta.version(),
        };
        // Alter the region.
        logging::debug!(
            "start altering region {} of table {}, with request {:?}",
            region.name(),
            table_name,
            alter_req,
        );
        region.alter(alter_req).await.map_err(TableError::new)?;

        // Update in memory metadata of the table.
        self.set_table_info(new_info);

        Ok(())
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> table::error::Result<FilterPushDownType> {
        Ok(FilterPushDownType::Inexact)
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

#[inline]
fn column_qualified_name(table_name: &str, region_name: &str, column_name: &str) -> String {
    format!("{table_name}.{region_name}.{column_name}")
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
        table_dir: &str,
        table_info: TableInfo,
        region: R,
        object_store: ObjectStore,
    ) -> Result<MitoTable<R>> {
        let manifest = TableManifest::new(&table_manifest_dir(table_dir), object_store);

        // TODO(dennis): save manifest version into catalog?
        let _manifest_version = manifest
            .update(TableMetaActionList::with_action(TableMetaAction::Change(
                Box::new(TableChange {
                    table_info: RawTableInfo::from(table_info.clone()),
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
        table_dir: &str,
        region: R,
        object_store: ObjectStore,
    ) -> Result<MitoTable<R>> {
        let manifest = TableManifest::new(&table_manifest_dir(table_dir), object_store);

        let mut table_info = Self::recover_table_info(table_name, &manifest)
            .await?
            .context(TableInfoNotFoundSnafu { table_name })?;
        table_info.meta.region_numbers = vec![(region.id() & 0xFFFFFFFF) as u32];
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
                        table_info = Some(
                            TableInfo::try_from(c.table_info).context(error::ConvertRawSnafu)?,
                        );
                    }
                    TableMetaAction::Protocol(_) => {}
                    TableMetaAction::Remove(_) => unimplemented!("Drop table is unimplemented"),
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

/// Create [`AlterOperation`] according to given `alter_kind`.
fn create_alter_operation(
    table_name: &str,
    alter_kind: &AlterKind,
    table_meta: &mut TableMeta,
) -> TableResult<AlterOperation> {
    match alter_kind {
        AlterKind::AddColumns { columns } => {
            create_add_columns_operation(table_name, columns, table_meta)
        }
        AlterKind::DropColumns { names } => Ok(AlterOperation::DropColumns {
            names: names.to_vec(),
        }),
        // No need to build alter operation when reaming tables.
        AlterKind::RenameTable { .. } => unreachable!(),
    }
}

fn create_add_columns_operation(
    table_name: &str,
    requests: &[AddColumnRequest],
    table_meta: &mut TableMeta,
) -> TableResult<AlterOperation> {
    let columns = requests
        .iter()
        .map(|request| {
            let new_column = &request.column_schema;
            let desc = table_meta.alloc_new_column(table_name, new_column)?;

            Ok(AddColumn {
                desc,
                is_key: request.is_key,
            })
        })
        .collect::<TableResult<Vec<_>>>()?;

    Ok(AlterOperation::AddColumns { columns })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_manifest_dir() {
        assert_eq!("demo/manifest/", table_manifest_dir("demo"));
        assert_eq!("numbers/manifest/", table_manifest_dir("numbers"));
    }
}
