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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_catalog::consts::IMMUTABLE_FILE_ENGINE;
use common_error::prelude::BoxedError;
use common_telemetry::{debug, logging};
use object_store::ObjectStore;
use snafu::ResultExt;
use table::engine::{table_dir, EngineContext, TableEngine, TableReference};
use table::error::TableOperationSnafu;
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder, TableType};
use table::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use table::{error as table_error, Result as TableResult, Table, TableRef};
use tokio::sync::Mutex;

use crate::engine::INIT_TABLE_VERSION;
use crate::error::{
    BuildTableInfoSnafu, BuildTableMetaSnafu, DropTableSnafu, Result, TableExistsSnafu,
};
use crate::manifest::immutable::ImmutableManifest;
use crate::table::immutable::{ImmutableFileTable, ImmutableFileTableRef};

///  [TableEngine] implementation.
#[derive(Clone)]
pub struct ImmutableFileTableEngine {
    inner: Arc<EngineInner>,
}

#[async_trait]
impl TableEngine for ImmutableFileTableEngine {
    fn name(&self) -> &str {
        IMMUTABLE_FILE_ENGINE
    }

    async fn create_table(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<TableRef> {
        self.inner
            .create_table(ctx, request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn open_table(
        &self,
        ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        self.inner
            .open_table(ctx, request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        _req: AlterTableRequest,
    ) -> TableResult<TableRef> {
        table_error::UnsupportedSnafu {
            operation: "ALTER TABLE",
        }
        .fail()
    }

    fn get_table(
        &self,
        _ctx: &EngineContext,
        table_ref: &TableReference,
    ) -> TableResult<Option<TableRef>> {
        Ok(self.inner.get_table(table_ref))
    }

    fn table_exists(&self, _ctx: &EngineContext, table_ref: &TableReference) -> bool {
        self.inner.get_table(table_ref).is_some()
    }

    async fn drop_table(
        &self,
        _ctx: &EngineContext,
        request: DropTableRequest,
    ) -> TableResult<bool> {
        self.inner
            .drop_table(request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn close(&self) -> TableResult<()> {
        self.inner.close().await
    }
}

struct EngineInner {
    /// All tables opened by the engine. Map key is formatted [TableReference].
    ///
    /// Writing to `tables` should also hold the `table_mutex`.
    tables: RwLock<HashMap<String, ImmutableFileTableRef>>,
    object_store: ObjectStore,

    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Mutex<()>,
}

impl EngineInner {
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        let CreateTableRequest {
            catalog_name,
            schema_name,
            table_name,
            create_if_not_exists,
            table_options,
            ..
        } = request;
        let table_ref = TableReference {
            catalog: &catalog_name,
            schema: &schema_name,
            table: &table_name,
        };

        if let Some(table) = self.get_table(&table_ref) {
            return if create_if_not_exists {
                Ok(table)
            } else {
                TableExistsSnafu { table_name }.fail()
            };
        }

        let table_id = request.id;
        let table_dir = table_dir(&catalog_name, &schema_name, table_id);

        let table_meta = TableMetaBuilder::new_external_table()
            .engine(IMMUTABLE_FILE_ENGINE)
            .options(table_options)
            .build()
            .context(BuildTableMetaSnafu {
                table_name: &table_name,
            })?;

        let table_info = TableInfoBuilder::new(&table_name, table_meta)
            .ident(table_id)
            .table_version(INIT_TABLE_VERSION)
            .table_type(TableType::Base)
            .catalog_name(catalog_name.to_string())
            .schema_name(schema_name.to_string())
            .desc(request.desc)
            .build()
            .context(BuildTableInfoSnafu {
                table_name: &table_name,
            })?;

        let table = Arc::new(
            ImmutableFileTable::create(
                &table_name,
                &table_dir,
                table_info,
                self.object_store.clone(),
            )
            .await?,
        );

        logging::info!(
            "Immutable file engine created table: {} in schema: {}, table_id: {}.",
            table_name,
            schema_name,
            table_id
        );

        self.tables
            .write()
            .unwrap()
            .insert(table_ref.to_string(), table.clone());

        Ok(table)
    }

    fn get_table(&self, table_ref: &TableReference) -> Option<TableRef> {
        self.tables
            .read()
            .unwrap()
            .get(&table_ref.to_string())
            .cloned()
            .map(|table| table as _)
    }

    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        let OpenTableRequest {
            catalog_name,
            schema_name,
            table_name,
            ..
        } = request;
        let table_ref = TableReference {
            catalog: &catalog_name,
            schema: &schema_name,
            table: &table_name,
        };

        if let Some(table) = self.get_table(&table_ref) {
            return Ok(Some(table));
        }

        let table = {
            let _lock = self.table_mutex.lock().await;
            // Checks again, read lock should be enough since we are guarded by the mutex.
            if let Some(table) = self.get_table(&table_ref) {
                return Ok(Some(table));
            }

            let table_id = request.table_id;
            let table_dir = table_dir(&catalog_name, &schema_name, table_id);

            let Some((manifest, table_info)) = self
                .recover_table_manifest_and_info(&table_name, &table_dir)
                .await.map_err(BoxedError::new)
                .context(TableOperationSnafu)? else { return Ok(None) };

            debug!(
                "Opening table {}, table info recovered: {:?}",
                table_id, table_info
            );

            let table = Arc::new(ImmutableFileTable::new(table_info, manifest));

            self.tables
                .write()
                .unwrap()
                .insert(table_ref.to_string(), table.clone());
            Some(table as _)
        };

        logging::info!(
            "Mito engine opened table: {} in schema: {}",
            table_name,
            schema_name
        );

        Ok(table)
    }

    async fn drop_table(&self, req: DropTableRequest) -> Result<bool> {
        let table_reference = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };

        let _lock = self.table_mutex.lock().await;

        if let Some(table) = self.get_table(&table_reference) {
            let table_id = table.table_info().ident.table_id;
            let table_dir = table_dir(&req.catalog_name, &req.schema_name, table_id);
            ImmutableManifest::delete(&table_dir, self.object_store.clone())
                .await
                .map_err(BoxedError::new)
                .with_context(|_| DropTableSnafu {
                    table_name: table_reference.to_string(),
                })?;
            self.tables
                .write()
                .unwrap()
                .remove(&table_reference.to_string());

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn close(&self) -> TableResult<()> {
        let _lock = self.table_mutex.lock().await;

        let tables = self.tables.write().unwrap().clone();

        futures::future::try_join_all(tables.values().map(|t| t.close()))
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        Ok(())
    }

    async fn recover_table_manifest_and_info(
        &self,
        table_name: &str,
        table_dir: &str,
    ) -> Result<Option<(ImmutableManifest, TableInfo)>> {
        let manifest = ImmutableManifest::new(table_dir, self.object_store.clone());

        let Some(table_info) =
        ImmutableFileTable::recover_table_info(table_name, &manifest)
                .await? else { return Ok(None) };

        Ok(Some((manifest, table_info)))
    }
}
