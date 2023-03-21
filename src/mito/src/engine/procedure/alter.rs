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

use std::sync::Arc;

use async_trait::async_trait;
use common_procedure::error::{Error, FromJsonSnafu, ToJsonSnafu};
use common_procedure::{Context, LockKey, Procedure, ProcedureManager, Result, Status};
use common_telemetry::logging;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::manifest::Manifest;
use store_api::storage::{AlterRequest, Region, RegionMeta, StorageEngine};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableInfo, TableVersion};
use table::requests::{AlterKind, AlterTableRequest};
use table::Table;

use crate::engine::MitoEngineInner;
use crate::error::{
    BuildTableMetaSnafu, TableNotFoundSnafu, UpdateTableManifestSnafu, VersionChangedSnafu,
};
use crate::manifest::action::{TableChange, TableMetaAction, TableMetaActionList};
use crate::table::{create_alter_operation, MitoTable};

/// Procedure to alter a [MitoTable].
pub(crate) struct AlterMitoTable<S: StorageEngine> {
    data: AlterTableData,
    engine_inner: Arc<MitoEngineInner<S>>,
    table: Arc<MitoTable<S::Region>>,
    /// The table info after alteration.
    new_info: Option<TableInfo>,
}

#[async_trait]
impl<S: StorageEngine> Procedure for AlterMitoTable<S> {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            AlterTableState::Prepare => self.on_prepare(),
            AlterTableState::AlterRegions => self.on_alter_regions().await,
            AlterTableState::UpdateTableManifest => self.on_update_table_manifest().await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(ToJsonSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = self.data.table_ref();
        let info = self.table.table_info();
        let mut keys: Vec<_> = info
            .meta
            .region_numbers
            .iter()
            .map(|number| format!("{table_ref}/region-{number}"))
            .collect();
        // If alter kind is rename, we also need to lock the region with another name.
        if let AlterKind::RenameTable { new_table_name } = &self.data.request.alter_kind {
            let new_table_ref = TableReference {
                catalog: &self.data.request.catalog_name,
                schema: &self.data.request.schema_name,
                table: new_table_name,
            };
            // We only acquire the first region.
            keys.push(format!("{new_table_ref}/region-0"));
        }
        LockKey::new(keys)
    }
}

impl<S: StorageEngine> AlterMitoTable<S> {
    const TYPE_NAME: &str = "mito::AlterMitoTable";

    /// Returns a new [AlterMitoTable].
    pub(crate) fn new(
        request: AlterTableRequest,
        engine_inner: Arc<MitoEngineInner<S>>,
    ) -> Result<Self> {
        let mut data = AlterTableData {
            state: AlterTableState::Prepare,
            request,
            // We set table version later.
            table_version: 0,
        };
        let table_ref = data.table_ref();
        let table =
            engine_inner
                .get_mito_table(&table_ref)
                .with_context(|| TableNotFoundSnafu {
                    table_name: table_ref.to_string(),
                })?;
        let info = table.table_info();
        data.table_version = info.ident.version;

        Ok(AlterMitoTable {
            data,
            engine_inner,
            table,
            new_info: None,
        })
    }

    /// Register the loader of this procedure to the `procedure_manager`.
    ///
    /// # Panics
    /// Panics on error.
    pub(crate) fn register_loader(
        engine_inner: Arc<MitoEngineInner<S>>,
        procedure_manager: &dyn ProcedureManager,
    ) {
        procedure_manager
            .register_loader(
                Self::TYPE_NAME,
                Box::new(move |data| {
                    Self::from_json(data, engine_inner.clone()).map(|p| Box::new(p) as _)
                }),
            )
            .unwrap()
    }

    /// Recover the procedure from json.
    fn from_json(json: &str, engine_inner: Arc<MitoEngineInner<S>>) -> Result<Self> {
        let data: AlterTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let table_ref = data.table_ref();
        let table =
            engine_inner
                .get_mito_table(&table_ref)
                .with_context(|| TableNotFoundSnafu {
                    table_name: table_ref.to_string(),
                })?;

        Ok(AlterMitoTable {
            data,
            engine_inner,
            table,
            new_info: None,
        })
    }

    /// Prepare table info.
    fn on_prepare(&mut self) -> Result<Status> {
        let current_info = self.table.table_info();
        ensure!(
            current_info.ident.version == self.data.table_version,
            VersionChangedSnafu {
                expect: self.data.table_version,
                actual: current_info.ident.version,
            }
        );

        self.init_new_info(&current_info)?;
        self.data.state = AlterTableState::AlterRegions;

        Ok(Status::executing(true))
    }

    /// Alter regions.
    async fn on_alter_regions(&mut self) -> Result<Status> {
        let current_info = self.table.table_info();
        ensure!(
            current_info.ident.version == self.data.table_version,
            VersionChangedSnafu {
                expect: self.data.table_version,
                actual: current_info.ident.version,
            }
        );

        self.init_new_info(&current_info)?;
        let new_info = self.new_info.as_mut().unwrap();
        let table_name = &self.data.request.table_name;

        let Some(alter_op) = create_alter_operation(table_name, &self.data.request.alter_kind, &mut new_info.meta)
            .map_err(Error::from_error_ext)? else {
                return Ok(Status::executing(true));
            };

        if let Some(alter_op) = create_alter_operation(
            table_name,
            &self.data.request.alter_kind,
            &mut new_info.meta,
        )
        .map_err(Error::from_error_ext)?
        {
            let regions = self.table.regions();
            // For each region, alter it if its version is not updated.
            for region in regions.values() {
                let region_meta = region.in_memory_metadata();
                if u64::from(region_meta.version()) > self.data.table_version {
                    // Region is already altered.
                    continue;
                }

                let alter_req = AlterRequest {
                    operation: alter_op.clone(),
                    version: region_meta.version(),
                };
                // Alter the region.
                logging::debug!(
                    "start altering region {} of table {}, with request {:?}",
                    region.name(),
                    table_name,
                    alter_req,
                );
                region
                    .alter(alter_req)
                    .await
                    .map_err(Error::from_error_ext)?;
            }
        }

        Ok(Status::executing(true))
    }

    /// Persist the alteration to the manifest and update table info.
    async fn on_update_table_manifest(&mut self) -> Result<Status> {
        // Get current table info.
        let current_info = self.table.table_info();
        if current_info.ident.version > self.data.table_version {
            logging::info!(
                "table {} version is already updated, current: {}, old_version: {}",
                self.data.request.table_name,
                current_info.ident.version,
                self.data.table_version,
            );
            return Ok(Status::Done);
        }

        self.init_new_info(&current_info)?;
        let new_info = self.new_info.as_ref().unwrap();
        let table_name = &self.data.request.table_name;

        logging::debug!(
            "start updating the manifest of table {} with new table info {:?}",
            table_name,
            new_info
        );

        self.table
            .manifest()
            .update(TableMetaActionList::with_action(TableMetaAction::Change(
                Box::new(TableChange {
                    table_info: RawTableInfo::from(new_info.clone()),
                }),
            )))
            .await
            .context(UpdateTableManifestSnafu { table_name })?;

        // Update in memory metadata of the table.
        self.table.set_table_info(new_info.clone());

        Ok(Status::Done)
    }

    fn init_new_info(&mut self, current_info: &TableInfo) -> Result<()> {
        if self.new_info.is_some() {
            return Ok(());
        }

        let table_name = &current_info.name;
        let mut new_info = TableInfo::clone(&*current_info);
        // setup new table info
        match &self.data.request.alter_kind {
            AlterKind::RenameTable { new_table_name } => {
                new_info.name = new_table_name.clone();
            }
            AlterKind::AddColumns { .. } | AlterKind::DropColumns { .. } => {
                let table_meta = &current_info.meta;
                let new_meta = table_meta
                    .builder_with_alter_kind(table_name, &self.data.request.alter_kind)
                    .map_err(Error::from_error_ext)?
                    .build()
                    .context(BuildTableMetaSnafu { table_name })?;
                new_info.meta = new_meta;
            }
        }
        // Increase version of the table.
        new_info.ident.version = current_info.ident.version + 1;

        self.new_info = Some(new_info);

        Ok(())
    }
}

/// Represents each step while altering table in the mito engine.
#[derive(Debug, Serialize, Deserialize)]
enum AlterTableState {
    /// Prepare to alter table.
    Prepare,
    /// Alter regions.
    AlterRegions,
    /// Update table manifest.
    UpdateTableManifest,
}

/// Serializable data of [AlterMitoTable].
#[derive(Debug, Serialize, Deserialize)]
struct AlterTableData {
    state: AlterTableState,
    request: AlterTableRequest,
    /// Table version before alteration.
    table_version: TableVersion,
}

impl AlterTableData {
    fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.request.catalog_name,
            schema: &self.request.schema_name,
            table: &self.request.table_name,
        }
    }
}
