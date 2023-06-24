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
use common_telemetry::metric::Timer;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::manifest::Manifest;
use store_api::storage::{AlterOperation, StorageEngine};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableInfo, TableVersion};
use table::requests::{AlterKind, AlterTableRequest};
use table::{Table, TableRef};

use crate::engine::MitoEngineInner;
use crate::error::{TableNotFoundSnafu, UpdateTableManifestSnafu, VersionChangedSnafu};
use crate::manifest::action::{TableChange, TableMetaAction, TableMetaActionList};
use crate::metrics;
use crate::table::MitoTable;

/// Procedure to alter a [MitoTable].
pub(crate) struct AlterMitoTable<S: StorageEngine> {
    data: AlterTableData,
    table: Arc<MitoTable<S::Region>>,
    /// The table info after alteration.
    new_info: Option<TableInfo>,
    /// The region alter operation.
    alter_op: Option<AlterOperation>,
    _timer: Timer,
}

#[async_trait]
impl<S: StorageEngine> Procedure for AlterMitoTable<S> {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            AlterTableState::Prepare => self.on_prepare(),
            AlterTableState::EngineAlterTable => {
                self.engine_alter_table().await?;
                Ok(Status::Done)
            }
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
        let table = engine_inner
            .get_mito_table(data.request.table_id)
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;
        let info = table.table_info();
        data.table_version = info.ident.version;

        Ok(AlterMitoTable {
            data,
            table,
            new_info: None,
            alter_op: None,
            _timer: common_telemetry::timer!(metrics::MITO_ALTER_TABLE_ELAPSED),
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
        let table = engine_inner
            .get_mito_table(data.request.table_id)
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;

        Ok(AlterMitoTable {
            data,
            table,
            new_info: None,
            alter_op: None,
            _timer: common_telemetry::timer!(metrics::MITO_ALTER_TABLE_ELAPSED),
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

        // We don't check the table name in the table engine as it is the catalog
        // manager's duty to ensure the table name is unused.
        self.data.state = AlterTableState::EngineAlterTable;

        Ok(Status::executing(true))
    }

    /// Engine alters the table.
    ///
    /// Note that calling this method directly (without submitting the procedure
    /// to the manager) to rename a table might have concurrent issue when
    /// we renaming two tables to the same table name.
    pub(crate) async fn engine_alter_table(&mut self) -> Result<TableRef> {
        let current_info = self.table.table_info();
        if current_info.ident.version > self.data.table_version {
            // The table is already altered.
            return Ok(self.table.clone());
        }
        self.init_new_info_and_op(&current_info)?;

        self.alter_regions().await?;

        self.update_table_manifest().await
    }

    /// Alter regions.
    async fn alter_regions(&mut self) -> Result<()> {
        let Some(alter_op) = &self.alter_op else {
                // Don't need to alter the region.
                return Ok(());
            };

        let table_name = &self.data.request.table_name;
        let table_version = self.data.table_version;
        self.table
            .alter_regions(table_name, table_version, alter_op)
            .await
            .map_err(Error::from_error_ext)
    }

    /// Persist the alteration to the manifest and update table info.
    async fn update_table_manifest(&mut self) -> Result<TableRef> {
        // Safety: We init new info in engine_alter_table()
        let new_info = self.new_info.as_ref().unwrap();
        let table_name = &self.data.request.table_name;

        logging::debug!(
            "start updating the manifest of table {} with new table info {:?}",
            table_name,
            new_info
        );

        // It is possible that we write the manifest multiple times and bump the manifest
        // version, but it is still correct as we always write the new table info.
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

        Ok(self.table.clone())
    }

    fn init_new_info_and_op(&mut self, current_info: &TableInfo) -> Result<()> {
        if self.new_info.is_some() {
            return Ok(());
        }

        let (new_info, alter_op) = self
            .table
            .info_and_op_for_alter(current_info, &self.data.request.alter_kind)
            .map_err(Error::from_error_ext)?;

        self.new_info = Some(new_info);
        self.alter_op = alter_op;

        Ok(())
    }
}

/// Represents each step while altering table in the mito engine.
#[derive(Debug, Serialize, Deserialize)]
enum AlterTableState {
    /// Prepare to alter the table.
    Prepare,
    /// Engine alters the table.
    EngineAlterTable,
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
        self.request.table_ref()
    }
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use table::engine::{EngineContext, TableEngine, TableEngineProcedure};
    use table::requests::AddColumnRequest;

    use super::*;
    use crate::engine::procedure::procedure_test_util::{self, TestEnv};
    use crate::engine::tests::new_add_columns_req_with_location;
    use crate::table::test_util;

    fn new_add_columns_req() -> AlterTableRequest {
        let new_tag = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_field = ColumnSchema::new("my_field", ConcreteDataType::string_datatype(), true);
        let alter_kind = AlterKind::AddColumns {
            columns: vec![
                AddColumnRequest {
                    column_schema: new_tag,
                    is_key: true,
                    location: None,
                },
                AddColumnRequest {
                    column_schema: new_field,
                    is_key: false,
                    location: None,
                },
            ],
        };
        test_util::new_alter_request(alter_kind)
    }

    #[tokio::test]
    async fn test_procedure_add_column() {
        common_telemetry::init_default_ut_logging();

        let TestEnv {
            table_engine,
            dir: _dir,
        } = procedure_test_util::setup_test_engine("add_column").await;
        let schema = Arc::new(test_util::schema_for_test());
        let request = test_util::new_create_request(schema.clone());

        let engine_ctx = EngineContext::default();
        // Create table first.
        let mut procedure = table_engine
            .create_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Get metadata of the created table.
        let table = table_engine
            .get_table(&engine_ctx, request.id)
            .unwrap()
            .unwrap();
        let old_info = table.table_info();
        let old_meta = &old_info.meta;

        // Alter the table.
        let table_id = request.id;
        let request = new_add_columns_req();
        let mut procedure = table_engine
            .alter_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Validate.
        let table = table_engine
            .get_table(&engine_ctx, table_id)
            .unwrap()
            .unwrap();
        let new_info = table.table_info();
        let new_meta = &new_info.meta;
        let new_schema = &new_meta.schema;

        assert_eq!(&[0, 4], &new_meta.primary_key_indices[..]);
        assert_eq!(&[1, 2, 3, 5], &new_meta.value_indices[..]);
        assert!(new_schema.column_schema_by_name("my_tag").is_some());
        assert!(new_schema.column_schema_by_name("my_field").is_some());
        assert_eq!(new_schema.version(), schema.version() + 1);
        assert_eq!(new_meta.next_column_id, old_meta.next_column_id + 2);

        // Alter the table.
        let new_tag = ColumnSchema::new("my_tag_first", ConcreteDataType::string_datatype(), true);
        let new_field = ColumnSchema::new(
            "my_field_after_ts",
            ConcreteDataType::string_datatype(),
            true,
        );
        let request = new_add_columns_req_with_location(table_id, &new_tag, &new_field);
        let mut procedure = table_engine
            .alter_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Validate.
        let table = table_engine
            .get_table(&engine_ctx, table_id)
            .unwrap()
            .unwrap();
        let new_info = table.table_info();
        let new_meta = &new_info.meta;
        let new_schema = &new_meta.schema;

        assert_eq!(&[0, 1, 6], &new_meta.primary_key_indices[..]);
        assert_eq!(&[2, 3, 4, 5, 7], &new_meta.value_indices[..]);
        assert!(new_schema.column_schema_by_name("my_tag_first").is_some());
        assert!(new_schema
            .column_schema_by_name("my_field_after_ts")
            .is_some());
        assert_eq!(new_schema.version(), schema.version() + 2);
        assert_eq!(new_meta.next_column_id, old_meta.next_column_id + 4);
        assert_eq!(new_schema.column_index_by_name("my_tag_first").unwrap(), 0);
        assert_eq!(
            new_schema
                .column_index_by_name("my_field_after_ts")
                .unwrap(),
            new_schema.column_index_by_name("ts").unwrap() + 1
        );
    }

    #[tokio::test]
    async fn test_procedure_drop_column() {
        common_telemetry::init_default_ut_logging();

        let TestEnv {
            table_engine,
            dir: _dir,
        } = procedure_test_util::setup_test_engine("drop_column").await;
        let schema = Arc::new(test_util::schema_for_test());
        let request = test_util::new_create_request(schema.clone());

        let engine_ctx = EngineContext::default();
        // Create table first.
        let mut procedure = table_engine
            .create_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Add columns.
        let table_id = request.id;
        let request = new_add_columns_req();
        let mut procedure = table_engine
            .alter_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Get metadata.
        let table = table_engine
            .get_table(&engine_ctx, table_id)
            .unwrap()
            .unwrap();
        let old_info = table.table_info();
        let old_meta = &old_info.meta;

        // Then remove memory and my_field from the table.
        let alter_kind = AlterKind::DropColumns {
            names: vec![String::from("memory"), String::from("my_field")],
        };
        let request = test_util::new_alter_request(alter_kind);
        let mut procedure = table_engine
            .alter_table_procedure(&engine_ctx, request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Validate.
        let new_info = table.table_info();
        let new_meta = &new_info.meta;
        let new_schema = &new_meta.schema;

        let remaining_names: Vec<String> = new_schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.name.clone())
            .collect();
        assert_eq!(&["host", "cpu", "ts", "my_tag"], &remaining_names[..]);
        assert_eq!(&[0, 3], &new_meta.primary_key_indices[..]);
        assert_eq!(&[1, 2], &new_meta.value_indices[..]);
        assert_eq!(new_schema.version(), old_meta.schema.version() + 1);
        assert_eq!(new_meta.region_numbers, old_meta.region_numbers);
    }

    #[tokio::test]
    async fn test_procedure_rename_table() {
        common_telemetry::init_default_ut_logging();

        let TestEnv {
            table_engine,
            dir: _dir,
        } = procedure_test_util::setup_test_engine("rename").await;
        let schema = Arc::new(test_util::schema_for_test());
        let create_request = test_util::new_create_request(schema.clone());

        let engine_ctx = EngineContext::default();
        // Create table first.
        let mut procedure = table_engine
            .create_table_procedure(&engine_ctx, create_request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Get metadata of the created table.
        let table = table_engine
            .get_table(&engine_ctx, create_request.id)
            .unwrap()
            .unwrap();

        // Rename the table.
        let new_name = "another_table".to_string();
        let alter_kind = AlterKind::RenameTable {
            new_table_name: new_name.clone(),
        };
        let alter_request = test_util::new_alter_request(alter_kind);
        let mut procedure = table_engine
            .alter_table_procedure(&engine_ctx, alter_request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        // Validate.
        let info = table.table_info();
        assert_eq!(new_name, info.name);
        assert!(table_engine
            .get_table(&engine_ctx, create_request.id)
            .unwrap()
            .is_some());
    }
}
