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
use std::sync::Arc;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{Context, Error, LockKey, Procedure, ProcedureManager, Result, Status};
use datatypes::schema::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::storage::{
    ColumnId, CreateOptions, EngineContext, OpenOptions, RegionDescriptorBuilder, RegionNumber,
    StorageEngine,
};
use table::engine::{region_id, table_dir};
use table::metadata::{TableInfoBuilder, TableMetaBuilder, TableType};
use table::requests::CreateTableRequest;

use crate::engine::{self, MitoEngineInner, TableReference};
use crate::error::{
    BuildRegionDescriptorSnafu, BuildTableInfoSnafu, BuildTableMetaSnafu, InvalidRawSchemaSnafu,
    TableExistsSnafu,
};
use crate::table::MitoTable;

/// Procedure to create a [MitoTable].
pub(crate) struct CreateMitoTable<S: StorageEngine> {
    data: CreateTableData,
    engine_inner: Arc<MitoEngineInner<S>>,
    /// Created regions of the table.
    regions: HashMap<RegionNumber, S::Region>,
    /// Schema of the table.
    table_schema: SchemaRef,
}

#[async_trait]
impl<S: StorageEngine> Procedure for CreateMitoTable<S> {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            CreateTableState::Prepare => self.on_prepare(),
            CreateTableState::CreateRegions => self.on_create_regions().await,
            CreateTableState::WriteTableManifest => self.on_write_table_manifest().await,
        }
    }

    fn dump(&self) -> Result<String> {
        let json = serde_json::to_string(&self.data).context(ToJsonSnafu)?;
        Ok(json)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = self.data.table_ref();
        let keys = self
            .data
            .request
            .region_numbers
            .iter()
            .map(|number| format!("{table_ref}/region-{number}"));
        LockKey::new(keys)
    }
}

impl<S: StorageEngine> CreateMitoTable<S> {
    const TYPE_NAME: &str = "mito::CreateMitoTable";

    /// Returns a new [CreateMitoTable].
    pub(crate) fn new(
        request: CreateTableRequest,
        engine_inner: Arc<MitoEngineInner<S>>,
    ) -> Result<Self> {
        let table_schema =
            Schema::try_from(request.schema.clone()).context(InvalidRawSchemaSnafu)?;

        Ok(CreateMitoTable {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                request,
                next_column_id: None,
            },
            engine_inner,
            regions: HashMap::new(),
            table_schema: Arc::new(table_schema),
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
        let data: CreateTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let table_schema =
            Schema::try_from(data.request.schema.clone()).context(InvalidRawSchemaSnafu)?;

        Ok(CreateMitoTable {
            data,
            engine_inner,
            regions: HashMap::new(),
            table_schema: Arc::new(table_schema),
        })
    }

    /// Checks whether the table exists.
    fn on_prepare(&mut self) -> Result<Status> {
        let table_ref = self.data.table_ref();
        if self.engine_inner.get_table(&table_ref).is_some() {
            // If the table already exists.
            ensure!(
                self.data.request.create_if_not_exists,
                TableExistsSnafu {
                    table_name: table_ref.to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.data.state = CreateTableState::CreateRegions;

        Ok(Status::executing(true))
    }

    /// Creates regions for the table.
    async fn on_create_regions(&mut self) -> Result<Status> {
        let engine_ctx = EngineContext::default();
        let table_dir = table_dir(
            &self.data.request.catalog_name,
            &self.data.request.schema_name,
            self.data.request.id,
        );
        let table_options = &self.data.request.table_options;
        let write_buffer_size = table_options.write_buffer_size.map(|size| size.0 as usize);
        let ttl = table_options.ttl;
        let compaction_time_window = table_options.compaction_time_window;
        let open_opts = OpenOptions {
            parent_dir: table_dir.clone(),
            write_buffer_size,
            ttl,
            compaction_time_window,
        };
        let create_opts = CreateOptions {
            parent_dir: table_dir,
            write_buffer_size,
            ttl,
            compaction_time_window,
        };

        let primary_key_indices = &self.data.request.primary_key_indices;
        let (next_column_id, default_cf) = engine::build_column_family(
            engine::INIT_COLUMN_ID,
            &self.data.request.table_name,
            &self.table_schema,
            primary_key_indices,
        )?;
        let (next_column_id, row_key) = engine::build_row_key_desc(
            next_column_id,
            &self.data.request.table_name,
            &self.table_schema,
            primary_key_indices,
        )?;
        self.data.next_column_id = Some(next_column_id);

        // Try to open all regions and collect the regions not exist.
        for number in &self.data.request.region_numbers {
            if self.regions.contains_key(number) {
                // Region is opened.
                continue;
            }

            let region_name = engine::region_name(self.data.request.id, *number);
            if let Some(region) = self
                .engine_inner
                .storage_engine
                .open_region(&engine_ctx, &region_name, &open_opts)
                .await
                .map_err(Error::from_error_ext)?
            {
                // Region already exists.
                self.regions.insert(*number, region);
                continue;
            }

            // We need to create that region.
            let region_id = region_id(self.data.request.id, *number);
            let region_desc = RegionDescriptorBuilder::default()
                .id(region_id)
                .name(region_name.clone())
                .row_key(row_key.clone())
                .default_cf(default_cf.clone())
                .build()
                .context(BuildRegionDescriptorSnafu {
                    table_name: &self.data.request.table_name,
                    region_name,
                })?;

            let region = self
                .engine_inner
                .storage_engine
                .create_region(&engine_ctx, region_desc, &create_opts)
                .await
                .map_err(Error::from_error_ext)?;

            self.regions.insert(*number, region);
        }

        // All regions are created, moves to the next step.
        self.data.state = CreateTableState::WriteTableManifest;

        Ok(Status::executing(true))
    }

    /// Writes metadata to the table manifest.
    async fn on_write_table_manifest(&mut self) -> Result<Status> {
        let table_dir = table_dir(
            &self.data.request.catalog_name,
            &self.data.request.schema_name,
            self.data.request.id,
        );
        // Try to open the table first, as the table manifest might already exist.
        let table_ref = self.data.table_ref();
        if let Some((manifest, table_info)) = self
            .engine_inner
            .recover_table_manifest_and_info(&self.data.request.table_name, &table_dir)
            .await?
        {
            let table = Arc::new(MitoTable::new(table_info, self.regions.clone(), manifest));

            self.engine_inner
                .tables
                .write()
                .unwrap()
                .insert(table_ref.to_string(), table);
            return Ok(Status::Done);
        }

        // We need to persist the table manifest and create the table.
        let table = self.write_manifest_and_create_table(&table_dir).await?;
        let table = Arc::new(table);
        self.engine_inner
            .tables
            .write()
            .unwrap()
            .insert(table_ref.to_string(), table);

        Ok(Status::Done)
    }

    /// Write metadata to the table manifest and return the created table.
    async fn write_manifest_and_create_table(
        &self,
        table_dir: &str,
    ) -> Result<MitoTable<S::Region>> {
        // Safety: We are in `WriteTableManifest` state.
        let next_column_id = self.data.next_column_id.unwrap();

        let table_meta = TableMetaBuilder::default()
            .schema(self.table_schema.clone())
            .engine(engine::MITO_ENGINE)
            .next_column_id(next_column_id)
            .primary_key_indices(self.data.request.primary_key_indices.clone())
            .region_numbers(self.data.request.region_numbers.clone())
            .build()
            .context(BuildTableMetaSnafu {
                table_name: &self.data.request.table_name,
            })?;

        let table_info = TableInfoBuilder::new(self.data.request.table_name.clone(), table_meta)
            .ident(self.data.request.id)
            .table_version(engine::INIT_TABLE_VERSION)
            .table_type(TableType::Base)
            .catalog_name(&self.data.request.catalog_name)
            .schema_name(&self.data.request.schema_name)
            .desc(self.data.request.desc.clone())
            .build()
            .context(BuildTableInfoSnafu {
                table_name: &self.data.request.table_name,
            })?;

        let table = MitoTable::create(
            &self.data.request.table_name,
            table_dir,
            table_info,
            self.regions.clone(),
            self.engine_inner.object_store.clone(),
        )
        .await?;

        Ok(table)
    }
}

/// Represents each step while creating table in the mito engine.
#[derive(Debug, Serialize, Deserialize)]
enum CreateTableState {
    /// Prepare to create region.
    Prepare,
    /// Create regions.
    CreateRegions,
    /// Write metadata to table manifest.
    WriteTableManifest,
}

/// Serializable data of [CreateMitoTable].
#[derive(Debug, Serialize, Deserialize)]
struct CreateTableData {
    state: CreateTableState,
    request: CreateTableRequest,
    /// Next id for column.
    ///
    /// Available in [CreateTableState::WriteTableManifest] state.
    next_column_id: Option<ColumnId>,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.request.catalog_name,
            schema: &self.request.schema_name,
            table: &self.request.table_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use table::engine::{EngineContext, TableEngine, TableEngineProcedure};

    use super::*;
    use crate::engine::procedure::procedure_test_util::{self, TestEnv};
    use crate::table::test_util;

    #[tokio::test]
    async fn test_create_table_procedure() {
        let TestEnv {
            table_engine,
            dir: _dir,
        } = procedure_test_util::setup_test_engine("create_procedure").await;
        let schema = Arc::new(test_util::schema_for_test());
        let request = test_util::new_create_request(schema);

        let mut procedure = table_engine
            .create_table_procedure(&EngineContext::default(), request.clone())
            .unwrap();
        procedure_test_util::execute_procedure_until_done(&mut procedure).await;

        let table_ref = TableReference {
            catalog: &request.catalog_name,
            schema: &request.schema_name,
            table: &request.table_name,
        };
        assert!(table_engine
            .get_table(&EngineContext::default(), &table_ref)
            .unwrap()
            .is_some());
    }
}
