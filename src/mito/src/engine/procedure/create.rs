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
use common_procedure::{Context, LockKey, Procedure, Result, Error, Status};
use datatypes::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::storage::{ColumnId, RegionId, EngineContext, OpenOptions, CreateOptions, StorageEngine, RegionDescriptor, RegionDescriptorBuilder};
use table::requests::CreateTableRequest;
use table::metadata::{TableMetaBuilder, TableType, TableInfoBuilder};
use crate::table::MitoTable;

use snafu::ResultExt;
use crate::engine::{self, TableReference, MitoEngineInner};
use crate::error::{TableExistsSnafu, BuildTableInfoSnafu, BuildTableMetaSnafu, BuildRegionDescriptorSnafu};

/// Procedure to create a [MitoTable].
pub struct CreateMitoTable<S: StorageEngine> {
    data: CreateTableData,
    schema: SchemaRef,
    engine_inner: Arc<MitoEngineInner<S>>,
    /// Region for the table.
    ///
    /// The region is `Some` while [CreateTableData::state] is
    /// [CreateTableState::WriteTableManifest].
    region: Option<S::Region>,
}

#[async_trait]
impl<S: StorageEngine> Procedure for CreateMitoTable<S> {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
        match self.data.state {
            CreateTableState::Prepare => todo!(),
            CreateTableState::CreateRegion => todo!(),
            CreateTableState::WriteTableManifest => todo!(),
        }
    }

    fn dump(&self) -> Result<String> {
        unimplemented!()
    }

    fn lock_key(&self) -> LockKey {
        unimplemented!()
    }
}

impl<S: StorageEngine> CreateMitoTable<S> {
    const TYPE_NAME: &str = "mito::CreateMitoTable";

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

        self.data.state = CreateTableState::CreateRegion;

        Ok(Status::executing(true))
    }

    /// Creates regions for the table.
    async fn on_create_region(&mut self) -> Result<Status> {
        // Try to open the region.
        let region_number = self.data.request.region_numbers[0];
        let region_name = engine::region_name(self.data.request.id, region_number);

        let engine_ctx = EngineContext::default();
        let table_dir = engine::table_dir(&self.data.request.catalog_name, &self.data.request.schema_name, self.data.request.id);
        let opts = OpenOptions {
            parent_dir: table_dir.clone(),
        };

        if let Some(region) = self
            .engine_inner
            .storage_engine
            .open_region(&engine_ctx, &region_name, &opts)
            .await
            .map_err(Error::external)?
        {
            // The region has been created, we could move to the next step.
            self.switch_to_write_table_manifest(region);

            return Ok(Status::executing(true));
        }

        // Create a new region.
        let region_id = engine::region_id(self.data.request.id, region_number);
        let region_desc = self.build_region_desc(region_id, &region_name)?;
        let opts = CreateOptions {
            parent_dir: table_dir,
        };
        let region = self
            .engine_inner
            .storage_engine
            .create_region(&engine_ctx, region_desc, &opts)
            .await
            .map_err(Error::external)?;

        self.switch_to_write_table_manifest(region);

        Ok(Status::executing(true))
    }

    /// Writes metadata to the table manifest.
    async fn on_write_table_manifest(&mut self) -> Result<Status> {
        let table_ref = self.data.table_ref();
        if let Some(table) = self.engine_inner.get_table(&table_ref) {
            // If the table is opened, we are done.
            return Ok(Status::Done);
        }

        // Try to open the table, as the table manifest might already exist.
        let table_dir = engine::table_dir(&self.data.request.catalog_name, &self.data.request.schema_name, self.data.request.id);
        // Safety: The region is not None in `WriteTableManifest` state.
        let region = self.region.clone().unwrap();
        let table_opt = MitoTable::open(
            &self.data.request.table_name,
            &table_dir,
            region.clone(),
            self.engine_inner.object_store.clone(),
        )
        .await?;
        if let Some(table) = table_opt {
            let table = Arc::new(table);
            // We already have the table manifest, just need to insert the table into the table map.
            self.engine_inner
                .tables
                .write()
                .unwrap()
                .insert(table_ref.to_string(), table.clone());
            
            return Ok(Status::Done);
        }

        // We need to persist the table manifest and create the table instance.
        let table = self
            .write_manifest_and_create_table(&table_dir, region)
            .await?;
        let table = Arc::new(table);
        self.engine_inner
            .tables
            .write()
            .unwrap()
            .insert(table_ref.to_string(), table.clone());

        Ok(Status::Done)
    }

    /// Switchs to [CreateTableState::WriteTableManifest] state and set [CreateTableProcedure::region].
    fn switch_to_write_table_manifest(&mut self, region: S::Region) {
        self.data.state = CreateTableState::WriteTableManifest;
        self.region = Some(region);
    }

    /// Builds [RegionDescriptor] and cache next column id in [CreateTableProcedure::data].
    fn build_region_desc(
        &mut self,
        region_id: RegionId,
        region_name: &str,
    ) -> Result<RegionDescriptor> {
        let primary_key_indices = &self.data.request.primary_key_indices;
        let (next_column_id, default_cf) = engine::build_column_family(
            engine::INIT_COLUMN_ID,
            &self.data.request.table_name,
            &self.schema,
            primary_key_indices,
        )?;
        let (next_column_id, row_key) = engine::build_row_key_desc(
            next_column_id,
            &self.data.request.table_name,
            &self.schema,
            primary_key_indices,
        )?;

        let region_desc = RegionDescriptorBuilder::default()
            .id(region_id)
            .name(region_name)
            .row_key(row_key)
            .default_cf(default_cf)
            .build()
            .context(BuildRegionDescriptorSnafu {
                table_name: &self.data.request.table_name,
                region_name,
            })?;

        self.data.next_column_id = Some(next_column_id);

        Ok(region_desc)
    }

    /// Write metadata to the table manifest and return the created table.
    async fn write_manifest_and_create_table(
        &self,
        table_dir: &str,
        region: S::Region,
    ) -> Result<MitoTable<S::Region>> {
        // Safety: We are in `WriteTableManifest` state.
        let next_column_id = self.data.next_column_id.unwrap();

        let table_meta = TableMetaBuilder::default()
            .schema(self.schema.clone())
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
            region,
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
    /// Create region.
    CreateRegion,
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
