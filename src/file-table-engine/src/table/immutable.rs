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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use datatypes::schema::SchemaRef;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::error::Result as TableResult;
use table::metadata::{RawTableInfo, TableInfo, TableInfoRef, TableType};
use table::Table;

use crate::error::{ConvertRawSnafu, CreateTableManifestSnafu, RecoverTableManifestSnafu, Result};
use crate::manifest::immutable::{ImmutableManifest, ImmutableMetadata};
use crate::manifest::table_manifest_dir;

pub struct ImmutableFileTable {
    manifest: ImmutableManifest,
    // currently, it's immutable
    table_info: Arc<TableInfo>,
}

pub type ImmutableFileTableRef = Arc<ImmutableFileTable>;

#[async_trait]
impl Table for ImmutableFileTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_info().meta.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.table_info().table_type
    }

    fn table_info(&self) -> TableInfoRef {
        self.table_info.clone()
    }

    async fn scan(
        &self,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> TableResult<PhysicalPlanRef> {
        todo!()
    }

    async fn flush(
        &self,
        _region_number: Option<RegionNumber>,
        _wait: Option<bool>,
    ) -> TableResult<()> {
        // nothing to flush
        Ok(())
    }

    async fn close(&self) -> TableResult<()> {
        todo!()
    }
}

impl ImmutableFileTable {
    #[inline]
    pub fn manifest(&self) -> &ImmutableManifest {
        &self.manifest
    }

    pub(crate) fn new(table_info: TableInfo, manifest: ImmutableManifest) -> Self {
        Self {
            manifest,
            table_info: Arc::new(table_info),
        }
    }

    pub async fn create(
        table_name: &str,
        table_dir: &str,
        table_info: TableInfo,
        object_store: ObjectStore,
    ) -> Result<ImmutableFileTable> {
        let manifest = ImmutableManifest::new(&table_manifest_dir(table_dir), object_store);

        manifest
            .write(&ImmutableMetadata {
                table_info: RawTableInfo::from(table_info.clone()),
            })
            .await
            .map_err(BoxedError::new)
            .context(CreateTableManifestSnafu { table_name })?;

        Ok(ImmutableFileTable::new(table_info, manifest))
    }

    pub(crate) async fn recover_table_info(
        table_name: &str,
        manifest: &ImmutableManifest,
    ) -> Result<Option<TableInfo>> {
        let metadata = manifest
            .read()
            .await
            .map_err(BoxedError::new)
            .context(RecoverTableManifestSnafu { table_name })?;

        Ok(Some(
            TableInfo::try_from(metadata.table_info).context(ConvertRawSnafu)?,
        ))
    }
}
