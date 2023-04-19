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
use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use datatypes::schema::SchemaRef;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::error::Result as TableResult;
use table::metadata::{RawTableInfo, TableInfo, TableInfoRef, TableType};
use table::Table;

use crate::error::{ConvertRawSnafu, Result};
use crate::manifest::immutable::{
    read_table_manifest, write_table_manifest, ImmutableMetadata, INIT_META_VERSION,
};
use crate::manifest::table_manifest_dir;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ImmutableFileTableOptions {
    pub files: Vec<String>,
}

pub struct ImmutableFileTable {
    metadata: ImmutableMetadata,
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
        Ok(())
    }
}

impl ImmutableFileTable {
    #[inline]
    pub fn metadata(&self) -> &ImmutableMetadata {
        &self.metadata
    }

    pub(crate) fn new(table_info: TableInfo, metadata: ImmutableMetadata) -> Self {
        Self {
            metadata,
            table_info: Arc::new(table_info),
        }
    }

    pub async fn create(
        table_name: &str,
        table_dir: &str,
        table_info: TableInfo,
        object_store: ObjectStore,
    ) -> Result<ImmutableFileTable> {
        let metadata = ImmutableMetadata {
            table_info: RawTableInfo::from(table_info.clone()),
            version: INIT_META_VERSION,
        };

        write_table_manifest(
            table_name,
            &table_manifest_dir(table_dir),
            &object_store,
            &metadata,
        )
        .await?;

        Ok(ImmutableFileTable::new(table_info, metadata))
    }

    pub(crate) async fn recover_table_info(
        table_name: &str,
        table_dir: &str,
        object_store: &ObjectStore,
    ) -> Result<(ImmutableMetadata, TableInfo)> {
        let metadata = read_table_manifest(table_name, table_dir, object_store).await?;
        let table_info =
            TableInfo::try_from(metadata.table_info.clone()).context(ConvertRawSnafu)?;

        Ok((metadata, table_info))
    }
}
