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
use common_datasource::file_format::Format;
use common_datasource::object_store::build_backend;
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionNumber, ScanRequest};
use table::error::{self as table_error, Result as TableResult};
use table::metadata::{RawTableInfo, TableInfo, TableInfoRef, TableType};
use table::{requests, Table};

use super::format::create_stream;
use crate::error::{self, ConvertRawSnafu, Result};
use crate::manifest::immutable::{
    read_table_manifest, write_table_manifest, ImmutableMetadata, INIT_META_VERSION,
};
use crate::manifest::table_manifest_dir;
use crate::table::format::{CreateScanPlanContext, ScanPlanConfig};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ImmutableFileTableOptions {
    pub files: Vec<String>,
}

pub struct ImmutableFileTable {
    metadata: ImmutableMetadata,
    // currently, it's immutable
    table_info: Arc<TableInfo>,
    object_store: ObjectStore,
    files: Vec<String>,
    format: Format,
}

pub type ImmutableFileTableRef = Arc<ImmutableFileTable>;

#[async_trait]
impl Table for ImmutableFileTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// The [`SchemaRef`] before the projection.
    /// It contains all the columns that may appear in the files (All missing columns should be filled NULLs).
    fn schema(&self) -> SchemaRef {
        self.table_info().meta.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        self.table_info.clone()
    }

    fn table_type(&self) -> TableType {
        self.table_info().table_type
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> TableResult<SendableRecordBatchStream> {
        create_stream(
            &self.format,
            &CreateScanPlanContext::default(),
            &ScanPlanConfig {
                file_schema: self.schema(),
                files: &self.files,
                projection: request.projection.as_ref(),
                filters: &request.filters,
                limit: request.limit,
                store: self.object_store.clone(),
            },
        )
        .map_err(BoxedError::new)
        .context(table_error::TableOperationSnafu)
    }

    async fn flush(
        &self,
        _region_number: Option<RegionNumber>,
        _wait: Option<bool>,
    ) -> TableResult<()> {
        // nothing to flush
        Ok(())
    }
}

impl ImmutableFileTable {
    #[inline]
    pub fn metadata(&self) -> &ImmutableMetadata {
        &self.metadata
    }

    pub(crate) fn new(table_info: TableInfo, metadata: ImmutableMetadata) -> Result<Self> {
        let table_info = Arc::new(table_info);
        let options = &table_info.meta.options.extra_options;

        let url = options
            .get(requests::IMMUTABLE_TABLE_LOCATION_KEY)
            .context(error::MissingRequiredFieldSnafu {
                name: requests::IMMUTABLE_TABLE_LOCATION_KEY,
            })?;

        let meta = options.get(requests::IMMUTABLE_TABLE_META_KEY).context(
            error::MissingRequiredFieldSnafu {
                name: requests::IMMUTABLE_TABLE_META_KEY,
            },
        )?;

        let meta: ImmutableFileTableOptions =
            serde_json::from_str(meta).context(error::DecodeJsonSnafu)?;
        let format = Format::try_from(options).context(error::ParseFileFormatSnafu)?;

        let object_store = build_backend(url, options).context(error::BuildBackendSnafu)?;

        Ok(Self {
            metadata,
            table_info,
            object_store,
            files: meta.files,
            format,
        })
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

        ImmutableFileTable::new(table_info, metadata)
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
