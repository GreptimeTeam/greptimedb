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

use std::sync::{Arc, Weak};

use common_catalog::consts::{
    INFORMATION_SCHEMA_SSTS_INDEX_META_TABLE_ID, INFORMATION_SCHEMA_SSTS_MANIFEST_TABLE_ID,
    INFORMATION_SCHEMA_SSTS_STORAGE_TABLE_ID,
};
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::adapter::AsyncRecordBatchStreamAdapter;
use datatypes::schema::SchemaRef;
use snafu::ResultExt;
use store_api::sst_entry::{ManifestSstEntry, PuffinIndexMetaEntry, StorageSstEntry};
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::{ProjectSchemaSnafu, Result};
use crate::information_schema::{
    DatanodeInspectKind, DatanodeInspectRequest, InformationTable, SSTS_INDEX_META, SSTS_MANIFEST,
    SSTS_STORAGE,
};
use crate::system_schema::utils;

/// Information schema table for sst manifest.
pub struct InformationSchemaSstsManifest {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaSstsManifest {
    pub(super) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: ManifestSstEntry::schema(),
            catalog_manager,
        }
    }
}

impl InformationTable for InformationSchemaSstsManifest {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_SSTS_MANIFEST_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        SSTS_MANIFEST
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = if let Some(p) = &request.projection {
            Arc::new(self.schema.try_project(p).context(ProjectSchemaSnafu)?)
        } else {
            self.schema.clone()
        };
        let info_ext = utils::information_extension(&self.catalog_manager)?;
        let req = DatanodeInspectRequest {
            kind: DatanodeInspectKind::SstManifest,
            scan: request,
        };

        let future = async move {
            info_ext
                .inspect_datanode(req)
                .await
                .map_err(BoxedError::new)
                .context(common_recordbatch::error::ExternalSnafu)
        };
        Ok(Box::pin(AsyncRecordBatchStreamAdapter::new(
            schema,
            Box::pin(future),
        )))
    }
}

/// Information schema table for sst storage.
pub struct InformationSchemaSstsStorage {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaSstsStorage {
    pub(super) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: StorageSstEntry::schema(),
            catalog_manager,
        }
    }
}

impl InformationTable for InformationSchemaSstsStorage {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_SSTS_STORAGE_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        SSTS_STORAGE
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = if let Some(p) = &request.projection {
            Arc::new(self.schema.try_project(p).context(ProjectSchemaSnafu)?)
        } else {
            self.schema.clone()
        };

        let info_ext = utils::information_extension(&self.catalog_manager)?;
        let req = DatanodeInspectRequest {
            kind: DatanodeInspectKind::SstStorage,
            scan: request,
        };

        let future = async move {
            info_ext
                .inspect_datanode(req)
                .await
                .map_err(BoxedError::new)
                .context(common_recordbatch::error::ExternalSnafu)
        };
        Ok(Box::pin(AsyncRecordBatchStreamAdapter::new(
            schema,
            Box::pin(future),
        )))
    }
}

/// Information schema table for index metadata.
pub struct InformationSchemaSstsIndexMeta {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaSstsIndexMeta {
    pub(super) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: PuffinIndexMetaEntry::schema(),
            catalog_manager,
        }
    }
}

impl InformationTable for InformationSchemaSstsIndexMeta {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_SSTS_INDEX_META_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        SSTS_INDEX_META
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = if let Some(p) = &request.projection {
            Arc::new(self.schema.try_project(p).context(ProjectSchemaSnafu)?)
        } else {
            self.schema.clone()
        };

        let info_ext = utils::information_extension(&self.catalog_manager)?;
        let req = DatanodeInspectRequest {
            kind: DatanodeInspectKind::SstIndexMeta,
            scan: request,
        };

        let future = async move {
            info_ext
                .inspect_datanode(req)
                .await
                .map_err(BoxedError::new)
                .context(common_recordbatch::error::ExternalSnafu)
        };
        Ok(Box::pin(AsyncRecordBatchStreamAdapter::new(
            schema,
            Box::pin(future),
        )))
    }
}
