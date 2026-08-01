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
use datafusion::physical_plan::ExecutionPlan;
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

fn projected_schema(schema: &SchemaRef, request: &ScanRequest) -> Result<SchemaRef> {
    if let Some(p) = request.projection.as_deref() {
        Ok(Arc::new(schema.try_project(p).context(ProjectSchemaSnafu)?))
    } else {
        Ok(schema.clone())
    }
}

fn inspect_plan(
    catalog_manager: &Weak<dyn CatalogManager>,
    kind: DatanodeInspectKind,
    schema: &SchemaRef,
    request: ScanRequest,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let projected_schema = projected_schema(schema, &request)?;
    let info_ext = utils::information_extension(catalog_manager)?;
    let req = DatanodeInspectRequest {
        kind,
        scan: request,
    };

    info_ext.inspect_datanode_plan(req, projected_schema)
}

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
        let schema = projected_schema(&self.schema, &request)?;
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

    fn scan_plan(&self, request: ScanRequest) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        inspect_plan(
            &self.catalog_manager,
            DatanodeInspectKind::SstManifest,
            &self.schema,
            request,
        )
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
        let schema = projected_schema(&self.schema, &request)?;

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

    fn scan_plan(&self, request: ScanRequest) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        inspect_plan(
            &self.catalog_manager,
            DatanodeInspectKind::SstStorage,
            &self.schema,
            request,
        )
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
        let schema = projected_schema(&self.schema, &request)?;

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

    fn scan_plan(&self, request: ScanRequest) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        inspect_plan(
            &self.catalog_manager,
            DatanodeInspectKind::SstIndexMeta,
            &self.schema,
            request,
        )
    }
}

#[cfg(test)]
mod tests {
    use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, INFORMATION_SCHEMA_NAME};
    use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
    use common_meta::cluster::NodeInfo;
    use common_meta::datanode::RegionStat;
    use common_meta::key::flow::flow_state::FlowStat;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_procedure::ProcedureInfo;
    use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::error::Error;
    use crate::information_schema::InformationExtension;
    use crate::kvbackend::KvBackendCatalogManagerBuilder;

    struct PlanInformationExtension;

    #[async_trait::async_trait]
    impl InformationExtension for PlanInformationExtension {
        type Error = Error;

        async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
            Ok(vec![])
        }

        async fn procedures(
            &self,
        ) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
            Ok(vec![])
        }

        async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
            Ok(vec![])
        }

        async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error> {
            Ok(None)
        }

        async fn inspect_datanode(
            &self,
            _request: DatanodeInspectRequest,
        ) -> std::result::Result<SendableRecordBatchStream, Self::Error> {
            Ok(RecordBatches::empty().as_stream())
        }

        fn inspect_datanode_plan(
            &self,
            _request: DatanodeInspectRequest,
            schema: SchemaRef,
        ) -> std::result::Result<Option<Arc<dyn ExecutionPlan>>, Self::Error> {
            Ok(Some(Arc::new(EmptyExec::new(
                schema.arrow_schema().clone(),
            ))))
        }
    }

    #[tokio::test]
    async fn ssts_manifest_uses_information_extension_scan_plan() {
        let backend = Arc::new(MemoryKvBackend::default());
        let layered_cache_builder = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(CacheRegistryBuilder::default().build());
        let fundamental_cache_registry = build_fundamental_cache_registry(backend.clone());
        let layered_cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        let catalog_manager = KvBackendCatalogManagerBuilder::new(
            Arc::new(PlanInformationExtension),
            backend,
            layered_cache_registry,
        )
        .build();

        let table = catalog_manager
            .table(
                DEFAULT_CATALOG_NAME,
                INFORMATION_SCHEMA_NAME,
                SSTS_MANIFEST,
                None,
            )
            .await
            .unwrap()
            .unwrap();

        let request = ScanRequest {
            projection: Some(vec![0]),
            ..Default::default()
        };
        let plan = table.scan_to_plan(request).unwrap().unwrap();

        assert!(plan.as_any().is::<EmptyExec>());
        assert_eq!(1, plan.schema().fields().len());
    }
}
