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

use common_meta::key::TableMetadataManagerRef;
use datafusion::physical_plan::ExecutionPlan;
use datatypes::schema::SchemaRef;
use snafu::{OptionExt, ResultExt};
use store_api::storage::ScanRequest;

use crate::CatalogManager;
use crate::error::{
    GetInformationExtensionSnafu, ProjectSchemaSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::{
    DatanodeInspectKind, DatanodeInspectRequest, InformationExtensionRef,
};
use crate::kvbackend::KvBackendCatalogManager;
use crate::system_schema::semantic_graph::EntityGraphProviderRef;

pub mod tables;

/// Projects an inspection table schema according to the scan request.
pub(crate) fn projected_schema(schema: &SchemaRef, request: &ScanRequest) -> Result<SchemaRef> {
    if let Some(p) = request.projection.as_deref() {
        Ok(Arc::new(schema.try_project(p).context(ProjectSchemaSnafu)?))
    } else {
        Ok(schema.clone())
    }
}

/// Builds a datanode inspection plan with the requested output schema.
pub(crate) fn inspect_plan(
    catalog_manager: &Weak<dyn CatalogManager>,
    kind: DatanodeInspectKind,
    schema: &SchemaRef,
    request: ScanRequest,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let projected_schema = projected_schema(schema, &request)?;
    let info_ext = information_extension(catalog_manager)?;
    let req = DatanodeInspectRequest {
        kind,
        scan: request,
    };

    info_ext.inspect_datanode_plan(req, projected_schema)
}

/// Try to get the entity-graph provider from a `[CatalogManager]` weak reference.
/// Returns `None` when the manager is not the kv-backed one or the provider has
/// not been injected yet (the computed graph tables then stream empty).
pub fn entity_graph_provider(
    catalog_manager: &Weak<dyn CatalogManager>,
) -> Result<Option<EntityGraphProviderRef>> {
    let catalog_manager = catalog_manager
        .upgrade()
        .context(UpgradeWeakCatalogManagerRefSnafu)?;

    Ok(catalog_manager
        .as_any()
        .downcast_ref::<KvBackendCatalogManager>()
        .and_then(|manager| manager.entity_graph_provider()))
}

/// Try to get the `[InformationExtension]` from `[CatalogManager]` weak reference.
pub fn information_extension(
    catalog_manager: &Weak<dyn CatalogManager>,
) -> Result<InformationExtensionRef> {
    let catalog_manager = catalog_manager
        .upgrade()
        .context(UpgradeWeakCatalogManagerRefSnafu)?;

    let information_extension = catalog_manager
        .as_any()
        .downcast_ref::<KvBackendCatalogManager>()
        .map(|manager| manager.information_extension())
        .context(GetInformationExtensionSnafu)?;

    Ok(information_extension)
}

/// Try to get the `[TableMetadataManagerRef]` from `[CatalogManager]` weak reference.
pub fn table_meta_manager(
    catalog_manager: &Weak<dyn CatalogManager>,
) -> Result<Option<TableMetadataManagerRef>> {
    let catalog_manager = catalog_manager
        .upgrade()
        .context(UpgradeWeakCatalogManagerRefSnafu)?;

    Ok(catalog_manager
        .as_any()
        .downcast_ref::<KvBackendCatalogManager>()
        .map(|manager| manager.table_metadata_manager_ref().clone()))
}
