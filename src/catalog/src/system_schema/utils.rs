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

pub mod tables;

use std::sync::{Arc, Weak};

use common_config::Mode;
use common_meta::key::TableMetadataManagerRef;
use meta_client::client::MetaClient;
use snafu::OptionExt;

use crate::error::{Result, UpgradeWeakCatalogManagerRefSnafu};
use crate::kvbackend::KvBackendCatalogManager;
use crate::CatalogManager;

/// Try to get the server running mode from `[CatalogManager]` weak reference.
pub fn running_mode(catalog_manager: &Weak<dyn CatalogManager>) -> Result<Option<Mode>> {
    let catalog_manager = catalog_manager
        .upgrade()
        .context(UpgradeWeakCatalogManagerRefSnafu)?;

    Ok(catalog_manager
        .as_any()
        .downcast_ref::<KvBackendCatalogManager>()
        .map(|manager| manager.running_mode())
        .copied())
}

/// Try to get the `[MetaClient]` from `[CatalogManager]` weak reference.
pub fn meta_client(catalog_manager: &Weak<dyn CatalogManager>) -> Result<Option<Arc<MetaClient>>> {
    let catalog_manager = catalog_manager
        .upgrade()
        .context(UpgradeWeakCatalogManagerRefSnafu)?;

    let meta_client = match catalog_manager
        .as_any()
        .downcast_ref::<KvBackendCatalogManager>()
    {
        None => None,
        Some(manager) => manager.meta_client(),
    };

    Ok(meta_client)
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
