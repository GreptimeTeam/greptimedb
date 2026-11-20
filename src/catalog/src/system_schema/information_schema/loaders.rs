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

//! Metadata loaders used by information schema scans.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use common_catalog::consts::MITO_ENGINE;
use common_error::ext::BoxedError;
use common_meta::cache::{FlowInfoCache, ViewInfoCache, load_shared_flow_infos};
use common_meta::key::FlowId;
use common_meta::key::flow::flow_info::{FlowInfoManager, FlowInfoValue};
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::key::view_info::ViewInfoValue;
use partition::manager::PartitionRuleManager;
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;
use table::TableRef;
use table::metadata::TableInfoRef;

use crate::CatalogManager;
use crate::error::{
    FindRegionRoutesSnafu, FlowInfoNotFoundSnafu, GetViewCacheSnafu, InternalSnafu, Result,
};

/// Reads one batch of named flows, preserving the order of the name scan.
pub(crate) struct FlowInfoLoader<'a> {
    cache: Option<&'a FlowInfoCache>,
    manager: &'a FlowInfoManager,
    catalog: &'a str,
}

impl<'a> FlowInfoLoader<'a> {
    pub(crate) fn new(
        cache: Option<&'a FlowInfoCache>,
        manager: &'a FlowInfoManager,
        catalog: &'a str,
    ) -> Self {
        Self {
            cache,
            manager,
            catalog,
        }
    }

    pub(crate) async fn load(
        &self,
        entries: Vec<(String, FlowId)>,
    ) -> Result<Vec<(FlowId, Arc<FlowInfoValue>)>> {
        let ids: Vec<_> = entries.iter().map(|(_, id)| *id).collect();
        let values = if let Some(cache) = self.cache {
            cache.get_many(&ids).await
        } else {
            load_shared_flow_infos(self.manager, &ids)
                .await
                .map(|values| ids.iter().map(|id| values.get(id).cloned()).collect())
        }
        .map_err(BoxedError::new)
        .context(InternalSnafu)?;
        entries
            .into_iter()
            .zip(values)
            .map(|((name, id), value)| {
                let info = value.context(FlowInfoNotFoundSnafu {
                    catalog_name: self.catalog,
                    flow_name: name,
                })?;
                Ok((id, info))
            })
            .collect::<Result<Vec<_>>>()
    }
}

pub(crate) async fn load_source_table_names(
    catalog_manager: &dyn CatalogManager,
    flows: &[(FlowId, Arc<FlowInfoValue>)],
) -> Result<HashMap<TableId, String>> {
    let source_ids: BTreeSet<_> = flows
        .iter()
        .flat_map(|(_, info)| info.source_table_ids().iter().copied())
        .collect();
    let source_ids: Vec<_> = source_ids.into_iter().collect();
    let mut names = HashMap::new();
    for ids in source_ids.chunks(128) {
        let infos = catalog_manager.table_infos_by_ids(ids).await?;
        names.extend(
            ids.iter()
                .copied()
                .zip(infos)
                .filter_map(|(id, info)| Some((id, info?.full_table_name()))),
        );
    }
    Ok(names)
}

pub(crate) async fn load_view_definitions(
    cache: &ViewInfoCache,
    tables: Vec<TableInfoRef>,
) -> Result<impl Iterator<Item = (TableInfoRef, Option<Arc<ViewInfoValue>>)> + use<>> {
    let ids = tables
        .iter()
        .map(|info| info.table_id())
        .collect::<Vec<_>>();
    let definitions = cache.get_many(&ids).await.context(GetViewCacheSnafu)?;
    Ok(tables.into_iter().zip(definitions))
}

pub(crate) async fn load_table_routes(
    manager: Option<&PartitionRuleManager>,
    tables: &[TableRef],
) -> Result<HashMap<TableId, Arc<PhysicalTableRouteValue>>> {
    let Some(manager) = manager else {
        return Ok(HashMap::new());
    };
    let ids = tables
        .iter()
        .map(|table| table.table_info())
        .filter(|info| info.meta.engine == MITO_ENGINE || info.is_physical_table())
        .map(|info| info.table_id())
        .collect::<Vec<_>>();
    manager
        .batch_cached_physical_table_routes(&ids)
        .await
        .context(FindRegionRoutesSnafu)
}
