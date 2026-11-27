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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, OnceLock, Weak};

use async_stream::try_stream;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME,
    INFORMATION_SCHEMA_NAME, PG_CATALOG_NAME,
};
use common_error::ext::BoxedError;
use common_meta::cache::{
    LayeredCacheRegistryRef, TableInfoCacheRef, TableNameCacheRef, TableRoute, TableRouteCacheRef,
    ViewInfoCacheRef,
};
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_name::TableNameKey;
use common_meta::kv_backend::KvBackendRef;
use common_procedure::ProcedureManagerRef;
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt, TryStreamExt};
use itertools::Either;
use moka::sync::Cache;
use partition::manager::PartitionRuleManagerRef;
use session::context::{Channel, QueryContext};
use snafu::prelude::*;
use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
use table::TableRef;
use table::dist_table::DistTable;
use table::metadata::{TableId, TableInfoRef};
use table::table::PartitionRules;
use table::table_name::TableName;

use crate::CatalogManager;
use crate::batch_scan::BatchScanner;
use crate::error::{
    CacheNotFoundSnafu, GetTableCacheSnafu, ListCatalogsSnafu, ListSchemasSnafu, ListTablesSnafu,
    Result, TableMetadataManagerSnafu,
};
use crate::information_schema::{
    InformationExtensionRef, InformationSchemaProvider, InformationSchemaSessionVariables,
    InformationSchemaTableFactoryRef, QueryOptionValueProviderRef, SESSION_VARIABLES,
};
use crate::kvbackend::TableCacheRef;
use crate::process_manager::ProcessManagerRef;
use crate::system_schema::SystemSchemaProvider;
use crate::system_schema::numbers_table_provider::NumbersTableProvider;
use crate::system_schema::pg_catalog::PGCatalogProvider;
use crate::system_schema::semantic_graph::{EntityGraphProviderRef, SemanticGraphTableProvider};

/// Access all existing catalog, schema and tables.
///
/// The result comes from two source, all the user tables are presented in
/// a kv-backend which persists the metadata of a table. And system tables
/// comes from `SystemCatalog`, which is static and read-only.
#[derive(Clone)]
pub struct KvBackendCatalogManager {
    /// Provides the extension methods for the `information_schema` tables
    pub(super) information_extension: InformationExtensionRef,
    /// Backs the computed entity-graph tables (`semantic_entities` /
    /// `semantic_relationships`). Set once, after the query engine is built, to
    /// break the `catalog -> query` cycle; `None` until then. `Arc` so the cell
    /// stays shared across `Clone`s of the manager (a plain `OnceLock` would
    /// fork on clone).
    pub(super) entity_graph_provider: Arc<OnceLock<EntityGraphProviderRef>>,
    /// Resolves `query.*` options to their effective values for
    /// `information_schema.session_variables`. Set once, after the query engine
    /// is built, for the same `catalog -> query` cycle reason as
    /// `entity_graph_provider`; the table omits those options until then.
    pub(super) query_option_value_provider: Arc<OnceLock<QueryOptionValueProviderRef>>,
    /// Manages partition rules.
    pub(super) partition_manager: PartitionRuleManagerRef,
    /// Manages table metadata.
    pub(super) table_metadata_manager: TableMetadataManagerRef,
    /// A sub-CatalogManager that handles system tables
    pub(super) system_catalog: SystemCatalog,
    /// Cache registry for all caches.
    pub(super) cache_registry: LayeredCacheRegistryRef,
    /// Only available in `Standalone` mode.
    pub(super) procedure_manager: Option<ProcedureManagerRef>,
}

pub(super) const CATALOG_CACHE_MAX_CAPACITY: u64 = 128;

impl KvBackendCatalogManager {
    /// Returns the registered system tables, excluding user-created metadata.
    pub fn information_schema_provider(&self) -> &InformationSchemaProvider {
        &self.system_catalog.information_schema_provider
    }

    pub(crate) async fn schema_options(&self, catalog: &str) -> Result<BTreeMap<String, String>> {
        let mut schemas = self
            .table_metadata_manager
            .schema_manager()
            .schemas(catalog)
            .map_ok(|(name, value)| (name, value.to_string()))
            .try_collect::<BTreeMap<_, _>>()
            .await
            .map_err(BoxedError::new)
            .context(ListSchemasSnafu { catalog })?;
        for name in self.system_catalog.schema_names(None) {
            schemas.entry(name).or_default();
        }
        Ok(schemas)
    }

    pub(crate) fn flow_info_cache(&self) -> Option<common_meta::cache::FlowInfoCacheRef> {
        self.cache_registry.get()
    }

    pub fn view_info_cache(&self) -> Result<ViewInfoCacheRef> {
        self.cache_registry.get().context(CacheNotFoundSnafu {
            name: "view_info_cache",
        })
    }

    /// Returns the [`InformationExtension`].
    pub fn information_extension(&self) -> InformationExtensionRef {
        self.information_extension.clone()
    }

    /// Returns the entity-graph provider, or `None` if it has not been injected
    /// yet (before the query engine is built). The computed graph tables stream
    /// empty until it is set.
    pub fn entity_graph_provider(&self) -> Option<EntityGraphProviderRef> {
        self.entity_graph_provider.get().cloned()
    }

    /// Injects the entity-graph provider once the query engine exists. A second
    /// call is a no-op (the first binding wins).
    pub fn set_entity_graph_provider(&self, provider: EntityGraphProviderRef) {
        let _ = self.entity_graph_provider.set(provider);
    }

    /// Returns the query-option resolver, or `None` if it has not been injected
    /// yet. `information_schema.session_variables` omits `query.*` options until
    /// it is set.
    pub fn query_option_value_provider(&self) -> Option<QueryOptionValueProviderRef> {
        self.query_option_value_provider.get().cloned()
    }

    /// Injects the query-option resolver once the query engine exists. A second
    /// call is a no-op (the first binding wins).
    pub fn set_query_option_value_provider(&self, provider: QueryOptionValueProviderRef) {
        let _ = self.query_option_value_provider.set(provider);
    }

    pub fn partition_manager(&self) -> PartitionRuleManagerRef {
        self.partition_manager.clone()
    }

    pub fn table_metadata_manager_ref(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn procedure_manager(&self) -> Option<ProcedureManagerRef> {
        self.procedure_manager.clone()
    }

    // Override logical table's partition key indices with physical table's.
    async fn override_logical_table_partition_key_indices(
        table_route_cache: &TableRouteCacheRef,
        table_info_cache: &TableInfoCacheRef,
        table: TableRef,
    ) -> Result<TableRef> {
        // If the table is not a metric table, return the table directly.
        if table.table_info().meta.engine != METRIC_ENGINE_NAME {
            return Ok(table);
        }

        if let Some(table_route_value) = table_route_cache
            .get(table.table_info().table_id())
            .await
            .context(TableMetadataManagerSnafu)?
            && let TableRoute::Logical(logical_route) = &*table_route_value
            && let Some(physical_table_info) = table_info_cache
                .get(logical_route.physical_table_id())
                .await
                .context(TableMetadataManagerSnafu)?
        {
            return Ok(Self::logical_table(
                table.table_info(),
                &physical_table_info,
            ));
        }
        Ok(table)
    }

    async fn load_tables(
        ids: Vec<TableId>,
        infos: &TableInfoCacheRef,
        routes: &TableRouteCacheRef,
    ) -> Result<impl Iterator<Item = TableRef> + use<>> {
        let tables: Vec<_> = infos
            .get_many(&ids)
            .await
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .flatten()
            .collect();
        let metric_ids: Vec<_> = tables
            .iter()
            .filter(|info| info.meta.engine == METRIC_ENGINE_NAME)
            .map(|info| info.table_id())
            .collect();
        let metric_routes = routes
            .get_many(&metric_ids)
            .await
            .context(TableMetadataManagerSnafu)?;
        let logical: HashMap<_, _> = metric_ids
            .into_iter()
            .zip(metric_routes)
            .filter_map(|(id, route)| match route.as_deref()? {
                TableRoute::Logical(route) => Some((id, route.physical_table_id())),
                TableRoute::Physical(_) => None,
            })
            .collect();
        let physical_ids: BTreeSet<_> = logical.values().copied().collect();
        let physical_ids: Vec<_> = physical_ids.into_iter().collect();
        let physical = infos
            .get_many(&physical_ids)
            .await
            .context(TableMetadataManagerSnafu)?;
        let physical: HashMap<_, _> = physical_ids
            .into_iter()
            .zip(physical)
            .filter_map(|(id, info)| Some((id, info?)))
            .collect();
        Ok(tables.into_iter().map(move |info| {
            if let Some(physical) = logical
                .get(&info.table_id())
                .and_then(|id| physical.get(id))
            {
                Self::logical_table(info, physical)
            } else {
                DistTable::table(info)
            }
        }))
    }

    fn logical_table(info: TableInfoRef, physical_table_info: &TableInfoRef) -> TableRef {
        let mut new_table_info = info.as_ref().clone();

        let mut phy_part_cols_not_in_logical_table = vec![];

        // Remap partition key indices from physical table to logical table
        new_table_info.meta.partition_key_indices = physical_table_info
            .meta
            .partition_key_indices
            .iter()
            .filter_map(|&physical_index| {
                // Get the column name from the physical table using the physical index
                physical_table_info
                    .meta
                    .schema
                    .column_schemas()
                    .get(physical_index)
                    .and_then(|physical_column| {
                        // Find the corresponding index in the logical table schema
                        let idx = new_table_info
                            .meta
                            .schema
                            .column_index_by_name(physical_column.name.as_str());
                        if idx.is_none() {
                            // not all part columns in physical table that are also in logical table
                            phy_part_cols_not_in_logical_table.push(physical_column.name.clone());
                        }

                        idx
                    })
            })
            .collect();

        let partition_rules = if !phy_part_cols_not_in_logical_table.is_empty() {
            Some(PartitionRules {
                extra_phy_cols_not_in_logical_table: phy_part_cols_not_in_logical_table,
            })
        } else {
            None
        };

        DistTable::table_partitioned(Arc::new(new_table_info), partition_rules)
    }
}

#[async_trait::async_trait]
impl CatalogManager for KvBackendCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        let stream = self
            .table_metadata_manager
            .catalog_manager()
            .catalog_names();

        let keys = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(BoxedError::new)
            .context(ListCatalogsSnafu)?;

        Ok(keys)
    }

    async fn schema_names(
        &self,
        catalog: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Vec<String>> {
        let stream = self
            .table_metadata_manager
            .schema_manager()
            .schema_names(catalog);
        let mut keys = stream
            .try_collect::<BTreeSet<_>>()
            .await
            .map_err(BoxedError::new)
            .context(ListSchemasSnafu { catalog })?;

        keys.extend(self.system_catalog.schema_names(query_ctx));

        Ok(keys.into_iter().collect())
    }

    async fn table_names(
        &self,
        catalog: &str,
        schema: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Vec<String>> {
        let mut tables = self
            .table_metadata_manager
            .table_name_manager()
            .tables(catalog, schema)
            .map_ok(|(table_name, _)| table_name)
            .try_collect::<Vec<_>>()
            .await
            .map_err(BoxedError::new)
            .context(ListTablesSnafu { catalog, schema })?;

        tables.extend(self.system_catalog.table_names(schema, query_ctx));
        Ok(tables)
    }

    async fn catalog_exists(&self, catalog: &str) -> Result<bool> {
        self.table_metadata_manager
            .catalog_manager()
            .exists(CatalogNameKey::new(catalog))
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn schema_exists(
        &self,
        catalog: &str,
        schema: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        if self.system_catalog.schema_exists(schema, query_ctx) {
            return Ok(true);
        }

        self.table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey::new(catalog, schema))
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn table_exists(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        if self.system_catalog.table_exists(schema, table, query_ctx) {
            return Ok(true);
        }

        let key = TableNameKey::new(catalog, schema, table);
        self.table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataManagerSnafu)
            .map(|x| x.is_some())
    }

    async fn table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Option<TableRef>> {
        let channel = query_ctx.map_or(Channel::Unknown, |ctx| ctx.channel());
        if let Some(table) =
            self.system_catalog
                .table(catalog_name, schema_name, table_name, query_ctx)
        {
            return Ok(Some(table));
        }

        let table_cache: TableCacheRef = self.cache_registry.get().context(CacheNotFoundSnafu {
            name: "table_cache",
        })?;

        let table = table_cache
            .get_by_ref(&TableName {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            })
            .await
            .context(GetTableCacheSnafu)?;

        if let Some(table) = table {
            let table_route_cache: TableRouteCacheRef =
                self.cache_registry.get().context(CacheNotFoundSnafu {
                    name: "table_route_cache",
                })?;
            let table_info_cache: TableInfoCacheRef =
                self.cache_registry.get().context(CacheNotFoundSnafu {
                    name: "table_info_cache",
                })?;
            return Self::override_logical_table_partition_key_indices(
                &table_route_cache,
                &table_info_cache,
                table,
            )
            .await
            .map(Some);
        }

        if channel == Channel::Postgres {
            // falldown to pg_catalog
            if let Some(table) =
                self.system_catalog
                    .table(catalog_name, PG_CATALOG_NAME, table_name, query_ctx)
            {
                return Ok(Some(table));
            }
        }

        Ok(None)
    }

    async fn table_id(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Option<TableId>> {
        let channel = query_ctx.map_or(Channel::Unknown, |ctx| ctx.channel());
        if let Some(table) =
            self.system_catalog
                .table(catalog_name, schema_name, table_name, query_ctx)
        {
            return Ok(Some(table.table_info().table_id()));
        }

        let table_cache: TableNameCacheRef =
            self.cache_registry.get().context(CacheNotFoundSnafu {
                name: "table_name_cache",
            })?;

        let table = table_cache
            .get_by_ref(&TableName {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            })
            .await
            .context(GetTableCacheSnafu)?;

        if let Some(table) = table {
            return Ok(Some(table));
        }

        if channel == Channel::Postgres {
            // falldown to pg_catalog
            if let Some(table) =
                self.system_catalog
                    .table(catalog_name, PG_CATALOG_NAME, table_name, query_ctx)
            {
                return Ok(Some(table.table_info().table_id()));
            }
        }

        Ok(None)
    }

    async fn table_ids(&self, names: &[TableName]) -> Result<Vec<Option<TableId>>> {
        let mut results = vec![None; names.len()];
        let mut missing = Vec::new();
        let mut positions = Vec::new();
        for (index, name) in names.iter().enumerate() {
            if let Some(table) = self.system_catalog.table(
                &name.catalog_name,
                &name.schema_name,
                &name.table_name,
                None,
            ) {
                results[index] = Some(table.table_info().table_id());
            } else {
                missing.push(name.clone());
                positions.push(index);
            }
        }
        if missing.is_empty() {
            return Ok(results);
        }
        let cache: TableNameCacheRef = self.cache_registry.get().context(CacheNotFoundSnafu {
            name: "table_name_cache",
        })?;
        for (index, value) in positions
            .into_iter()
            .zip(cache.get_many(&missing).await.context(GetTableCacheSnafu)?)
        {
            results[index] = value;
        }
        Ok(results)
    }

    async fn table_infos_by_ids(&self, ids: &[TableId]) -> Result<Vec<Option<TableInfoRef>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let cache: TableInfoCacheRef = self.cache_registry.get().context(CacheNotFoundSnafu {
            name: "table_info_cache",
        })?;
        cache.get_many(ids).await.context(GetTableCacheSnafu)
    }

    async fn table_info_by_id(&self, table_id: TableId) -> Result<Option<TableInfoRef>> {
        let table_info_cache: TableInfoCacheRef =
            self.cache_registry.get().context(CacheNotFoundSnafu {
                name: "table_info_cache",
            })?;
        table_info_cache
            .get_by_ref(&table_id)
            .await
            .context(GetTableCacheSnafu)
    }

    async fn tables_by_ids(
        &self,
        catalog: &str,
        schema: &str,
        table_ids: &[TableId],
    ) -> Result<Vec<TableRef>> {
        let table_info_values = self
            .table_metadata_manager
            .table_info_manager()
            .batch_get(table_ids)
            .await
            .context(TableMetadataManagerSnafu)?;

        let tables = table_info_values
            .into_values()
            .filter(|t| t.table_info.catalog_name == catalog && t.table_info.schema_name == schema)
            .map(build_table)
            .collect::<Vec<_>>();

        Ok(tables)
    }

    fn tables<'a>(
        &'a self,
        catalog: &'a str,
        schema: &'a str,
        query_ctx: Option<&'a QueryContext>,
    ) -> BoxStream<'a, Result<TableRef>> {
        try_stream! {
            for table in self.system_tables(catalog, schema, query_ctx) {
                yield table;
            }
            let (infos, routes) = self.table_caches()?;
            let ids = self.user_table_ids(catalog, schema);
            let batches =
                BatchScanner::METADATA.scan(ids, |ids| Self::load_tables(ids, &infos, &routes));
            let mut batches = std::pin::pin!(batches);
            while let Some(batch) = batches.try_next().await? {
                for table in batch {
                    yield table;
                }
            }
        }
        .boxed()
    }
}

impl KvBackendCatalogManager {
    fn system_tables<'a>(
        &'a self,
        catalog: &'a str,
        schema: &'a str,
        query_ctx: Option<&'a QueryContext>,
    ) -> impl Iterator<Item = TableRef> + 'a {
        self.system_catalog
            .table_names(schema, query_ctx)
            .into_iter()
            .filter_map(move |name| self.system_catalog.table(catalog, schema, &name, query_ctx))
    }

    fn user_table_ids<'a>(
        &'a self,
        catalog: &'a str,
        schema: &'a str,
    ) -> impl Stream<Item = Result<TableId>> + 'a {
        self.table_metadata_manager
            .table_name_manager()
            .tables(catalog, schema)
            .map(move |entry| {
                entry
                    .map(|(_, value)| value.table_id())
                    .map_err(BoxedError::new)
                    .context(ListTablesSnafu { catalog, schema })
            })
    }

    fn table_caches(&self) -> Result<(TableInfoCacheRef, TableRouteCacheRef)> {
        let routes = self.cache_registry.get().context(CacheNotFoundSnafu {
            name: "table_route_cache",
        })?;
        let infos = self.cache_registry.get().context(CacheNotFoundSnafu {
            name: "table_info_cache",
        })?;
        Ok((infos, routes))
    }

    /// Yields a complete table batch before starting the next metadata load.
    pub(crate) fn sequential_table_batches<'a>(
        &'a self,
        catalog: &'a str,
        schema: &'a str,
    ) -> impl Stream<Item = Result<impl Iterator<Item = TableRef> + 'a>> + 'a {
        const BATCH_SIZE: usize = 128;
        try_stream! {
            yield Either::Left(self.system_tables(catalog, schema, None));

            let (infos, routes) = self.table_caches()?;
            let mut batches = self.user_table_ids(catalog, schema).ready_chunks(BATCH_SIZE);
            while let Some(batch) = batches.next().await {
                let ids = batch.into_iter().collect::<Result<Vec<_>>>()?;
                yield Either::Right(Self::load_tables(ids, &infos, &routes).await?);
            }
        }
    }
}

fn build_table(table_info_value: TableInfoValue) -> TableRef {
    let table_info = table_info_value.table_info;
    DistTable::table(Arc::new(table_info))
}

// TODO: This struct can hold a static map of all system tables when
// the upper layer (e.g., procedure) can inform the catalog manager
// a new catalog is created.
/// Existing system tables:
/// - public.numbers
/// - information_schema.{tables}
/// - pg_catalog.{tables}
#[derive(Clone)]
pub(super) struct SystemCatalog {
    pub(super) catalog_manager: Weak<KvBackendCatalogManager>,
    pub(super) catalog_cache: Cache<String, Arc<InformationSchemaProvider>>,
    pub(super) pg_catalog_cache: Cache<String, Arc<PGCatalogProvider>>,

    // system_schema_provider for default catalog
    pub(super) information_schema_provider: Arc<InformationSchemaProvider>,
    pub(super) pg_catalog_provider: Arc<PGCatalogProvider>,
    pub(super) numbers_table_provider: NumbersTableProvider,
    pub(super) backend: KvBackendRef,
    pub(super) process_manager: Option<ProcessManagerRef>,
    pub(super) extra_information_table_factories:
        std::collections::HashMap<String, InformationSchemaTableFactoryRef>,
}

impl SystemCatalog {
    fn schema_names(&self, query_ctx: Option<&QueryContext>) -> Vec<String> {
        let channel = query_ctx.map_or(Channel::Unknown, |ctx| ctx.channel());
        match channel {
            // pg_catalog only visible under postgres protocol
            Channel::Postgres => vec![
                INFORMATION_SCHEMA_NAME.to_string(),
                PG_CATALOG_NAME.to_string(),
            ],
            _ => {
                vec![INFORMATION_SCHEMA_NAME.to_string()]
            }
        }
    }

    fn table_names(&self, schema: &str, query_ctx: Option<&QueryContext>) -> Vec<String> {
        let channel = query_ctx.map_or(Channel::Unknown, |ctx| ctx.channel());
        match schema {
            INFORMATION_SCHEMA_NAME => self.information_schema_provider.table_names(),
            PG_CATALOG_NAME if channel == Channel::Postgres => {
                self.pg_catalog_provider.table_names()
            }
            DEFAULT_SCHEMA_NAME => self.numbers_table_provider.table_names(),
            // Computed entity-graph tables overlay the physical tables of
            // `greptime_private` (the caller appends these to the physical list).
            DEFAULT_PRIVATE_SCHEMA_NAME => SemanticGraphTableProvider::table_names(),
            _ => vec![],
        }
    }

    fn schema_exists(&self, schema: &str, query_ctx: Option<&QueryContext>) -> bool {
        let channel = query_ctx.map_or(Channel::Unknown, |ctx| ctx.channel());
        match channel {
            Channel::Postgres => schema == PG_CATALOG_NAME || schema == INFORMATION_SCHEMA_NAME,
            _ => schema == INFORMATION_SCHEMA_NAME,
        }
    }

    fn table_exists(&self, schema: &str, table: &str, query_ctx: Option<&QueryContext>) -> bool {
        let channel = query_ctx.map_or(Channel::Unknown, |ctx| ctx.channel());
        if schema == INFORMATION_SCHEMA_NAME {
            self.information_schema_provider.table(table).is_some()
        } else if schema == DEFAULT_SCHEMA_NAME {
            self.numbers_table_provider.table_exists(table)
        } else if schema == PG_CATALOG_NAME && channel == Channel::Postgres {
            self.pg_catalog_provider.table(table).is_some()
        } else if schema == DEFAULT_PRIVATE_SCHEMA_NAME {
            SemanticGraphTableProvider::table_exists(table)
        } else {
            false
        }
    }

    fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Option<TableRef> {
        let channel = query_ctx.map_or(Channel::Unknown, |ctx| ctx.channel());
        if schema == INFORMATION_SCHEMA_NAME {
            // `session_variables` reports per-session state, which the cached
            // provider cannot hold. Rebuild it per resolution so it reads the
            // resolving query's context, the way the computed entity-graph
            // tables do. Everything else comes from the per-catalog cache.
            if table_name.eq_ignore_ascii_case(SESSION_VARIABLES) {
                let query_options = self
                    .catalog_manager
                    .upgrade()
                    .and_then(|manager| manager.query_option_value_provider());
                return Some(InformationSchemaSessionVariables::build_table(
                    catalog,
                    query_ctx.cloned().map(Arc::new),
                    query_options,
                ));
            }

            let information_schema_provider =
                self.catalog_cache.get_with_by_ref(catalog, move || {
                    let provider = InformationSchemaProvider::new(
                        catalog.to_string(),
                        self.catalog_manager.clone(),
                        Arc::new(FlowMetadataManager::new(self.backend.clone())),
                        self.process_manager.clone(),
                        self.backend.clone(),
                    );
                    let provider = provider
                        .with_extra_table_factories(self.extra_information_table_factories.clone());
                    Arc::new(provider)
                });
            information_schema_provider.table(table_name)
        } else if schema == PG_CATALOG_NAME && channel == Channel::Postgres {
            if catalog == DEFAULT_CATALOG_NAME {
                self.pg_catalog_provider.table(table_name)
            } else {
                let pg_catalog_provider =
                    self.pg_catalog_cache.get_with_by_ref(catalog, move || {
                        Arc::new(PGCatalogProvider::new(
                            catalog.to_string(),
                            self.catalog_manager.clone(),
                        ))
                    });
                pg_catalog_provider.table(table_name)
            }
        } else if schema == DEFAULT_SCHEMA_NAME {
            self.numbers_table_provider.table(table_name)
        } else if schema == DEFAULT_PRIVATE_SCHEMA_NAME
            && SemanticGraphTableProvider::table_exists(table_name)
        {
            // Constructed on demand (the provider is a name + a weak ref); the
            // system catalog is consulted before physical resolution, so the
            // computed tables shadow same-named physical tables by design.
            SemanticGraphTableProvider::new(
                catalog.to_string(),
                self.catalog_manager.clone(),
                query_ctx.cloned().map(Arc::new),
            )
            .table(table_name)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common_catalog::consts::NUMBERS_TABLE_ID;
    use common_meta::cache::{
        CacheContainer, CacheRegistryBuilder, Initializer, LayeredCacheRegistryBuilder,
    };
    use common_meta::ddl::test_util::create_table::test_create_table_task;
    use common_meta::instruction::CacheIdent;
    use common_meta::key::table_route::{LogicalTableRouteValue, TableRouteValue};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use moka::future::Cache as FutureCache;
    use partition::cache::PartitionInfoCacheRef;
    use table::metadata::TableInfo;
    use table::table::numbers::NUMBERS_TABLE_NAME;

    use super::*;

    fn batch_lookup_manager(
        backend: KvBackendRef,
        registry: LayeredCacheRegistryRef,
    ) -> Arc<KvBackendCatalogManager> {
        crate::kvbackend::KvBackendCatalogManagerBuilder::new(
            Arc::new(crate::information_schema::NoopInformationExtension),
            backend,
            registry,
        )
        .build()
    }

    #[tokio::test]
    async fn test_batch_lookups_without_table_caches() {
        let backend = Arc::new(MemoryKvBackend::default());
        let caches = cache::build_datanode_layered_cache_registry(backend.clone());
        let routing = CacheRegistryBuilder::default()
            .add_cache(caches.get::<TableRouteCacheRef>().unwrap())
            .add_cache(caches.get::<PartitionInfoCacheRef>().unwrap())
            .build();
        let registry = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(routing)
            .build();
        let manager = batch_lookup_manager(backend, Arc::new(registry));
        assert!(manager.table_ids(&[]).await.unwrap().is_empty());
        assert!(manager.table_infos_by_ids(&[]).await.unwrap().is_empty());
        let numbers = TableName::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            NUMBERS_TABLE_NAME,
        );
        assert_eq!(
            manager.table_ids(&[numbers]).await.unwrap(),
            [Some(NUMBERS_TABLE_ID)]
        );
    }

    #[tokio::test]
    async fn test_batch_lookups_preserve_positions_and_system_table_precedence() {
        let backend = Arc::new(MemoryKvBackend::default());
        let registry = cache::build_datanode_layered_cache_registry(backend.clone());
        let manager = batch_lookup_manager(backend, Arc::new(registry));
        for (name, id) in [
            ("warm", 1024),
            ("cold", 1025),
            (NUMBERS_TABLE_NAME, 1026),
            ("other", 1027),
        ] {
            manager
                .table_metadata_manager
                .create_table_metadata(
                    test_create_table_task(name, id).table_info,
                    TableRouteValue::physical(vec![]),
                    Default::default(),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            manager
                .table_id(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "warm", None)
                .await
                .unwrap(),
            Some(1024)
        );
        assert_eq!(
            manager.table_info_by_id(1024).await.unwrap().unwrap().name,
            "warm"
        );
        let names = [
            "other",
            NUMBERS_TABLE_NAME,
            "cold",
            "missing",
            "warm",
            "other",
        ]
        .map(|name| TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, name));
        assert_eq!(
            manager.table_ids(&names).await.unwrap(),
            [
                Some(1027),
                Some(NUMBERS_TABLE_ID),
                Some(1025),
                None,
                Some(1024),
                Some(1027)
            ]
        );
        let infos = manager
            .table_infos_by_ids(&[1026, 4040, 1024, 1025, 1026])
            .await
            .unwrap();
        let entries: Vec<_> = infos
            .iter()
            .map(|info| {
                info.as_ref()
                    .map(|info| (info.table_id(), info.name.as_str()))
            })
            .collect();
        assert_eq!(
            entries,
            [
                Some((1026, NUMBERS_TABLE_NAME)),
                None,
                Some((1024, "warm")),
                Some((1025, "cold")),
                Some((1026, NUMBERS_TABLE_NAME))
            ]
        );
    }

    #[tokio::test]
    async fn test_schema_options_preserve_user_options_and_system_schema() {
        use std::time::Duration;

        use common_meta::key::schema_name::SchemaNameValue;
        use datatypes::arrow_array::string_array_value_at_index;

        let backend = Arc::new(MemoryKvBackend::default());
        let registry = cache::build_datanode_layered_cache_registry(backend.clone());
        let manager = batch_lookup_manager(backend, Arc::new(registry));
        let options = SchemaNameValue {
            ttl: Some(Duration::from_secs(9).into()),
            extra_options: [("region".into(), "east".into())].into(),
            ..Default::default()
        };
        manager
            .table_metadata_manager
            .schema_manager()
            .create(
                SchemaNameKey::new(DEFAULT_CATALOG_NAME, "user_schema"),
                Some(options),
                true,
            )
            .await
            .unwrap();
        let schemata = manager
            .table(
                DEFAULT_CATALOG_NAME,
                INFORMATION_SCHEMA_NAME,
                "schemata",
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let batches: Vec<_> = schemata
            .scan_to_stream(Default::default())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            let names = batch.column_by_name("schema_name").unwrap();
            let options = batch.column_by_name("options").unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    string_array_value_at_index(names, row),
                    string_array_value_at_index(options, row),
                ));
            }
        }
        rows.sort_unstable();
        assert_eq!(
            rows,
            [
                (Some(INFORMATION_SCHEMA_NAME), Some("")),
                (Some("user_schema"), Some("'ttl'='9s'\n'region'='east'\n")),
            ]
        );
    }

    #[tokio::test]
    async fn test_tables_reuse_batch_loaded_metadata() {
        use std::collections::HashMap;
        use std::sync::Mutex;

        use common_meta::key::table_info::TableInfoKey;
        use common_meta::key::table_name::TableNameValue;
        use common_meta::key::{MetadataKey, MetadataValue};
        use common_meta::kv_backend::test_util::MockKvBackendBuilder;
        use common_meta::rpc::KeyValue;
        use common_meta::rpc::store::{BatchGetResponse, RangeResponse};

        let mut names = Vec::new();
        let mut infos = HashMap::new();
        for id in [1024, 1025] {
            let name = format!("table_{id}");
            names.push(KeyValue {
                key: TableNameKey::new(DEFAULT_CATALOG_NAME, "scan_test", &name).to_bytes(),
                value: TableNameValue::new(id).try_as_raw_value().unwrap(),
            });
            let info = test_create_table_task(&name, id).table_info;
            infos.insert(
                TableInfoKey::new(id).to_bytes(),
                TableInfoValue::new(info).try_as_raw_value().unwrap(),
            );
        }
        let reads = Arc::new(Mutex::new(Vec::new()));
        let observed = reads.clone();
        let backend = MockKvBackendBuilder::default()
            .range_fn(Arc::new(move |_| {
                Ok(RangeResponse {
                    kvs: names.clone(),
                    more: false,
                })
            }))
            .batch_get_fn(Arc::new(move |req| {
                let mut keys = req.keys;
                keys.sort();
                observed.lock().unwrap().push(keys.clone());
                Ok(BatchGetResponse {
                    kvs: keys
                        .into_iter()
                        .map(|key| KeyValue {
                            value: infos[&key].clone(),
                            key,
                        })
                        .collect(),
                })
            }))
            .max_txn_ops(128)
            .build()
            .unwrap();
        let backend = Arc::new(backend);
        let registry = cache::build_datanode_layered_cache_registry(backend.clone());
        let manager = batch_lookup_manager(backend, Arc::new(registry));
        let ids = [1024, 1025];
        let keys = ids.map(|id| TableInfoKey::new(id).to_bytes()).to_vec();
        for _ in 0..3 {
            let baseline = manager
                .table_metadata_manager
                .table_info_manager()
                .batch_get(&ids)
                .await
                .unwrap();
            assert_eq!(baseline.len(), ids.len());
        }
        assert_eq!(
            std::mem::take(&mut *reads.lock().unwrap()),
            vec![keys.clone(); 3]
        );
        for _ in 0..3 {
            let ids_read: Vec<_> = manager
                .tables(DEFAULT_CATALOG_NAME, "scan_test", None)
                .map_ok(|table| table.table_info().table_id())
                .try_collect()
                .await
                .unwrap();
            assert_eq!(ids_read, ids);
        }
        assert_eq!(*reads.lock().unwrap(), vec![keys]);
    }

    #[tokio::test]
    async fn test_views_scan_yields_incremental_batches() {
        let backend = Arc::new(MemoryKvBackend::default());
        let registry = cache::with_default_composite_cache_registry(
            LayeredCacheRegistryBuilder::default()
                .add_cache_registry(cache::build_fundamental_cache_registry(backend.clone())),
        )
        .unwrap()
        .build();
        let manager = batch_lookup_manager(backend, Arc::new(registry));
        let views = manager
            .table(DEFAULT_CATALOG_NAME, INFORMATION_SCHEMA_NAME, "views", None)
            .await
            .unwrap()
            .unwrap();
        let empty: Vec<_> = views
            .scan_to_stream(Default::default())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].num_rows(), 0);
        manager
            .table_metadata_manager
            .schema_manager()
            .create(
                SchemaNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
                None,
                true,
            )
            .await
            .unwrap();
        for id in 1024..1153 {
            let name = format!("view_{id}");
            let mut info = test_create_table_task(&name, id).table_info;
            info.catalog_name = DEFAULT_CATALOG_NAME.into();
            info.schema_name = DEFAULT_SCHEMA_NAME.into();
            info.table_type = table::metadata::TableType::View;
            manager
                .table_metadata_manager
                .create_view_metadata(
                    info,
                    vec![],
                    Default::default(),
                    vec![],
                    vec![],
                    format!("SELECT {id}"),
                )
                .await
                .unwrap();
        }
        let batches: Vec<_> = views
            .scan_to_stream(Default::default())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(batches.len() > 1);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            129
        );
        let cache = manager.view_info_cache().unwrap();
        assert!(cache.contains_key(&1024));
        assert!(cache.contains_key(&1152));
    }

    fn metric_table_info(
        table_id: TableId,
        table_name: &str,
        partition_key_indices: Vec<usize>,
    ) -> TableInfo {
        let mut table_info = test_create_table_task(table_name, table_id).table_info;
        table_info.meta.engine = METRIC_ENGINE_NAME.to_string();
        table_info.meta.partition_key_indices = partition_key_indices;
        table_info
    }

    #[tokio::test]
    async fn test_logical_table_uses_cached_physical_table_info() {
        const LOGICAL_TABLE_ID: TableId = 1;
        const PHYSICAL_TABLE_ID: TableId = 2;

        let route = Arc::new(TableRoute::Logical(Arc::new(LogicalTableRouteValue::new(
            PHYSICAL_TABLE_ID,
        ))));
        let route_initializer: Initializer<TableId, Arc<TableRoute>> = Arc::new(move |_| {
            let route = route.clone();
            Box::pin(async move { Ok(Some(route)) })
        });
        let table_route_cache = Arc::new(CacheContainer::new(
            "test_table_route_cache".to_string(),
            FutureCache::new(16),
            route_initializer,
            CacheIdent::table_id_key,
        ));

        let load_count = Arc::new(AtomicUsize::new(0));
        let physical_table_info =
            Arc::new(metric_table_info(PHYSICAL_TABLE_ID, "physical", vec![1]));
        let table_info_initializer: Initializer<TableId, Arc<TableInfo>> = {
            let load_count = load_count.clone();
            let table_info = physical_table_info.clone();
            Arc::new(move |_| {
                load_count.fetch_add(1, Ordering::Relaxed);
                let table_info = table_info.clone();
                Box::pin(async move { Ok(Some(table_info)) })
            })
        };
        let table_info_cache = Arc::new(CacheContainer::new(
            "test_table_info_cache".to_string(),
            FutureCache::new(16),
            table_info_initializer,
            CacheIdent::table_id_key,
        ));

        let logical_table = DistTable::table(Arc::new(metric_table_info(
            LOGICAL_TABLE_ID,
            "logical",
            vec![],
        )));
        for _ in 0..2 {
            let table = KvBackendCatalogManager::override_logical_table_partition_key_indices(
                &table_route_cache,
                &table_info_cache,
                logical_table.clone(),
            )
            .await
            .unwrap();
            assert_eq!(table.table_info().meta.partition_key_indices, vec![1]);
        }
        assert_eq!(load_count.load(Ordering::Relaxed), 1);
    }
}
