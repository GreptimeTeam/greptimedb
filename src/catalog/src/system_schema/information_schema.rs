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

mod cluster_info;
pub mod columns;
pub mod flow_statistics;
pub mod flows;
mod information_memory_table;
pub mod key_column_usage;
mod partitions;
mod procedure_info;
pub mod process_list;
#[cfg(feature = "enterprise")]
mod recycle_bin;
mod region_info;
pub mod region_peers;
mod region_statistics;
pub mod schemata;
mod ssts;
pub mod statistics;
mod table_constraints;
mod table_names;
mod table_semantics;
pub mod tables;
mod views;

#[cfg(all(test, feature = "enterprise"))]
mod recycle_bin_test;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use common_catalog::consts::{self, DEFAULT_CATALOG_NAME, INFORMATION_SCHEMA_NAME};
use common_error::ext::ErrorExt;
use common_meta::cluster::NodeInfo;
use common_meta::datanode::RegionStat;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::flow::flow_state::FlowStat;
use common_meta::kv_backend::KvBackendRef;
use common_procedure::ProcedureInfo;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datatypes::schema::SchemaRef;
use lazy_static::lazy_static;
use paste::paste;
use process_list::InformationSchemaProcessList;
use region_info::InformationSchemaRegionInfo;
use store_api::metric_engine_consts::{
    MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING, PRIMARY_KEY_ENCODING,
};
use store_api::region_info::RegionInfoEntry;
use store_api::sst_entry::{ManifestSstEntry, PuffinIndexMetaEntry, StorageSstEntry};
use store_api::storage::{ScanRequest, TableId};
use table::metadata::{FilterPushDownType, TableType};
use table::requests::SEMANTIC_TABLE_SCOPE;
use table::{Table, TableRef};
pub use table_names::*;
use views::InformationSchemaViews;

use self::columns::InformationSchemaColumns;
use crate::CatalogManager;
use crate::error::{Error, Result};
use crate::process_manager::ProcessManagerRef;
use crate::system_schema::information_schema::cluster_info::InformationSchemaClusterInfo;
use crate::system_schema::information_schema::flow_statistics::InformationSchemaFlowStatistics;
use crate::system_schema::information_schema::flows::InformationSchemaFlows;
use crate::system_schema::information_schema::information_memory_table::get_schema_columns;
use crate::system_schema::information_schema::key_column_usage::InformationSchemaKeyColumnUsage;
use crate::system_schema::information_schema::partitions::InformationSchemaPartitions;
#[cfg(feature = "enterprise")]
use crate::system_schema::information_schema::recycle_bin::InformationSchemaRecycleBin;
use crate::system_schema::information_schema::region_peers::InformationSchemaRegionPeers;
use crate::system_schema::information_schema::schemata::InformationSchemaSchemata;
use crate::system_schema::information_schema::ssts::{
    InformationSchemaSstsIndexMeta, InformationSchemaSstsManifest, InformationSchemaSstsStorage,
};
use crate::system_schema::information_schema::statistics::InformationSchemaStatistics;
use crate::system_schema::information_schema::table_constraints::InformationSchemaTableConstraints;
use crate::system_schema::information_schema::table_semantics::InformationSchemaTableSemantics;
use crate::system_schema::information_schema::tables::InformationSchemaTables;
use crate::system_schema::memory_table::MemoryTable;
pub(crate) use crate::system_schema::predicate::Predicates;
use crate::system_schema::{
    SystemSchemaProvider, SystemSchemaProviderInner, SystemTable, SystemTableRef,
};

const DENSE_PRIMARY_KEY_ENCODING: &str = "dense";
const SPARSE_PRIMARY_KEY_ENCODING: &str = "sparse";

pub(crate) fn primary_key_encoding_index_type(options: &HashMap<String, String>) -> &'static str {
    options
        .get(PRIMARY_KEY_ENCODING)
        .or_else(|| options.get(MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING))
        .map(|value| {
            if value.eq_ignore_ascii_case(SPARSE_PRIMARY_KEY_ENCODING) {
                SPARSE_PRIMARY_KEY_ENCODING
            } else {
                DENSE_PRIMARY_KEY_ENCODING
            }
        })
        .unwrap_or(DENSE_PRIMARY_KEY_ENCODING)
}

lazy_static! {
    // Memory tables in `information_schema`.
    static ref MEMORY_TABLES: &'static [&'static str] = &[
        ENGINES,
        COLUMN_PRIVILEGES,
        COLUMN_STATISTICS,
        CHARACTER_SETS,
        COLLATIONS,
        COLLATION_CHARACTER_SET_APPLICABILITY,
        CHECK_CONSTRAINTS,
        EVENTS,
        FILES,
        OPTIMIZER_TRACE,
        PARAMETERS,
        PROFILING,
        REFERENTIAL_CONSTRAINTS,
        ROUTINES,
        SCHEMA_PRIVILEGES,
        TABLE_PRIVILEGES,
        GLOBAL_STATUS,
        SESSION_STATUS,
        PLUGINS,
        USER_PRIVILEGES,
        PROCESSLIST,
    ];
}

macro_rules! setup_memory_table {
    ($name: expr) => {
        paste! {
            {
                let (schema, columns) = get_schema_columns($name);
                Some(Arc::new(MemoryTable::new(
                    consts::[<INFORMATION_SCHEMA_ $name  _TABLE_ID>],
                    $name,
                    schema,
                    columns
                )) as _)
            }
        }
    };
}

pub struct MakeInformationTableRequest {
    pub catalog_name: String,
    pub catalog_manager: Weak<dyn CatalogManager>,
    pub kv_backend: KvBackendRef,
}

/// The visibility guaranteed by an information-schema table provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InformationSchemaTableScope {
    /// Every query is filtered to the catalog supplied when constructing the provider.
    Catalog,
    /// The table contains catalog-independent or shared data, including compatibility stubs.
    Global,
}

impl InformationSchemaTableScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Global => "global",
        }
    }
}

/// A factory trait for making information schema tables.
///
/// This trait allows for extensibility of the information schema by providing
/// a way to dynamically create custom information schema tables.
pub trait InformationSchemaTableFactory {
    /// Returns the visibility guaranteed by every table created by this factory.
    fn scope(&self) -> InformationSchemaTableScope;

    fn make_information_table(&self, req: MakeInformationTableRequest) -> SystemTableRef;
}

pub type InformationSchemaTableFactoryRef = Arc<dyn InformationSchemaTableFactory + Send + Sync>;

/// The `information_schema` tables info provider.
pub struct InformationSchemaProvider {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    process_manager: Option<ProcessManagerRef>,
    flow_metadata_manager: Arc<FlowMetadataManager>,
    tables: HashMap<String, TableRef>,
    kv_backend: KvBackendRef,
    extra_table_factories: HashMap<String, InformationSchemaTableFactoryRef>,
}

impl SystemSchemaProvider for InformationSchemaProvider {
    fn tables(&self) -> &HashMap<String, TableRef> {
        assert!(!self.tables.is_empty());

        &self.tables
    }
}

impl SystemSchemaProviderInner for InformationSchemaProvider {
    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
    fn schema_name() -> &'static str {
        INFORMATION_SCHEMA_NAME
    }

    fn system_table(&self, name: &str) -> Option<SystemTableRef> {
        if let Some(factory) = self.extra_table_factories.get(name) {
            let req = MakeInformationTableRequest {
                catalog_name: self.catalog_name.clone(),
                catalog_manager: self.catalog_manager.clone(),
                kv_backend: self.kv_backend.clone(),
            };
            return Some(factory.make_information_table(req));
        }

        match name.to_ascii_lowercase().as_str() {
            TABLES => Some(Arc::new(InformationSchemaTables::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            COLUMNS => Some(Arc::new(InformationSchemaColumns::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            ENGINES => setup_memory_table!(ENGINES),
            COLUMN_PRIVILEGES => setup_memory_table!(COLUMN_PRIVILEGES),
            COLUMN_STATISTICS => setup_memory_table!(COLUMN_STATISTICS),
            BUILD_INFO => setup_memory_table!(BUILD_INFO),
            CHARACTER_SETS => setup_memory_table!(CHARACTER_SETS),
            COLLATIONS => setup_memory_table!(COLLATIONS),
            COLLATION_CHARACTER_SET_APPLICABILITY => {
                setup_memory_table!(COLLATION_CHARACTER_SET_APPLICABILITY)
            }
            CHECK_CONSTRAINTS => setup_memory_table!(CHECK_CONSTRAINTS),
            EVENTS => setup_memory_table!(EVENTS),
            FILES => setup_memory_table!(FILES),
            OPTIMIZER_TRACE => setup_memory_table!(OPTIMIZER_TRACE),
            PARAMETERS => setup_memory_table!(PARAMETERS),
            PROFILING => setup_memory_table!(PROFILING),
            REFERENTIAL_CONSTRAINTS => setup_memory_table!(REFERENTIAL_CONSTRAINTS),
            ROUTINES => setup_memory_table!(ROUTINES),
            SCHEMA_PRIVILEGES => setup_memory_table!(SCHEMA_PRIVILEGES),
            TABLE_PRIVILEGES => setup_memory_table!(TABLE_PRIVILEGES),
            GLOBAL_STATUS => setup_memory_table!(GLOBAL_STATUS),
            SESSION_STATUS => setup_memory_table!(SESSION_STATUS),
            PLUGINS => setup_memory_table!(PLUGINS),
            USER_PRIVILEGES => setup_memory_table!(USER_PRIVILEGES),
            PROCESSLIST => setup_memory_table!(PROCESSLIST),
            KEY_COLUMN_USAGE => Some(Arc::new(InformationSchemaKeyColumnUsage::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            SCHEMATA => Some(Arc::new(InformationSchemaSchemata::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            PARTITIONS => Some(Arc::new(InformationSchemaPartitions::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            REGION_PEERS => Some(Arc::new(InformationSchemaRegionPeers::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            TABLE_CONSTRAINTS => Some(Arc::new(InformationSchemaTableConstraints::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            STATISTICS => Some(Arc::new(InformationSchemaStatistics::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            CLUSTER_INFO => Some(Arc::new(InformationSchemaClusterInfo::new(
                self.catalog_manager.clone(),
            )) as _),
            VIEWS => Some(Arc::new(InformationSchemaViews::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            FLOWS => Some(Arc::new(InformationSchemaFlows::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
                self.flow_metadata_manager.clone(),
            )) as _),
            FLOW_STATISTICS => Some(Arc::new(InformationSchemaFlowStatistics::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
                self.flow_metadata_manager.clone(),
            )) as _),
            PROCEDURE_INFO => Some(
                Arc::new(procedure_info::InformationSchemaProcedureInfo::new(
                    self.catalog_manager.clone(),
                )) as _,
            ),
            #[cfg(feature = "enterprise")]
            RECYCLE_BIN => Some(Arc::new(InformationSchemaRecycleBin::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            REGION_STATISTICS => Some(Arc::new(
                region_statistics::InformationSchemaRegionStatistics::new(
                    self.catalog_manager.clone(),
                ),
            ) as _),
            REGION_INFO => Some(Arc::new(InformationSchemaRegionInfo::new(
                self.catalog_manager.clone(),
            )) as _),
            PROCESS_LIST => self
                .process_manager
                .as_ref()
                .map(|p| Arc::new(InformationSchemaProcessList::new(p.clone())) as _),
            SSTS_MANIFEST => Some(Arc::new(InformationSchemaSstsManifest::new(
                self.catalog_manager.clone(),
            )) as _),
            SSTS_STORAGE => Some(Arc::new(InformationSchemaSstsStorage::new(
                self.catalog_manager.clone(),
            )) as _),
            SSTS_INDEX_META => Some(Arc::new(InformationSchemaSstsIndexMeta::new(
                self.catalog_manager.clone(),
            )) as _),
            TABLE_SEMANTICS => Some(Arc::new(InformationSchemaTableSemantics::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            _ => None,
        }
    }
}

impl InformationSchemaProvider {
    pub fn new(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
        flow_metadata_manager: Arc<FlowMetadataManager>,
        process_manager: Option<ProcessManagerRef>,
        kv_backend: KvBackendRef,
    ) -> Self {
        let mut provider = Self {
            catalog_name,
            catalog_manager,
            flow_metadata_manager,
            process_manager,
            tables: HashMap::new(),
            kv_backend,
            extra_table_factories: HashMap::new(),
        };

        provider.build_tables();

        provider
    }

    pub(crate) fn with_extra_table_factories(
        mut self,
        factories: HashMap<String, InformationSchemaTableFactoryRef>,
    ) -> Self {
        self.extra_table_factories = factories;
        self.build_tables();
        self
    }

    fn build_tables(&mut self) {
        use InformationSchemaTableScope::{Catalog, Global};

        // Registration requires a scope; metadata is attached centrally below.
        let mut table_scopes = HashMap::new();

        // SECURITY NOTE:
        // Carefully consider the tables that may expose sensitive cluster configurations,
        // authentication details, and other critical information.
        // Only put these tables under `greptime` catalog to prevent info leak.
        if self.catalog_name == DEFAULT_CATALOG_NAME {
            for (name, scope) in [
                (BUILD_INFO, Global),
                (REGION_PEERS, Catalog),
                (CLUSTER_INFO, Global),
                (PROCEDURE_INFO, Global),
                (REGION_STATISTICS, Global),
                (REGION_INFO, Global),
                (SSTS_MANIFEST, Global),
                (SSTS_STORAGE, Global),
                (SSTS_INDEX_META, Global),
            ] {
                table_scopes.insert(name, scope);
            }
        }

        for name in [
            TABLES,
            VIEWS,
            SCHEMATA,
            COLUMNS,
            KEY_COLUMN_USAGE,
            TABLE_CONSTRAINTS,
            STATISTICS,
            FLOWS,
            FLOW_STATISTICS,
            #[cfg(feature = "enterprise")]
            RECYCLE_BIN,
            TABLE_SEMANTICS,
            PARTITIONS,
        ] {
            table_scopes.insert(name, Catalog);
        }
        if self.process_manager.is_some() {
            table_scopes.insert(PROCESS_LIST, Global);
        }
        // Add memory tables
        for name in MEMORY_TABLES.iter() {
            table_scopes.insert(*name, Global);
        }
        for (name, factory) in &self.extra_table_factories {
            table_scopes.insert(name.as_str(), factory.scope());
        }
        self.tables = table_scopes
            .into_iter()
            .map(|(name, scope)| {
                let table = self.build_table(name).expect(name);
                let mut info = (*table.table_info()).clone();
                info.meta
                    .options
                    .extra_options
                    .insert(SEMANTIC_TABLE_SCOPE.to_string(), scope.as_str().to_string());
                let table = Arc::new(Table::new(
                    Arc::new(info),
                    FilterPushDownType::Inexact,
                    table.data_source(),
                ));
                (name.to_string(), table)
            })
            .collect();
    }
}

pub trait InformationTable {
    fn table_id(&self) -> TableId;

    fn table_name(&self) -> &'static str;

    fn schema(&self) -> SchemaRef;

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream>;

    fn scan_plan(&self, _request: ScanRequest) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}

// Provide compatibility for legacy `information_schema` code.
impl<T> SystemTable for T
where
    T: InformationTable,
{
    fn table_id(&self) -> TableId {
        InformationTable::table_id(self)
    }

    fn table_name(&self) -> &'static str {
        InformationTable::table_name(self)
    }

    fn schema(&self) -> SchemaRef {
        InformationTable::schema(self)
    }

    fn table_type(&self) -> TableType {
        InformationTable::table_type(self)
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        InformationTable::to_stream(self, request)
    }

    fn scan_plan(&self, request: ScanRequest) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        InformationTable::scan_plan(self, request)
    }
}

pub type InformationExtensionRef = Arc<dyn InformationExtension<Error = Error> + Send + Sync>;

/// The `InformationExtension` trait provides the extension methods for the `information_schema` tables.
#[async_trait::async_trait]
pub trait InformationExtension {
    type Error: ErrorExt;

    /// Gets the nodes information.
    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error>;

    /// Gets the procedures information.
    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error>;

    /// Gets the region statistics.
    async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error>;

    /// Get the flow statistics. If no flownode is available, return `None`.
    async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error>;

    /// Inspects the datanode.
    async fn inspect_datanode(
        &self,
        request: DatanodeInspectRequest,
    ) -> std::result::Result<SendableRecordBatchStream, Self::Error>;

    /// Builds a physical plan for datanode inspect if the extension can expose
    /// the distributed fan-in semantics to DataFusion.
    fn inspect_datanode_plan(
        &self,
        _request: DatanodeInspectRequest,
        _schema: SchemaRef,
    ) -> std::result::Result<Option<Arc<dyn ExecutionPlan>>, Self::Error> {
        Ok(None)
    }
}

/// The request to inspect the datanode.
#[derive(Debug, Clone, PartialEq)]
pub struct DatanodeInspectRequest {
    /// Kind to fetch from datanode.
    pub kind: DatanodeInspectKind,

    /// Pushdown scan configuration (projection/predicate/limit) for the returned stream.
    /// This allows server-side filtering to reduce I/O and network costs.
    pub scan: ScanRequest,
}

/// The kind of the datanode inspect request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatanodeInspectKind {
    /// List SST entries recorded in manifest
    SstManifest,
    /// List SST entries discovered in storage layer
    SstStorage,
    /// List index metadata collected from manifest
    SstIndexMeta,
    /// List region runtime and manifest info
    RegionInfo,
}

impl DatanodeInspectRequest {
    /// Builds a logical plan for the datanode inspect request.
    pub fn build_plan(self) -> std::result::Result<LogicalPlan, DataFusionError> {
        match self.kind {
            DatanodeInspectKind::SstManifest => ManifestSstEntry::build_plan(self.scan),
            DatanodeInspectKind::SstStorage => StorageSstEntry::build_plan(self.scan),
            DatanodeInspectKind::SstIndexMeta => PuffinIndexMetaEntry::build_plan(self.scan),
            DatanodeInspectKind::RegionInfo => RegionInfoEntry::build_plan(self.scan),
        }
    }
}
pub struct NoopInformationExtension;

#[async_trait::async_trait]
impl InformationExtension for NoopInformationExtension {
    type Error = Error;

    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
        Ok(vec![])
    }

    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
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
        Ok(common_recordbatch::RecordBatches::empty().as_stream())
    }
}

#[cfg(test)]
mod tests {
    use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
    use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
    use common_meta::ddl::test_util::create_table::test_create_table_task;
    use common_meta::key::schema_name::SchemaNameKey;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::txn::TxnService;
    use common_meta::wal_provider::RegionWalOptions;
    use datatypes::schema::Schema;
    use futures_util::TryStreamExt;
    use store_api::region_info::RegionInfoEntry;

    use super::*;
    use crate::information_schema::NoopInformationExtension;
    use crate::kvbackend::KvBackendCatalogManagerBuilder;
    use crate::memory::MemoryCatalogManager;
    use crate::process_manager::ProcessManager;

    struct ScopedFactory {
        name: &'static str,
        scope: InformationSchemaTableScope,
    }

    impl InformationSchemaTableFactory for ScopedFactory {
        fn scope(&self) -> InformationSchemaTableScope {
            self.scope
        }

        fn make_information_table(&self, _req: MakeInformationTableRequest) -> SystemTableRef {
            Arc::new(MemoryTable::new(
                999,
                self.name,
                Arc::new(Schema::new(vec![])),
                vec![],
            ))
        }
    }

    #[test]
    fn every_registered_information_schema_table_has_scope() {
        use InformationSchemaTableScope::{Catalog, Global};

        let manager: Arc<dyn CatalogManager> = MemoryCatalogManager::new();
        let backend = Arc::new(MemoryKvBackend::default());
        let factories: HashMap<_, InformationSchemaTableFactoryRef> = [
            ("future_catalog_table", Catalog),
            ("future_global_table", Global),
        ]
        .into_iter()
        .map(|(name, scope)| {
            (
                name.to_string(),
                Arc::new(ScopedFactory { name, scope }) as _,
            )
        })
        .collect();
        let catalog_tables = [
            TABLES,
            VIEWS,
            SCHEMATA,
            COLUMNS,
            KEY_COLUMN_USAGE,
            TABLE_CONSTRAINTS,
            STATISTICS,
            FLOWS,
            FLOW_STATISTICS,
            TABLE_SEMANTICS,
            PARTITIONS,
            REGION_PEERS,
            #[cfg(feature = "enterprise")]
            RECYCLE_BIN,
            "future_catalog_table",
        ];

        for (catalog, with_process_manager) in [
            (DEFAULT_CATALOG_NAME, true),
            (DEFAULT_CATALOG_NAME, false),
            ("tenant", true),
            ("tenant", false),
        ] {
            let provider = InformationSchemaProvider::new(
                catalog.to_string(),
                Arc::downgrade(&manager),
                Arc::new(FlowMetadataManager::new(backend.clone())),
                with_process_manager.then(|| Arc::new(ProcessManager::new(String::new(), None))),
                backend.clone(),
            )
            .with_extra_table_factories(factories.clone());
            let expected_count =
                43 + usize::from(with_process_manager) + usize::from(cfg!(feature = "enterprise"))
                    - if catalog == DEFAULT_CATALOG_NAME {
                        0
                    } else {
                        9
                    };
            assert_eq!(provider.tables().len(), expected_count);
            for (name, table) in provider.tables() {
                let scope = if catalog_tables.contains(&name.as_str()) {
                    Catalog
                } else {
                    Global
                };
                assert_eq!(
                    table
                        .table_info()
                        .meta
                        .options
                        .extra_options
                        .get(SEMANTIC_TABLE_SCOPE)
                        .map(String::as_str),
                    Some(scope.as_str()),
                    "{catalog}.{name}"
                );
            }
            assert_eq!(
                provider.table(REGION_PEERS).is_some(),
                catalog == DEFAULT_CATALOG_NAME
            );
            assert_eq!(provider.table(PROCESS_LIST).is_some(), with_process_manager);
            if let Some(table) = provider.table(PROCESS_LIST) {
                assert!(table.schema().column_schema_by_name("catalog").is_some());
            }
        }
    }

    #[tokio::test]
    async fn newly_accessible_information_schema_tables_filter_catalogs() {
        let backend = Arc::new(MemoryKvBackend::default());
        let caches = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(CacheRegistryBuilder::default().build())
            .add_cache_registry(build_fundamental_cache_registry(backend.clone()));
        let manager = KvBackendCatalogManagerBuilder::new(
            Arc::new(NoopInformationExtension),
            backend.clone(),
            Arc::new(
                with_default_composite_cache_registry(caches)
                    .unwrap()
                    .build(),
            ),
        )
        .build();
        let metadata = manager.table_metadata_manager_ref();
        let flow_metadata = FlowMetadataManager::new(backend.clone());
        for (catalog, name, id) in [
            ("tenant_a", "visible_a", 1024),
            ("tenant_b", "visible_b", 1025),
        ] {
            metadata
                .schema_manager()
                .create(SchemaNameKey::new(catalog, "public"), None, false)
                .await
                .unwrap();
            let mut info = test_create_table_task(name, id).table_info;
            info.catalog_name = catalog.to_string();
            info.meta
                .options
                .extra_options
                .insert(SEMANTIC_TABLE_SCOPE.to_string(), "catalog".to_string());
            metadata
                .create_table_metadata(
                    info,
                    TableRouteValue::physical(vec![]),
                    RegionWalOptions::default(),
                )
                .await
                .unwrap();
            let (txn, _) = flow_metadata
                .flow_name_manager()
                .build_create_txn(catalog, name, id)
                .unwrap();
            backend.txn(txn).await.unwrap();
        }

        for (catalog, visible, hidden) in [
            ("tenant_a", "visible_a", "visible_b"),
            ("tenant_b", "visible_b", "visible_a"),
        ] {
            for name in [TABLE_SEMANTICS, STATISTICS, FLOW_STATISTICS] {
                let table = manager
                    .table(catalog, INFORMATION_SCHEMA_NAME, name, None)
                    .await
                    .unwrap()
                    .unwrap();
                let batches = table
                    .scan_to_stream(ScanRequest::default())
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let output = batches
                    .iter()
                    .map(|batch| batch.pretty_print())
                    .collect::<Vec<_>>()
                    .join("\n");
                assert!(output.contains(visible), "{catalog}.{name}: {output}");
                assert!(!output.contains(hidden), "{catalog}.{name}: {output}");
            }
        }
        assert!(
            !manager
                .information_schema_provider()
                .tables()
                .contains_key("visible_a")
        );
    }

    #[test]
    fn test_datanode_inspect_region_info_build_plan() {
        let plan = DatanodeInspectRequest {
            kind: DatanodeInspectKind::RegionInfo,
            scan: ScanRequest::default(),
        }
        .build_plan()
        .unwrap();

        let LogicalPlan::TableScan(scan) = plan else {
            panic!("expected table scan");
        };
        assert_eq!(
            scan.table_name.to_string(),
            RegionInfoEntry::reserved_table_name_for_inspection()
        );
    }
}
