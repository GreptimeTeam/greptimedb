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
pub mod flows;
mod information_memory_table;
pub mod key_column_usage;
mod partitions;
mod procedure_info;
pub mod process_list;
pub mod region_peers;
mod region_statistics;
pub mod schemata;
mod ssts;
mod table_constraints;
mod table_names;
pub mod tables;
mod views;

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
use datatypes::schema::SchemaRef;
use lazy_static::lazy_static;
use paste::paste;
use process_list::InformationSchemaProcessList;
use store_api::sst_entry::{ManifestSstEntry, PuffinIndexMetaEntry, StorageSstEntry};
use store_api::storage::{ScanRequest, TableId};
use table::TableRef;
use table::metadata::TableType;
pub use table_names::*;
use views::InformationSchemaViews;

use self::columns::InformationSchemaColumns;
use crate::CatalogManager;
use crate::error::{Error, Result};
use crate::process_manager::ProcessManagerRef;
use crate::system_schema::information_schema::cluster_info::InformationSchemaClusterInfo;
use crate::system_schema::information_schema::flows::InformationSchemaFlows;
use crate::system_schema::information_schema::information_memory_table::get_schema_columns;
use crate::system_schema::information_schema::key_column_usage::InformationSchemaKeyColumnUsage;
use crate::system_schema::information_schema::partitions::InformationSchemaPartitions;
use crate::system_schema::information_schema::region_peers::InformationSchemaRegionPeers;
use crate::system_schema::information_schema::schemata::InformationSchemaSchemata;
use crate::system_schema::information_schema::ssts::{
    InformationSchemaSstsIndexMeta, InformationSchemaSstsManifest, InformationSchemaSstsStorage,
};
use crate::system_schema::information_schema::table_constraints::InformationSchemaTableConstraints;
use crate::system_schema::information_schema::tables::InformationSchemaTables;
use crate::system_schema::memory_table::MemoryTable;
pub(crate) use crate::system_schema::predicate::Predicates;
use crate::system_schema::{
    SystemSchemaProvider, SystemSchemaProviderInner, SystemTable, SystemTableRef,
};

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
        PARTITIONS,
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

/// A factory trait for making information schema tables.
///
/// This trait allows for extensibility of the information schema by providing
/// a way to dynamically create custom information schema tables.
pub trait InformationSchemaTableFactory {
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
            PROCEDURE_INFO => Some(
                Arc::new(procedure_info::InformationSchemaProcedureInfo::new(
                    self.catalog_manager.clone(),
                )) as _,
            ),
            REGION_STATISTICS => Some(Arc::new(
                region_statistics::InformationSchemaRegionStatistics::new(
                    self.catalog_manager.clone(),
                ),
            ) as _),
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
        let mut tables = HashMap::new();

        // SECURITY NOTE:
        // Carefully consider the tables that may expose sensitive cluster configurations,
        // authentication details, and other critical information.
        // Only put these tables under `greptime` catalog to prevent info leak.
        if self.catalog_name == DEFAULT_CATALOG_NAME {
            tables.insert(
                BUILD_INFO.to_string(),
                self.build_table(BUILD_INFO).unwrap(),
            );
            tables.insert(
                REGION_PEERS.to_string(),
                self.build_table(REGION_PEERS).unwrap(),
            );
            tables.insert(
                CLUSTER_INFO.to_string(),
                self.build_table(CLUSTER_INFO).unwrap(),
            );
            tables.insert(
                PROCEDURE_INFO.to_string(),
                self.build_table(PROCEDURE_INFO).unwrap(),
            );
            tables.insert(
                REGION_STATISTICS.to_string(),
                self.build_table(REGION_STATISTICS).unwrap(),
            );
            tables.insert(
                SSTS_MANIFEST.to_string(),
                self.build_table(SSTS_MANIFEST).unwrap(),
            );
            tables.insert(
                SSTS_STORAGE.to_string(),
                self.build_table(SSTS_STORAGE).unwrap(),
            );
            tables.insert(
                SSTS_INDEX_META.to_string(),
                self.build_table(SSTS_INDEX_META).unwrap(),
            );
        }

        tables.insert(TABLES.to_string(), self.build_table(TABLES).unwrap());
        tables.insert(VIEWS.to_string(), self.build_table(VIEWS).unwrap());
        tables.insert(SCHEMATA.to_string(), self.build_table(SCHEMATA).unwrap());
        tables.insert(COLUMNS.to_string(), self.build_table(COLUMNS).unwrap());
        tables.insert(
            KEY_COLUMN_USAGE.to_string(),
            self.build_table(KEY_COLUMN_USAGE).unwrap(),
        );
        tables.insert(
            TABLE_CONSTRAINTS.to_string(),
            self.build_table(TABLE_CONSTRAINTS).unwrap(),
        );
        tables.insert(FLOWS.to_string(), self.build_table(FLOWS).unwrap());
        if let Some(process_list) = self.build_table(PROCESS_LIST) {
            tables.insert(PROCESS_LIST.to_string(), process_list);
        }
        for name in self.extra_table_factories.keys() {
            tables.insert(name.clone(), self.build_table(name).expect(name));
        }
        // Add memory tables
        for name in MEMORY_TABLES.iter() {
            tables.insert((*name).to_string(), self.build_table(name).expect(name));
        }
        self.tables = tables;
    }
}

pub trait InformationTable {
    fn table_id(&self) -> TableId;

    fn table_name(&self) -> &'static str;

    fn schema(&self) -> SchemaRef;

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream>;

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
}

impl DatanodeInspectRequest {
    /// Builds a logical plan for the datanode inspect request.
    pub fn build_plan(self) -> std::result::Result<LogicalPlan, DataFusionError> {
        match self.kind {
            DatanodeInspectKind::SstManifest => ManifestSstEntry::build_plan(self.scan),
            DatanodeInspectKind::SstStorage => StorageSstEntry::build_plan(self.scan),
            DatanodeInspectKind::SstIndexMeta => PuffinIndexMetaEntry::build_plan(self.scan),
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
