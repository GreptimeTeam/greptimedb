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

use std::collections::HashMap;
use std::sync::Arc;

use store_api::storage::{RegionId, RegionNumber, TableId};

use crate::DatanodeId;
use crate::cache_invalidator::CacheInvalidatorRef;
use crate::ddl::flow_meta::FlowMetadataAllocatorRef;
use crate::ddl::table_meta::TableMetadataAllocatorRef;
use crate::key::TableMetadataManagerRef;
use crate::key::flow::FlowMetadataManagerRef;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::node_manager::NodeManagerRef;
use crate::region_keeper::MemoryRegionKeeperRef;
use crate::region_registry::LeaderRegionRegistryRef;

pub mod allocator;
pub mod alter_database;
pub mod alter_logical_tables;
pub mod alter_table;
pub mod comment_on;
pub mod create_database;
pub mod create_flow;
pub mod create_logical_tables;
pub mod create_table;
pub(crate) use create_table::{CreateRequestBuilder, build_template_from_raw_table_info};
pub mod create_view;
pub mod drop_database;
pub mod drop_flow;
pub mod drop_table;
pub mod drop_view;
pub mod flow_meta;
pub mod table_meta;
#[cfg(any(test, feature = "testing"))]
pub mod test_util;
#[cfg(test)]
pub(crate) mod tests;
pub mod truncate_table;
pub mod utils;

/// Metadata allocated to a table.
#[derive(Default)]
pub struct TableMetadata {
    /// Table id.
    pub table_id: TableId,
    /// Route information for each region of the table.
    pub table_route: PhysicalTableRouteValue,
    /// The encoded wal options for regions of the table.
    // If a region does not have an associated wal options, no key for the region would be found in the map.
    pub region_wal_options: HashMap<RegionNumber, String>,
}

pub type RegionFailureDetectorControllerRef = Arc<dyn RegionFailureDetectorController>;

pub type DetectingRegion = (DatanodeId, RegionId);

/// Used for actively registering Region failure detectors.
///
/// Ensuring the Region Supervisor can detect Region failures without relying on the first heartbeat from the datanode.
#[async_trait::async_trait]
pub trait RegionFailureDetectorController: Send + Sync {
    /// Registers failure detectors for the given identifiers.
    async fn register_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>);

    /// Deregisters failure detectors for the given identifiers.
    async fn deregister_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>);
}

/// A noop implementation of [`RegionFailureDetectorController`].
#[derive(Debug, Clone)]
pub struct NoopRegionFailureDetectorControl;

#[async_trait::async_trait]
impl RegionFailureDetectorController for NoopRegionFailureDetectorControl {
    async fn register_failure_detectors(&self, _detecting_regions: Vec<DetectingRegion>) {}

    async fn deregister_failure_detectors(&self, _detecting_regions: Vec<DetectingRegion>) {}
}

/// The context of ddl.
#[derive(Clone)]
pub struct DdlContext {
    /// Sends querying and requests to nodes.
    pub node_manager: NodeManagerRef,
    /// Cache invalidation.
    pub cache_invalidator: CacheInvalidatorRef,
    /// Keep tracking operating regions.
    pub memory_region_keeper: MemoryRegionKeeperRef,
    /// The leader region registry.
    pub leader_region_registry: LeaderRegionRegistryRef,
    /// Table metadata manager.
    pub table_metadata_manager: TableMetadataManagerRef,
    /// Allocator for table metadata.
    pub table_metadata_allocator: TableMetadataAllocatorRef,
    /// Flow metadata manager.
    pub flow_metadata_manager: FlowMetadataManagerRef,
    /// Allocator for flow metadata.
    pub flow_metadata_allocator: FlowMetadataAllocatorRef,
    /// controller of region failure detector.
    pub region_failure_detector_controller: RegionFailureDetectorControllerRef,
}

impl DdlContext {
    /// Notifies the RegionSupervisor to register failure detector of new created regions.
    ///
    /// The datanode may crash without sending a heartbeat that contains information about newly created regions,
    /// which may prevent the RegionSupervisor from detecting failures in these newly created regions.
    pub async fn register_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        self.region_failure_detector_controller
            .register_failure_detectors(detecting_regions)
            .await;
    }

    /// Notifies the RegionSupervisor to remove failure detectors.
    ///
    /// Once the regions were dropped, subsequent heartbeats no longer include these regions.
    /// Therefore, we should remove the failure detectors for these dropped regions.
    async fn deregister_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        self.region_failure_detector_controller
            .deregister_failure_detectors(detecting_regions)
            .await;
    }
}
