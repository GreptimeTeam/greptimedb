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

mod alter;
mod create;
mod put;
mod state;

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;

use ahash::{AHasher, RandomState};
use api::helper::to_column_data_type;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema as PbColumnSchema, Row, Rows, SemanticType};
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{error, info};
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use mito2::engine::{MitoEngine, MITO_ENGINE_NAME};
use object_store::util::join_dir;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    AlterKind, RegionAlterRequest, RegionCreateRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{RegionGroup, RegionId, ScanRequest, TableId};
use tokio::sync::RwLock;

use self::state::MetricEngineState;
use crate::consts::{
    DATA_REGION_SUBDIR, DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    LOGICAL_TABLE_METADATA_KEY, METADATA_REGION_SUBDIR, METADATA_SCHEMA_KEY_COLUMN_INDEX,
    METADATA_SCHEMA_KEY_COLUMN_NAME, METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX,
    METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME, METADATA_SCHEMA_VALUE_COLUMN_INDEX,
    METADATA_SCHEMA_VALUE_COLUMN_NAME, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
    RANDOM_STATE,
};
use crate::data_region::DataRegion;
use crate::error::{
    ColumnNotFoundSnafu, ConflictRegionOptionSnafu, CreateMitoRegionSnafu,
    ForbiddenPhysicalAlterSnafu, InternalColumnOccupiedSnafu, LogicalRegionNotFoundSnafu,
    MissingRegionOptionSnafu, ParseRegionIdSnafu, PhysicalRegionNotFoundSnafu, Result,
};
use crate::metadata_region::MetadataRegion;
use crate::metrics::{
    FORBIDDEN_OPERATION_COUNT, LOGICAL_REGION_COUNT, PHYSICAL_COLUMN_COUNT, PHYSICAL_REGION_COUNT,
};
use crate::utils::{to_data_region_id, to_metadata_region_id};

#[cfg_attr(doc, aquamarine::aquamarine)]
/// # Metric Engine
///
/// ## Regions
///
/// Regions in this metric engine has several roles. There is `PhysicalRegion`,
/// which refer to the region that actually stores the data. And `LogicalRegion`
/// that is "simulated" over physical regions. Each logical region is associated
/// with one physical region group, which is a group of two physical regions.
/// Their relationship is illustrated below:
///
/// ```mermaid
/// erDiagram
///     LogicalRegion ||--o{ PhysicalRegionGroup : corresponds
///     PhysicalRegionGroup ||--|| DataRegion : contains
///     PhysicalRegionGroup ||--|| MetadataRegion : contains
/// ```
///
/// Metric engine uses two region groups. One is for data region
/// ([METRIC_DATA_REGION_GROUP](crate::consts::METRIC_DATA_REGION_GROUP)), and the
/// other is for metadata region ([METRIC_METADATA_REGION_GROUP](crate::consts::METRIC_METADATA_REGION_GROUP)).
/// From the definition of [`RegionId`], we can convert between these two physical
/// region ids easily. Thus in the code base we usually refer to one "physical
/// region id", and convert it to the other one when necessary.
///
/// The logical region, in contrast, is a virtual region. It doesn't has dedicated
/// storage or region group. Only a region id that is allocated by meta server.
/// And all other things is shared with other logical region that are associated
/// with the same physical region group.
///
/// For more doucment about physical regions, please refer to [`MetadataRegion`]
/// and [`DataRegion`].
///
/// ## Operations
///
/// Both physical and logical region are accessible to user. But the operation
/// they support are different. List below:
///
/// | Operations | Logical Region | Physical Region |
/// | :--------: | :------------: | :-------------: |
/// |   Create   |       ✅        |        ✅        |
/// |    Drop    |       ✅        |        ❌        |
/// |   Write    |       ✅        |        ❌        |
/// |    Read    |       ✅        |        ✅        |
/// |   Close    |       ✅        |        ✅        |
/// |    Open    |       ✅        |        ✅        |
/// |   Alter    |       ✅        |        ❌        |
///
/// ## Internal Columns
///
/// The physical data region contains two internal columns. Should
/// mention that "internal" here is for metric engine itself. Mito
/// engine will add it's internal columns to the region as well.
///
/// Their column id is registered in [`ReservedColumnId`]. And column name is
/// defined in [`DATA_SCHEMA_TSID_COLUMN_NAME`] and [`DATA_SCHEMA_TABLE_ID_COLUMN_NAME`].
///
/// Tsid is generated by hashing all tags. And table id is retrieved from logical region
/// id to distinguish data from different logical tables.
#[derive(Clone)]
pub struct MetricEngine {
    inner: Arc<MetricEngineInner>,
}

#[async_trait]
impl RegionEngine for MetricEngine {
    /// Name of this engine
    fn name(&self) -> &str {
        METRIC_ENGINE_NAME
    }

    /// Handles request to the region.
    ///
    /// Only query is not included, which is handled in `handle_query`
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> std::result::Result<Output, BoxedError> {
        let result = match request {
            RegionRequest::Put(put) => self.inner.put_region(region_id, put).await,
            RegionRequest::Delete(_) => todo!(),
            RegionRequest::Create(create) => self
                .inner
                .create_region(region_id, create)
                .await
                .map(|_| Output::AffectedRows(0)),
            RegionRequest::Drop(_) => todo!(),
            RegionRequest::Open(_) => todo!(),
            RegionRequest::Close(_) => todo!(),
            RegionRequest::Alter(alter) => self
                .inner
                .alter_region(region_id, alter)
                .await
                .map(|_| Output::AffectedRows(0)),
            RegionRequest::Flush(_) => todo!(),
            RegionRequest::Compact(_) => todo!(),
            RegionRequest::Truncate(_) => todo!(),
        };

        result.map_err(BoxedError::new)
    }

    /// Handles substrait query and return a stream of record batches
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        todo!()
    }

    /// Retrieves region's metadata.
    async fn get_metadata(
        &self,
        region_id: RegionId,
    ) -> std::result::Result<RegionMetadataRef, BoxedError> {
        todo!()
    }

    /// Retrieves region's disk usage.
    async fn region_disk_usage(&self, region_id: RegionId) -> Option<i64> {
        todo!()
    }

    /// Stops the engine
    async fn stop(&self) -> std::result::Result<(), BoxedError> {
        todo!()
    }

    fn set_writable(
        &self,
        region_id: RegionId,
        writable: bool,
    ) -> std::result::Result<(), BoxedError> {
        todo!()
    }

    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        todo!()
    }
}

impl MetricEngine {
    pub fn new(mito: MitoEngine) -> Self {
        let metadata_region = MetadataRegion::new(mito.clone());
        let data_region = DataRegion::new(mito.clone());
        Self {
            inner: Arc::new(MetricEngineInner {
                mito,
                metadata_region,
                data_region,
                state: RwLock::default(),
            }),
        }
    }
}

struct MetricEngineInner {
    mito: MitoEngine,
    metadata_region: MetadataRegion,
    data_region: DataRegion,
    state: RwLock<MetricEngineState>,
}
