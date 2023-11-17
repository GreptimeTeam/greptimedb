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

impl MetricEngineInner {
    /// Dispatch region put request
    pub async fn put_region(
        &self,
        region_id: RegionId,
        request: RegionPutRequest,
    ) -> Result<Output> {
        let is_putting_physical_region = self
            .state
            .read()
            .await
            .physical_regions()
            .contains_key(&region_id);

        if is_putting_physical_region {
            info!(
                "Metric region received put request {request:?} on physical region {region_id:?}"
            );
            FORBIDDEN_OPERATION_COUNT.inc();

            ForbiddenPhysicalAlterSnafu.fail()
        } else {
            self.put_logical_region(region_id, request).await
        }
    }

    async fn put_logical_region(
        &self,
        logical_region_id: RegionId,
        mut request: RegionPutRequest,
    ) -> Result<Output> {
        let physical_region_id = *self
            .state
            .read()
            .await
            .logical_regions()
            .get(&logical_region_id)
            .with_context(|| LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            })?;
        let data_region_id = to_data_region_id(physical_region_id);

        self.verify_put_request(logical_region_id, physical_region_id, &request)
            .await?;

        // write to data region
        // TODO: retrieve table name
        self.modify_rows(logical_region_id.table_id(), &mut request.rows)?;
        self.data_region.write_data(data_region_id, request).await
    }

    /// Verifies a put request for a logical region against its corresponding metadata region.
    ///
    /// Includes:
    /// - Check if the logical region exists
    /// - Check if the columns exist
    async fn verify_put_request(
        &self,
        logical_region_id: RegionId,
        physical_region_id: RegionId,
        request: &RegionPutRequest,
    ) -> Result<()> {
        // check if the region exists
        let metadata_region_id = to_metadata_region_id(physical_region_id);
        if !self
            .metadata_region
            .is_logical_region_exists(metadata_region_id, logical_region_id)
            .await?
        {
            error!("Trying to write to an nonexistent region {logical_region_id}");
            return LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            }
            .fail();
        }

        // check if the columns exist
        for col in &request.rows.schema {
            if self
                .metadata_region
                .column_semantic_type(metadata_region_id, logical_region_id, &col.column_name)
                .await?
                .is_none()
            {
                return ColumnNotFoundSnafu {
                    name: col.column_name.clone(),
                    region_id: logical_region_id,
                }
                .fail();
            }
        }

        Ok(())
    }

    /// Perform metric engine specific logic to incoming rows.
    /// - Change the semantic type of tag columns to field
    /// - Add table_id column
    /// - Generate tsid
    fn modify_rows(&self, table_id: TableId, rows: &mut Rows) -> Result<()> {
        // gather tag column indices
        let mut tag_col_indices = rows
            .schema
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if col.semantic_type == SemanticType::Tag as i32 {
                    Some((idx, col.column_name.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // generate new schema
        rows.schema = rows
            .schema
            .clone()
            .into_iter()
            .map(|mut col| {
                if col.semantic_type == SemanticType::Tag as i32 {
                    col.semantic_type = SemanticType::Field as i32;
                }
                col
            })
            .collect::<Vec<_>>();
        // add table_name column
        rows.schema.push(PbColumnSchema {
            column_name: DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
            datatype: to_column_data_type(&ConcreteDataType::uint32_datatype())
                .unwrap()
                .into(),
            semantic_type: SemanticType::Tag as _,
        });
        // add tsid column
        rows.schema.push(PbColumnSchema {
            column_name: DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
            datatype: to_column_data_type(&ConcreteDataType::uint64_datatype())
                .unwrap()
                .into(),
            semantic_type: SemanticType::Tag as _,
        });

        // fill internal columns
        let mut random_state = RANDOM_STATE.clone();
        for row in &mut rows.rows {
            Self::fill_internal_columns(&mut random_state, table_id, &tag_col_indices, row);
        }

        Ok(())
    }

    /// Fills internal columns of a row with table name and a hash of tag values.
    fn fill_internal_columns(
        random_state: &mut RandomState,
        table_id: TableId,
        tag_col_indices: &[(usize, String)],
        row: &mut Row,
    ) {
        let mut hasher = random_state.build_hasher();
        for (idx, name) in tag_col_indices {
            let tag = row.values[*idx].clone();
            name.hash(&mut hasher);
            // The type is checked before. So only null is ignored.
            if let Some(ValueData::StringValue(string)) = tag.value_data {
                string.hash(&mut hasher);
            }
        }
        let hash = hasher.finish();

        // fill table id and tsid
        row.values.push(ValueData::U32Value(table_id).into());
        row.values.push(ValueData::U64Value(hash).into());
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use api::v1::region::alter_request;
    use store_api::region_request::AddColumn;

    use super::*;
    use crate::test_util::{self, TestEnv};

    #[tokio::test]
    async fn test_write_logical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        // add columns
        let logical_region_id = env.default_logical_region_id();
        let columns = &["odd", "even", "Ev_En"];
        let alter_request = test_util::alter_logical_region_add_tag_columns(columns);
        engine
            .handle_request(logical_region_id, RegionRequest::Alter(alter_request))
            .await
            .unwrap();

        // prepare data
        let schema = test_util::row_schema_with_tags(columns);
        let rows = test_util::build_rows(3, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
        });

        // write data
        let Output::AffectedRows(count) = engine
            .handle_request(logical_region_id, request)
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(100, count);
    }

    #[tokio::test]
    async fn test_write_physical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let physical_region_id = env.default_physical_region_id();
        let schema = test_util::row_schema_with_tags(&["abc"]);
        let rows = test_util::build_rows(1, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
        });

        engine
            .handle_request(physical_region_id, request)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_write_nonexist_logical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let logical_region_id = RegionId::new(175, 8345);
        let schema = test_util::row_schema_with_tags(&["def"]);
        let rows = test_util::build_rows(1, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
        });

        engine
            .handle_request(logical_region_id, request)
            .await
            .unwrap_err();
    }
}
