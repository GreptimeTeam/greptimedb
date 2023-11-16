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
use store_api::storage::{RegionGroup, RegionId, ScanRequest};
use tokio::sync::RwLock;

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
use crate::utils::{self, to_data_region_id};

/// region group value for data region inside a metric region
pub const METRIC_DATA_REGION_GROUP: RegionGroup = 0;

/// region group value for metadata region inside a metric region
pub const METRIC_METADATA_REGION_GROUP: RegionGroup = 1;

pub const METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME: &str = "ts";
pub const METADATA_SCHEMA_KEY_COLUMN_NAME: &str = "k";
pub const METADATA_SCHEMA_VALUE_COLUMN_NAME: &str = "v";

pub const METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX: usize = 0;
pub const METADATA_SCHEMA_KEY_COLUMN_INDEX: usize = 1;
pub const METADATA_SCHEMA_VALUE_COLUMN_INDEX: usize = 2;

/// Column name of internal column `__metric` that stores the original metric name
pub const DATA_SCHEMA_METRIC_NAME_COLUMN_NAME: &str = "__metric";
pub const DATA_SCHEMA_TSID_COLUMN_NAME: &str = "__tsid";

pub const METADATA_REGION_SUBDIR: &str = "metadata";
pub const DATA_REGION_SUBDIR: &str = "data";

pub const METRIC_ENGINE_NAME: &str = "metric";

/// Metadata key present in the `CREATE TABLE ... WITH ()` clause. This key is
/// used to identify the table is a physical metric table. E.g.:
/// ```sql
/// CREATE TABLE physical_table (
///     ...
/// )
/// ENGINE = metric
/// WITH (
///     physical_metric_table,
/// );
/// ```
pub const PHYSICAL_TABLE_METADATA_KEY: &str = "physical_metric_table";

/// Metadata key present in the `CREATE TABLE ... WITH ()` clause. This key is
/// used to identify a logical table and associate it with a corresponding physical
/// table . E.g.:
/// ```sql
/// CREATE TABLE logical_table (
///     ...
/// )
/// ENGINE = metric
/// WITH (
///     on_physical_table = "physical_table",
/// );
/// ```
/// And this key will be translated to corresponding physical **REGION** id in metasrv.
pub const LOGICAL_TABLE_METADATA_KEY: &str = "on_physical_table";

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

/// Internal states of metric engine
#[derive(Default)]
struct MetricEngineState {
    /// Mapping from physical region id to its logical region ids
    /// `logical_regions` records a reverse mapping from logical region id to
    /// physical region id
    physical_regions: HashMap<RegionId, HashSet<RegionId>>,
    /// Mapping from logical region id to physical region id.
    logical_regions: HashMap<RegionId, RegionId>,
    /// Cache for the columns of physical regions.
    /// The region id in key is the data region id.
    physical_columns: HashMap<RegionId, HashSet<String>>,
}

impl MetricEngineState {
    pub fn add_physical_region(
        &mut self,
        physical_region_id: RegionId,
        physical_columns: HashSet<String>,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        self.physical_regions
            .insert(physical_region_id, HashSet::new());
        self.physical_columns
            .insert(physical_region_id, physical_columns);
    }

    /// # Panic
    /// if the physical region does not exist
    pub fn add_physical_columns(
        &mut self,
        physical_region_id: RegionId,
        physical_columns: impl IntoIterator<Item = String>,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        let columns = self.physical_columns.get_mut(&physical_region_id).unwrap();
        for col in physical_columns {
            columns.insert(col);
        }
    }

    /// # Panic
    /// if the physical region does not exist
    pub fn add_logical_region(
        &mut self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        self.physical_regions
            .get_mut(&physical_region_id)
            .unwrap()
            .insert(logical_region_id);
        self.logical_regions
            .insert(logical_region_id, physical_region_id);
    }
}

struct MetricEngineInner {
    mito: MitoEngine,
    metadata_region: MetadataRegion,
    data_region: DataRegion,
    state: RwLock<MetricEngineState>,
}

impl MetricEngineInner {
    /// Dispatch region creation request to physical region creation or logical
    pub async fn create_region(
        &self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<()> {
        Self::verify_region_create_request(&request)?;

        if request.options.contains_key(PHYSICAL_TABLE_METADATA_KEY) {
            self.create_physical_region(region_id, request).await
        } else if request.options.contains_key(LOGICAL_TABLE_METADATA_KEY) {
            self.create_logical_region(region_id, request).await
        } else {
            MissingRegionOptionSnafu {}.fail()
        }
    }

    /// Initialize a physical metric region at given region id.
    async fn create_physical_region(
        &self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<()> {
        let (data_region_id, metadata_region_id) = Self::transform_region_id(region_id);

        // create metadata region
        let create_metadata_region_request =
            self.create_request_for_metadata_region(&request.region_dir);
        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Create(create_metadata_region_request),
            )
            .await
            .with_context(|_| CreateMitoRegionSnafu {
                region_type: METADATA_REGION_SUBDIR,
            })?;

        // create data region
        let create_data_region_request = self.create_request_for_data_region(&request);
        let physical_column_set = create_data_region_request
            .column_metadatas
            .iter()
            .map(|metadata| metadata.column_schema.name.clone())
            .collect::<HashSet<_>>();
        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Create(create_data_region_request),
            )
            .await
            .with_context(|_| CreateMitoRegionSnafu {
                region_type: DATA_REGION_SUBDIR,
            })?;

        info!("Created physical metric region {region_id:?}");
        PHYSICAL_REGION_COUNT.inc();

        // remember this table
        self.state
            .write()
            .await
            .add_physical_region(data_region_id, physical_column_set);

        Ok(())
    }

    /// Create a logical region.
    ///
    /// Physical table and logical table can have multiple regions, and their
    /// region number should be the same. Thus we can infer the physical region
    /// id by simply replace the table id part in the given region id, which
    /// represent the "logical region" to request.
    ///
    /// This method will alter the data region to add columns if necessary.
    ///
    /// If the logical region to create already exists, this method will do nothing.
    async fn create_logical_region(
        &self,
        logical_region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<()> {
        // transform IDs
        let physical_region_id_raw = request
            .options
            .get(LOGICAL_TABLE_METADATA_KEY)
            .ok_or(MissingRegionOptionSnafu {}.build())?;
        let physical_region_id: RegionId = physical_region_id_raw
            .parse::<u64>()
            .with_context(|_| ParseRegionIdSnafu {
                raw: physical_region_id_raw,
            })?
            .into();
        let (data_region_id, metadata_region_id) = Self::transform_region_id(physical_region_id);

        // check if the logical region already exist
        if self
            .metadata_region
            .is_logical_region_exists(metadata_region_id, logical_region_id)
            .await?
        {
            info!("Create a existing logical region {logical_region_id}. Skipped");
            return Ok(());
        }

        // find new columns to add
        let mut new_columns = vec![];
        {
            let physical_columns = &self.state.read().await.physical_columns;
            let physical_columns = physical_columns.get(&data_region_id).with_context(|| {
                PhysicalRegionNotFoundSnafu {
                    region_id: data_region_id,
                }
            })?;
            for col in &request.column_metadatas {
                if !physical_columns.contains(&col.column_schema.name) {
                    new_columns.push(col.clone());
                }
            }
        }
        info!("Found new columns {new_columns:?} to add to physical region {data_region_id}");

        self.add_columns_to_physical_data_region(
            data_region_id,
            metadata_region_id,
            logical_region_id,
            new_columns,
        )
        .await?;

        // register logical region to metadata region
        self.metadata_region
            .add_logical_region(metadata_region_id, logical_region_id)
            .await?;
        for col in &request.column_metadatas {
            self.metadata_region
                .add_column(
                    metadata_region_id,
                    logical_region_id,
                    &col.column_schema.name,
                    col.semantic_type,
                )
                .await?;
        }

        // update the mapping
        // Safety: previous steps ensure the physical region exist
        self.state
            .write()
            .await
            .add_logical_region(physical_region_id, logical_region_id);
        info!("Created new logical region {logical_region_id} on physical region {data_region_id}");
        LOGICAL_REGION_COUNT.inc();

        Ok(())
    }

    async fn add_columns_to_physical_data_region(
        &self,
        data_region_id: RegionId,
        metadata_region_id: RegionId,
        logical_region_id: RegionId,
        new_columns: Vec<ColumnMetadata>,
    ) -> Result<()> {
        // alter data region
        self.data_region
            .add_columns(data_region_id, new_columns.clone())
            .await?;

        // register columns to metadata region
        for col in &new_columns {
            self.metadata_region
                .add_column(
                    metadata_region_id,
                    logical_region_id,
                    &col.column_schema.name,
                    col.semantic_type,
                )
                .await?;
        }

        // safety: previous step has checked this
        self.state.write().await.add_physical_columns(
            data_region_id,
            new_columns
                .iter()
                .map(|meta| meta.column_schema.name.clone()),
        );
        info!("Create region {logical_region_id} leads to adding columns {new_columns:?} to physical region {data_region_id}");
        PHYSICAL_COLUMN_COUNT.add(new_columns.len() as _);

        Ok(())
    }

    /// Check if
    /// - internal columns are not occupied
    /// - required table option is present ([PHYSICAL_TABLE_METADATA_KEY] or
    ///   [LOGICAL_TABLE_METADATA_KEY])
    fn verify_region_create_request(request: &RegionCreateRequest) -> Result<()> {
        let name_to_index = request
            .column_metadatas
            .iter()
            .enumerate()
            .map(|(idx, metadata)| (metadata.column_schema.name.clone(), idx))
            .collect::<HashMap<String, usize>>();

        // check if internal columns are not occupied
        ensure!(
            !name_to_index.contains_key(DATA_SCHEMA_METRIC_NAME_COLUMN_NAME),
            InternalColumnOccupiedSnafu {
                column: DATA_SCHEMA_METRIC_NAME_COLUMN_NAME,
            }
        );
        ensure!(
            !name_to_index.contains_key(DATA_SCHEMA_TSID_COLUMN_NAME),
            InternalColumnOccupiedSnafu {
                column: DATA_SCHEMA_TSID_COLUMN_NAME,
            }
        );

        // check if required table option is present
        ensure!(
            request.options.contains_key(PHYSICAL_TABLE_METADATA_KEY)
                || request.options.contains_key(LOGICAL_TABLE_METADATA_KEY),
            MissingRegionOptionSnafu {}
        );
        ensure!(
            !(request.options.contains_key(PHYSICAL_TABLE_METADATA_KEY)
                && request.options.contains_key(LOGICAL_TABLE_METADATA_KEY)),
            ConflictRegionOptionSnafu {}
        );

        Ok(())
    }

    /// Build data region id and metadata region id from the given region id.
    ///
    /// Return value: (data_region_id, metadata_region_id)
    fn transform_region_id(region_id: RegionId) -> (RegionId, RegionId) {
        (
            utils::to_data_region_id(region_id),
            utils::to_metadata_region_id(region_id),
        )
    }

    /// Build [RegionCreateRequest] for metadata region
    ///
    /// This method will append [METADATA_REGION_SUBDIR] to the given `region_dir`.
    pub fn create_request_for_metadata_region(&self, region_dir: &str) -> RegionCreateRequest {
        // ts TIME INDEX DEFAULT 0
        let timestamp_column_metadata = ColumnMetadata {
            column_id: METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX as _,
            semantic_type: SemanticType::Timestamp,
            column_schema: ColumnSchema::new(
                METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_default_constraint(Some(datatypes::schema::ColumnDefaultConstraint::Value(
                Value::Timestamp(Timestamp::new_millisecond(0)),
            )))
            .unwrap(),
        };
        // key STRING PRIMARY KEY
        let key_column_metadata = ColumnMetadata {
            column_id: METADATA_SCHEMA_KEY_COLUMN_INDEX as _,
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                METADATA_SCHEMA_KEY_COLUMN_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
        };
        // val STRING
        let value_column_metadata = ColumnMetadata {
            column_id: METADATA_SCHEMA_VALUE_COLUMN_INDEX as _,
            semantic_type: SemanticType::Field,
            column_schema: ColumnSchema::new(
                METADATA_SCHEMA_VALUE_COLUMN_NAME,
                ConcreteDataType::string_datatype(),
                true,
            ),
        };

        // concat region dir
        let metadata_region_dir = join_dir(region_dir, METADATA_REGION_SUBDIR);

        RegionCreateRequest {
            engine: MITO_ENGINE_NAME.to_string(),
            column_metadatas: vec![
                timestamp_column_metadata,
                key_column_metadata,
                value_column_metadata,
            ],
            primary_key: vec![METADATA_SCHEMA_KEY_COLUMN_INDEX as _],
            options: HashMap::new(),
            region_dir: metadata_region_dir,
        }
    }

    /// Convert [RegionCreateRequest] for data region.
    ///
    /// All tag columns in the original request will be converted to value columns.
    /// Those columns real semantic type is stored in metadata region.
    ///
    /// This will also add internal columns to the request.
    pub fn create_request_for_data_region(
        &self,
        request: &RegionCreateRequest,
    ) -> RegionCreateRequest {
        let mut data_region_request = request.clone();

        // concat region dir
        data_region_request.region_dir = join_dir(&request.region_dir, DATA_REGION_SUBDIR);

        // convert semantic type
        data_region_request
            .column_metadatas
            .iter_mut()
            .for_each(|metadata| {
                if metadata.semantic_type == SemanticType::Tag {
                    metadata.semantic_type = SemanticType::Field;
                }
            });

        // add internal columns
        let [metric_name_col, tsid_col] = Self::internal_column_metadata();
        data_region_request.column_metadatas.push(metric_name_col);
        data_region_request.column_metadatas.push(tsid_col);
        data_region_request.primary_key =
            vec![ReservedColumnId::metric_name(), ReservedColumnId::tsid()];

        data_region_request
    }

    /// Generate internal column metadata.
    ///
    /// Return `[metric_name_col, tsid_col]`
    fn internal_column_metadata() -> [ColumnMetadata; 2] {
        let metric_name_col = ColumnMetadata {
            column_id: ReservedColumnId::metric_name(),
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_METRIC_NAME_COLUMN_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
        };
        let tsid_col = ColumnMetadata {
            column_id: ReservedColumnId::tsid(),
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ),
        };
        [metric_name_col, tsid_col]
    }
}

impl MetricEngineInner {
    /// Dispatch region alter request
    pub async fn alter_region(
        &self,
        region_id: RegionId,
        request: RegionAlterRequest,
    ) -> Result<()> {
        let is_altering_logical_region = self
            .state
            .read()
            .await
            .physical_regions
            .contains_key(&region_id);
        if is_altering_logical_region {
            self.alter_physical_region(region_id, request).await
        } else {
            self.alter_logical_region(region_id, request).await
        }
    }

    async fn alter_logical_region(
        &self,
        region_id: RegionId,
        request: RegionAlterRequest,
    ) -> Result<()> {
        let physical_region_id = {
            let logical_regions = &self.state.read().await.logical_regions;
            *logical_regions.get(&region_id).with_context(|| {
                error!("Trying to alter an nonexistent region {region_id}");
                LogicalRegionNotFoundSnafu { region_id }
            })?
        };

        // only handle adding column
        let AlterKind::AddColumns { columns } = request.kind else {
            return Ok(());
        };

        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
        let mut columns_to_add = vec![];
        for col in &columns {
            if self
                .metadata_region
                .column_semantic_type(
                    metadata_region_id,
                    region_id,
                    &col.column_metadata.column_schema.name,
                )
                .await?
                .is_none()
            {
                columns_to_add.push(col.column_metadata.clone());
            }
        }

        // alter data region
        let data_region_id = utils::to_data_region_id(physical_region_id);
        self.add_columns_to_physical_data_region(
            data_region_id,
            metadata_region_id,
            region_id,
            columns_to_add,
        )
        .await?;

        // register columns to logical region
        for col in columns {
            self.metadata_region
                .add_column(
                    metadata_region_id,
                    region_id,
                    &col.column_metadata.column_schema.name,
                    col.column_metadata.semantic_type,
                )
                .await?;
        }

        Ok(())
    }

    async fn alter_physical_region(
        &self,
        region_id: RegionId,
        request: RegionAlterRequest,
    ) -> Result<()> {
        info!("Metric region received alter request {request:?} on physical region {region_id:?}");
        FORBIDDEN_OPERATION_COUNT.inc();

        ForbiddenPhysicalAlterSnafu.fail()
    }
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
            .physical_regions
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
            .logical_regions
            .get(&logical_region_id)
            .with_context(|| LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            })?;
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);

        self.verify_put_request(logical_region_id, physical_region_id, &request)
            .await?;

        // write to data region
        // TODO: retrieve table name
        self.modify_rows("test".to_string(), &mut request.rows)?;
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
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
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

    /// Perform metric engine specific logic to incomming rows.
    /// - Change the semantic type of tag columns to field
    /// - Add table_name column
    /// - Generate tsid
    fn modify_rows(&self, table_name: String, rows: &mut Rows) -> Result<()> {
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
            column_name: DATA_SCHEMA_METRIC_NAME_COLUMN_NAME.to_string(),
            datatype: to_column_data_type(&ConcreteDataType::string_datatype())
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
        let mut random_state = ahash::RandomState::with_seeds(1, 2, 3, 4);
        for row in &mut rows.rows {
            Self::fill_internal_columns(&mut random_state, &table_name, &tag_col_indices, row);
        }

        Ok(())
    }

    /// Fills internal columns of a row with table name and a hash of tag values.
    fn fill_internal_columns(
        random_state: &mut RandomState,
        table_name: &str,
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

        // fill table name and tsid
        row.values
            .push(ValueData::StringValue(table_name.to_string()).into());
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

    #[test]
    fn test_verify_region_create_request() {
        // internal column is occupied
        let request = RegionCreateRequest {
            column_metadatas: vec![
                ColumnMetadata {
                    column_id: 0,
                    semantic_type: SemanticType::Timestamp,
                    column_schema: ColumnSchema::new(
                        METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                },
                ColumnMetadata {
                    column_id: 1,
                    semantic_type: SemanticType::Tag,
                    column_schema: ColumnSchema::new(
                        DATA_SCHEMA_METRIC_NAME_COLUMN_NAME,
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                },
            ],
            region_dir: "test_dir".to_string(),
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: HashMap::new(),
        };
        let result = MetricEngineInner::verify_region_create_request(&request);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Internal column __metric is reserved".to_string()
        );

        // valid request
        let request = RegionCreateRequest {
            column_metadatas: vec![
                ColumnMetadata {
                    column_id: 0,
                    semantic_type: SemanticType::Timestamp,
                    column_schema: ColumnSchema::new(
                        METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                },
                ColumnMetadata {
                    column_id: 1,
                    semantic_type: SemanticType::Tag,
                    column_schema: ColumnSchema::new(
                        "column1".to_string(),
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                },
            ],
            region_dir: "test_dir".to_string(),
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
                .into_iter()
                .collect(),
        };
        let result = MetricEngineInner::verify_region_create_request(&request);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_region_create_request_options() {
        let mut request = RegionCreateRequest {
            column_metadatas: vec![],
            region_dir: "test_dir".to_string(),
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: HashMap::new(),
        };
        let result = MetricEngineInner::verify_region_create_request(&request);
        assert!(result.is_err());

        let mut options = HashMap::new();
        options.insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), "value".to_string());
        request.options = options.clone();
        let result = MetricEngineInner::verify_region_create_request(&request);
        assert!(result.is_ok());

        options.insert(LOGICAL_TABLE_METADATA_KEY.to_string(), "value".to_string());
        request.options = options.clone();
        let result = MetricEngineInner::verify_region_create_request(&request);
        assert!(result.is_err());

        options.remove(PHYSICAL_TABLE_METADATA_KEY).unwrap();
        request.options = options;
        let result = MetricEngineInner::verify_region_create_request(&request);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_request_for_data_region() {
        let request = RegionCreateRequest {
            engine: METRIC_ENGINE_NAME.to_string(),
            column_metadatas: vec![
                ColumnMetadata {
                    column_id: 0,
                    semantic_type: SemanticType::Timestamp,
                    column_schema: ColumnSchema::new(
                        "timestamp",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                },
                ColumnMetadata {
                    column_id: 1,
                    semantic_type: SemanticType::Tag,
                    column_schema: ColumnSchema::new(
                        "tag",
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                },
            ],
            primary_key: vec![0],
            options: HashMap::new(),
            region_dir: "test_dir".to_string(),
        };

        let env = TestEnv::new().await;
        let engine = MetricEngine::new(env.mito());
        let engine_inner = engine.inner;
        let data_region_request = engine_inner.create_request_for_data_region(&request);

        assert_eq!(
            data_region_request.region_dir,
            "/test_dir/data/".to_string()
        );
        assert_eq!(data_region_request.column_metadatas.len(), 4);
        assert_eq!(
            data_region_request.primary_key,
            vec![ReservedColumnId::metric_name(), ReservedColumnId::tsid()]
        );
    }

    #[tokio::test]
    async fn test_alter_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();
        let engine_inner = engine.inner;

        // alter physical region
        let physical_region_id = env.default_physical_region_id();
        let request = RegionAlterRequest {
            schema_version: 0,
            kind: AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: ColumnMetadata {
                        column_id: 0,
                        semantic_type: SemanticType::Tag,
                        column_schema: ColumnSchema::new(
                            "tag1",
                            ConcreteDataType::string_datatype(),
                            false,
                        ),
                    },
                    location: None,
                }],
            },
        };

        let result = engine_inner
            .alter_physical_region(physical_region_id, request.clone())
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Alter request to physical region is forbidden".to_string()
        );

        // alter logical region
        let metadata_region = env.metadata_region();
        let logical_region_id = env.default_logical_region_id();
        let is_column_exist = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, "tag1")
            .await
            .unwrap()
            .is_some();
        assert!(!is_column_exist);

        let region_id = env.default_logical_region_id();
        engine_inner
            .alter_logical_region(region_id, request)
            .await
            .unwrap();
        let semantic_type = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, "tag1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(semantic_type, SemanticType::Tag);
        let timestamp_index = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, "greptime_timestamp")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(timestamp_index, SemanticType::Timestamp);
    }

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
