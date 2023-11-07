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
use std::sync::Arc;

use api::v1::SemanticType;
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
    AlterKind, RegionAlterRequest, RegionCreateRequest, RegionRequest,
};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{RegionGroup, RegionId, ScanRequest, TableId};
use tokio::sync::RwLock;

use crate::data_region::DataRegion;
use crate::error::{
    ConflictRegionOptionSnafu, CreateMitoRegionSnafu, InternalColumnOccupiedSnafu,
    LogicalTableNotFoundSnafu, MissingRegionOptionSnafu, PhysicalRegionNotFoundSnafu,
    PhysicalTableNotFoundSnafu, Result,
};
use crate::metadata_region::MetadataRegion;
use crate::metrics::{LOGICAL_REGION_COUNT, PHYSICAL_COLUMN_COUNT, PHYSICAL_REGION_COUNT};
use crate::utils;

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
pub const LOGICAL_TABLE_METADATA_KEY: &str = "on_physical_table";

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
            RegionRequest::Put(_) => todo!(),
            RegionRequest::Delete(_) => todo!(),
            RegionRequest::Create(create) => self
                .inner
                .create_region(region_id, create)
                .await
                .map(|_| Output::AffectedRows(0)),
            RegionRequest::Drop(_) => todo!(),
            RegionRequest::Open(_) => todo!(),
            RegionRequest::Close(_) => todo!(),
            RegionRequest::Alter(_) => todo!(),
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
                physical_tables: RwLock::default(),
                physical_columns: RwLock::default(),
            }),
        }
    }
}

struct MetricEngineInner {
    mito: MitoEngine,
    metadata_region: MetadataRegion,
    data_region: DataRegion,
    // TODO(ruihang): handle different catalog/schema
    /// Map from physical table name to table id.
    physical_tables: RwLock<HashMap<String, TableId>>,
    /// Cache for the columns of physical regions.
    /// The region id in key is the data region id.
    physical_columns: RwLock<HashMap<RegionId, HashSet<String>>>,
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

        // TODO: workaround for now, should find another way to retrieve the
        // table name.
        let physical_table_name = request
            .options
            .get(PHYSICAL_TABLE_METADATA_KEY)
            .ok_or(MissingRegionOptionSnafu {}.build())?
            .to_string();

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
        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Create(create_data_region_request),
            )
            .await
            .with_context(|_| CreateMitoRegionSnafu {
                region_type: DATA_REGION_SUBDIR,
            })?;

        info!("Created physical metric region {region_id:?} with table name {physical_table_name}");
        PHYSICAL_REGION_COUNT.inc();

        // remember this table
        self.physical_tables
            .write()
            .await
            .insert(physical_table_name, region_id.table_id());

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
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<()> {
        // transform IDs
        let physical_table_name = request
            .options
            .get(LOGICAL_TABLE_METADATA_KEY)
            .ok_or(MissingRegionOptionSnafu {}.build())?;
        let physical_table_id = *self
            .physical_tables
            .read()
            .await
            .get(physical_table_name)
            .with_context(|| PhysicalTableNotFoundSnafu {
                physical_table: physical_table_name,
            })?;
        let logical_table_id = region_id.table_id();
        let physical_region_id = RegionId::new(physical_table_id, region_id.region_number());
        let (data_region_id, metadata_region_id) = Self::transform_region_id(physical_region_id);

        // check if the logical table already exist
        if self
            .metadata_region
            .is_table_exist(metadata_region_id, logical_table_id)
            .await?
        {
            info!("Create a existing logical region {region_id}. Skipped");
            return Ok(());
        }

        // find new columns to add
        let physical_columns = self.physical_columns.read().await;
        let physical_columns =
            physical_columns
                .get(&data_region_id)
                .with_context(|| PhysicalRegionNotFoundSnafu {
                    region_id: data_region_id,
                })?;
        let mut new_columns = vec![];
        for col in &request.column_metadatas {
            if !physical_columns.contains(&col.column_schema.name) {
                new_columns.push(col.clone());
            }
        }

        self.add_columns_to_physical_data_region(
            data_region_id,
            metadata_region_id,
            logical_table_id,
            new_columns,
        )
        .await?;

        Ok(())
    }

    async fn add_columns_to_physical_data_region(
        &self,
        data_region_id: RegionId,
        metadata_region_id: RegionId,
        logical_table_id: TableId,
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
                    logical_table_id,
                    &col.column_schema.name,
                    col.semantic_type,
                )
                .await?;
        }

        let mut physical_columns = self.physical_columns.write().await;
        // safety: previous step has checked this
        let mut column_set = physical_columns.get_mut(&data_region_id).unwrap();
        for col in &new_columns {
            column_set.insert(col.column_schema.name.clone());
        }
        info!("Create table {logical_table_id} leads to adding columns {new_columns:?} to physical region {data_region_id}");
        PHYSICAL_COLUMN_COUNT.add(new_columns.len() as _);

        // register table to metadata region
        self.metadata_region
            .add_table(metadata_region_id, logical_table_id)
            .await?;
        info!("Created new logical table {logical_table_id} on physical region {data_region_id}");
        LOGICAL_REGION_COUNT.inc();

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
                ConcreteDataType::int64_datatype(),
                false,
            ),
        };
        data_region_request.column_metadatas.push(metric_name_col);
        data_region_request.column_metadatas.push(tsid_col);
        data_region_request.primary_key =
            vec![ReservedColumnId::metric_name(), ReservedColumnId::tsid()];

        data_region_request
    }
}

impl MetricEngineInner {
    pub async fn alter_logic_region(
        &self,
        region_id: RegionId,
        request: RegionAlterRequest,
    ) -> Result<()> {
        // only handle adding column
        let AlterKind::AddColumns { columns } = request.kind else {
            return Ok(());
        };
        let logical_table_id = region_id.table_id();

        // check if the table exists
        let metadata_region_id = utils::to_metadata_region_id(region_id);
        if !self
            .metadata_region
            .is_table_exist(metadata_region_id, logical_table_id)
            .await?
        {
            error!("Trying to alter an nonexistent table {logical_table_id}");
            return LogicalTableNotFoundSnafu {
                table_id: logical_table_id,
            }
            .fail();
        }

        let mut columns_to_add = vec![];
        for col in columns {
            if self
                .metadata_region
                .column_semantic_type(
                    metadata_region_id,
                    logical_table_id,
                    &col.column_metadata.column_schema.name,
                )
                .await?
                .is_none()
            {
                columns_to_add.push(col.column_metadata);
            }
        }

        let data_region_id = utils::to_data_region_id(region_id);
        self.add_columns_to_physical_data_region(
            data_region_id,
            metadata_region_id,
            logical_table_id,
            columns_to_add,
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use super::*;
    use crate::test_util::TestEnv;

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
}
