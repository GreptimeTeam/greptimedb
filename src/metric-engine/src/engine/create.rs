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

use api::v1::SemanticType;
use common_telemetry::info;
use common_time::Timestamp;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use mito2::engine::MITO_ENGINE_NAME;
use object_store::util::join_dir;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    DATA_REGION_SUBDIR, DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    LOGICAL_TABLE_METADATA_KEY, METADATA_REGION_SUBDIR, METADATA_SCHEMA_KEY_COLUMN_INDEX,
    METADATA_SCHEMA_KEY_COLUMN_NAME, METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX,
    METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME, METADATA_SCHEMA_VALUE_COLUMN_INDEX,
    METADATA_SCHEMA_VALUE_COLUMN_NAME, PHYSICAL_TABLE_METADATA_KEY,
};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionCreateRequest, RegionRequest};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{
    ConflictRegionOptionSnafu, CreateMitoRegionSnafu, InternalColumnOccupiedSnafu,
    MissingRegionOptionSnafu, ParseRegionIdSnafu, PhysicalRegionNotFoundSnafu, Result,
};
use crate::metrics::{LOGICAL_REGION_COUNT, PHYSICAL_COLUMN_COUNT, PHYSICAL_REGION_COUNT};
use crate::utils::{to_data_region_id, to_metadata_region_id};

impl MetricEngineInner {
    /// Dispatch region creation request to physical region creation or logical
    pub async fn create_region(
        &self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<AffectedRows> {
        Self::verify_region_create_request(&request)?;

        let result = if request.options.contains_key(PHYSICAL_TABLE_METADATA_KEY) {
            self.create_physical_region(region_id, request).await
        } else if request.options.contains_key(LOGICAL_TABLE_METADATA_KEY) {
            self.create_logical_region(region_id, request).await
        } else {
            MissingRegionOptionSnafu {}.fail()
        };

        result.map(|_| 0)
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

        info!("Created physical metric region {region_id}");
        PHYSICAL_REGION_COUNT.inc();

        // remember this table
        self.state
            .write()
            .unwrap()
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
            let state = &self.state.read().unwrap();
            let physical_columns =
                state
                    .physical_columns()
                    .get(&data_region_id)
                    .with_context(|| PhysicalRegionNotFoundSnafu {
                        region_id: data_region_id,
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
                .add_column(metadata_region_id, logical_region_id, col)
                .await?;
        }

        // update the mapping
        // Safety: previous steps ensure the physical region exist
        self.state
            .write()
            .unwrap()
            .add_logical_region(physical_region_id, logical_region_id);
        info!("Created new logical region {logical_region_id} on physical region {data_region_id}");
        LOGICAL_REGION_COUNT.inc();

        Ok(())
    }

    pub(crate) async fn add_columns_to_physical_data_region(
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
                .add_column(metadata_region_id, logical_region_id, col)
                .await?;
        }

        // safety: previous step has checked this
        self.state.write().unwrap().add_physical_columns(
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
            !name_to_index.contains_key(DATA_SCHEMA_TABLE_ID_COLUMN_NAME),
            InternalColumnOccupiedSnafu {
                column: DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
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
            to_data_region_id(region_id),
            to_metadata_region_id(region_id),
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
        let [table_id_col, tsid_col] = Self::internal_column_metadata();
        data_region_request.column_metadatas.push(table_id_col);
        data_region_request.column_metadatas.push(tsid_col);
        data_region_request.primary_key =
            vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()];

        data_region_request
    }

    /// Generate internal column metadata.
    ///
    /// Return `[table_id_col, tsid_col]`
    fn internal_column_metadata() -> [ColumnMetadata; 2] {
        let metric_name_col = ColumnMetadata {
            column_id: ReservedColumnId::table_id(),
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::uint32_datatype(),
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

#[cfg(test)]
mod test {
    use store_api::metric_engine_consts::METRIC_ENGINE_NAME;

    use super::*;
    use crate::engine::MetricEngine;
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
                        DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                        ConcreteDataType::uint32_datatype(),
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
            "Internal column __table_id is reserved".to_string()
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
            vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()]
        );
    }
}
