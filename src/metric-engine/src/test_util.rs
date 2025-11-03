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

//! Utilities for testing.

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema as PbColumnSchema, Row, SemanticType, Value};
use common_meta::ddl::utils::parse_column_metadatas;
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_telemetry::debug;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use mito2::test_util::TestEnv as MitoTestEnv;
use object_store::ObjectStore;
use object_store::util::join_dir;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    ALTER_PHYSICAL_EXTENSION_KEY, LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME,
    PHYSICAL_TABLE_METADATA_KEY, TABLE_COLUMN_METADATA_EXTENSION_KEY,
};
use store_api::path_utils::table_dir;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AddColumn, AlterKind, PathType, RegionAlterRequest, RegionCreateRequest, RegionOpenRequest,
    RegionRequest,
};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, RegionId};

use crate::config::EngineConfig;
use crate::data_region::DataRegion;
use crate::engine::MetricEngine;
use crate::metadata_region::MetadataRegion;

/// Env to test metric engine.
pub struct TestEnv {
    mito_env: MitoTestEnv,
    mito: MitoEngine,
    metric: MetricEngine,
}

impl TestEnv {
    /// Returns a new env with empty prefix for test.
    pub async fn new() -> Self {
        common_telemetry::init_default_ut_logging();
        Self::with_prefix("").await
    }

    /// Returns a new env with specific `prefix` for test.
    pub async fn with_prefix(prefix: &str) -> Self {
        Self::with_prefix_and_config(prefix, EngineConfig::default()).await
    }

    /// Returns a new env with specific `prefix` and `config` for test.
    pub async fn with_prefix_and_config(prefix: &str, config: EngineConfig) -> Self {
        let mut mito_env = MitoTestEnv::with_prefix(prefix).await;
        let mito = mito_env.create_engine(MitoConfig::default()).await;
        let metric = MetricEngine::try_new(mito.clone(), config).unwrap();
        Self {
            mito_env,
            mito,
            metric,
        }
    }

    /// Returns a new env with specific `prefix` and `mito_env` for test.
    pub async fn with_mito_env(mut mito_env: MitoTestEnv) -> Self {
        let mito = mito_env.create_engine(MitoConfig::default()).await;
        let metric = MetricEngine::try_new(mito.clone(), EngineConfig::default()).unwrap();
        Self {
            mito_env,
            mito,
            metric,
        }
    }

    pub fn data_home(&self) -> String {
        let env_root = self.mito_env.data_home().to_string_lossy().to_string();
        join_dir(&env_root, "data")
    }

    pub fn get_object_store(&self) -> Option<ObjectStore> {
        self.mito_env.get_object_store()
    }

    /// Returns a reference to the engine.
    pub fn mito(&self) -> MitoEngine {
        self.mito.clone()
    }

    pub fn metric(&self) -> MetricEngine {
        self.metric.clone()
    }

    /// Creates a new follower engine with the same config as the leader engine.
    pub async fn create_follower_engine(&mut self) -> (MitoEngine, MetricEngine) {
        let mito = self
            .mito_env
            .create_follower_engine(MitoConfig::default())
            .await;
        let metric = MetricEngine::try_new(mito.clone(), EngineConfig::default()).unwrap();

        let region_id = self.default_physical_region_id();
        debug!("opening default physical region: {region_id}");
        let physical_region_option = [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
            .into_iter()
            .collect();
        metric
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: METRIC_ENGINE_NAME.to_string(),
                    table_dir: Self::default_table_dir(),
                    path_type: PathType::Bare, // Use Bare path type for engine regions
                    options: physical_region_option,
                    skip_wal_replay: true,
                    checkpoint: None,
                }),
            )
            .await
            .unwrap();
        (mito, metric)
    }

    /// Create regions in [MetricEngine] with specific `physical_region_id`.
    pub async fn create_physical_region(
        &self,
        physical_region_id: RegionId,
        table_dir: &str,
        options: Vec<(String, String)>,
    ) {
        let region_create_request = RegionCreateRequest {
            engine: METRIC_ENGINE_NAME.to_string(),
            column_metadatas: vec![
                ColumnMetadata {
                    column_id: 0,
                    semantic_type: SemanticType::Timestamp,
                    column_schema: ColumnSchema::new(
                        greptime_timestamp(),
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                },
                ColumnMetadata {
                    column_id: 1,
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        greptime_value(),
                        ConcreteDataType::float64_datatype(),
                        false,
                    ),
                },
            ],
            primary_key: vec![],
            options: [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
                .into_iter()
                .chain(options.into_iter())
                .collect(),
            table_dir: table_dir.to_string(),
            path_type: PathType::Bare, // Use Bare path type for engine regions
            partition_expr_json: Some("".to_string()),
        };

        // create physical region
        let response = self
            .metric()
            .handle_request(
                physical_region_id,
                RegionRequest::Create(region_create_request),
            )
            .await
            .unwrap();
        let column_metadatas =
            parse_column_metadatas(&response.extensions, TABLE_COLUMN_METADATA_EXTENSION_KEY)
                .unwrap();
        assert_eq!(column_metadatas.len(), 4);
    }

    /// Create logical region in [MetricEngine] with specific `physical_region_id` and `logical_region_id`.
    pub async fn create_logical_region(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) {
        let region_create_request = create_logical_region_request(
            &["job"],
            physical_region_id,
            &table_dir("test", logical_region_id.table_id()),
        );
        let response = self
            .metric()
            .handle_request(
                logical_region_id,
                RegionRequest::Create(region_create_request),
            )
            .await
            .unwrap();
        let column_metadatas =
            parse_column_metadatas(&response.extensions, ALTER_PHYSICAL_EXTENSION_KEY).unwrap();
        assert_eq!(column_metadatas.len(), 5);
        let column_names = column_metadatas
            .iter()
            .map(|c| c.column_schema.name.as_str())
            .collect::<Vec<_>>();
        let column_ids = column_metadatas
            .iter()
            .map(|c| c.column_id)
            .collect::<Vec<_>>();
        assert_eq!(
            column_names,
            vec![
                greptime_timestamp(),
                greptime_value(),
                "__table_id",
                "__tsid",
                "job",
            ]
        );
        assert_eq!(
            column_ids,
            vec![
                0,
                1,
                ReservedColumnId::table_id(),
                ReservedColumnId::tsid(),
                2,
            ]
        );
    }

    /// Create regions in [MetricEngine] under [`default_region_id`]
    /// and region dir `"test_metric_region"`.
    ///
    /// This method will create one logical region with three columns `(ts, val, job)`
    /// under [`default_logical_region_id`].
    pub async fn init_metric_region(&self) {
        let physical_region_id = self.default_physical_region_id();
        self.create_physical_region(physical_region_id, &Self::default_table_dir(), vec![])
            .await;
        let logical_region_id = self.default_logical_region_id();
        self.create_logical_region(physical_region_id, logical_region_id)
            .await;
    }

    pub fn metadata_region(&self) -> MetadataRegion {
        MetadataRegion::new(self.mito())
    }

    pub fn data_region(&self) -> DataRegion {
        DataRegion::new(self.mito())
    }

    /// Default physical region id `RegionId::new(1, 2)`
    pub fn default_physical_region_id(&self) -> RegionId {
        RegionId::new(1, 2)
    }

    /// Default logical region id `RegionId::new(3, 2)`
    pub fn default_logical_region_id(&self) -> RegionId {
        RegionId::new(3, 2)
    }

    /// Default table dir `test_metric_table`
    pub fn default_table_dir() -> String {
        "test_metric_region".to_string()
    }
}

/// Generate a [RegionAlterRequest] for adding tag columns.
pub fn alter_logical_region_add_tag_columns(
    col_id_start: ColumnId,
    new_tags: &[&str],
) -> RegionAlterRequest {
    let mut new_columns = vec![];
    for (i, tag) in new_tags.iter().enumerate() {
        new_columns.push(AddColumn {
            column_metadata: ColumnMetadata {
                column_id: i as u32 + col_id_start,
                semantic_type: SemanticType::Tag,
                column_schema: ColumnSchema::new(
                    tag.to_string(),
                    ConcreteDataType::string_datatype(),
                    false,
                ),
            },
            location: None,
        });
    }
    RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: new_columns,
        },
    }
}

/// Generate a [RegionCreateRequest] for logical region.
/// Only need to specify tag column's name
pub fn create_logical_region_request(
    tags: &[&str],
    physical_region_id: RegionId,
    table_dir: &str,
) -> RegionCreateRequest {
    let mut column_metadatas = vec![
        ColumnMetadata {
            column_id: 0,
            semantic_type: SemanticType::Timestamp,
            column_schema: ColumnSchema::new(
                greptime_timestamp(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        },
        ColumnMetadata {
            column_id: 1,
            semantic_type: SemanticType::Field,
            column_schema: ColumnSchema::new(
                greptime_value(),
                ConcreteDataType::float64_datatype(),
                false,
            ),
        },
    ];
    for (bias, tag) in tags.iter().enumerate() {
        column_metadatas.push(ColumnMetadata {
            column_id: 2 + bias as ColumnId,
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                tag.to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
        });
    }
    RegionCreateRequest {
        engine: METRIC_ENGINE_NAME.to_string(),
        column_metadatas,
        primary_key: vec![],
        options: [(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            physical_region_id.as_u64().to_string(),
        )]
        .into_iter()
        .collect(),
        table_dir: table_dir.to_string(),
        path_type: PathType::Bare, // Use Bare path type for engine regions
        partition_expr_json: Some("".to_string()),
    }
}

/// Generate a [RegionAlterRequest] for logical region.
/// Only need to specify tag column's name
pub fn alter_logical_region_request(tags: &[&str]) -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: tags
                .iter()
                .map(|tag| AddColumn {
                    column_metadata: ColumnMetadata {
                        column_id: 0,
                        semantic_type: SemanticType::Tag,
                        column_schema: ColumnSchema::new(
                            tag.to_string(),
                            ConcreteDataType::string_datatype(),
                            false,
                        ),
                    },
                    location: None,
                })
                .collect::<Vec<_>>(),
        },
    }
}

/// Generate a row schema with given tag columns.
///
/// The result will also contains default timestamp and value column at beginning.
pub fn row_schema_with_tags(tags: &[&str]) -> Vec<PbColumnSchema> {
    let mut schema = vec![
        PbColumnSchema {
            column_name: greptime_timestamp().to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as _,
            datatype_extension: None,
            options: None,
        },
        PbColumnSchema {
            column_name: greptime_value().to_string(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as _,
            datatype_extension: None,
            options: None,
        },
    ];
    for tag in tags {
        schema.push(PbColumnSchema {
            column_name: tag.to_string(),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
            options: None,
        });
    }
    schema
}

/// Build [Row]s for assembling [RegionPutRequest](store_api::region_request::RegionPutRequest).
///
/// The schema is generated by [row_schema_with_tags]. `num_tags` doesn't need to be precise,
/// it's used to determine the column id for new columns.
pub fn build_rows(num_tags: usize, num_rows: usize) -> Vec<Row> {
    let mut rows = vec![];
    for i in 0..num_rows {
        let mut values = vec![
            Value {
                value_data: Some(ValueData::TimestampMillisecondValue(i as _)),
            },
            Value {
                value_data: Some(ValueData::F64Value(i as f64)),
            },
        ];
        for j in 0..num_tags {
            values.push(Value {
                value_data: Some(ValueData::StringValue(format!("tag_{}", j))),
            });
        }
        rows.push(Row { values });
    }
    rows
}

#[macro_export]
/// Skip the test if the environment variable `GT_KAFKA_ENDPOINTS` is not set.
///
/// The format of the environment variable is:
/// ```text
/// GT_KAFKA_ENDPOINTS=localhost:9092,localhost:9093
/// ```
macro_rules! maybe_skip_kafka_log_store_integration_test {
    () => {
        if std::env::var("GT_KAFKA_ENDPOINTS").is_err() {
            common_telemetry::warn!("The kafka endpoints is empty, skipping the test");
            return;
        }
    };
}

#[cfg(test)]
mod test {
    use object_store::ObjectStore;
    use object_store::services::Fs;
    use store_api::metric_engine_consts::{DATA_REGION_SUBDIR, METADATA_REGION_SUBDIR};

    use super::*;
    use crate::utils::{self, to_metadata_region_id};

    #[tokio::test]
    async fn create_metadata_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let region_id = to_metadata_region_id(env.default_physical_region_id());

        let builder = Fs::default().root(&env.data_home());
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let table_dir = TestEnv::default_table_dir();
        let region_dir = join_dir(&table_dir, "1_0000000002");
        // assert metadata region's dir
        let metadata_region_dir = join_dir(&region_dir, METADATA_REGION_SUBDIR);
        let exist = object_store.exists(&metadata_region_dir).await.unwrap();
        assert!(exist);

        // assert data region's dir
        let data_region_dir = join_dir(&region_dir, DATA_REGION_SUBDIR);
        let exist = object_store.exists(&data_region_dir).await.unwrap();
        assert!(exist);

        // check mito engine
        let metadata_region_id = utils::to_metadata_region_id(region_id);
        let _ = env.mito().get_metadata(metadata_region_id).await.unwrap();
        let data_region_id = utils::to_data_region_id(region_id);
        let _ = env.mito().get_metadata(data_region_id).await.unwrap();
    }
}
