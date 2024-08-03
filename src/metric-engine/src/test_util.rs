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
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use mito2::test_util::TestEnv as MitoTestEnv;
use object_store::util::join_dir;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AddColumn, AlterKind, RegionAlterRequest, RegionCreateRequest, RegionRequest,
};
use store_api::storage::{ColumnId, RegionId};

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
        let mut mito_env = MitoTestEnv::with_prefix(prefix);
        let mito = mito_env.create_engine(MitoConfig::default()).await;
        let metric = MetricEngine::new(mito.clone());
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

    /// Returns a reference to the engine.
    pub fn mito(&self) -> MitoEngine {
        self.mito.clone()
    }

    pub fn metric(&self) -> MetricEngine {
        self.metric.clone()
    }

    /// Create regions in [MetricEngine] under [`default_region_id`]
    /// and region dir `"test_metric_region"`.
    ///
    /// This method will create one logical region with three columns `(ts, val, job)`
    /// under [`default_logical_region_id`].
    pub async fn init_metric_region(&self) {
        let region_id = self.default_physical_region_id();
        let region_create_request = RegionCreateRequest {
            engine: METRIC_ENGINE_NAME.to_string(),
            column_metadatas: vec![
                ColumnMetadata {
                    column_id: 0,
                    semantic_type: SemanticType::Timestamp,
                    column_schema: ColumnSchema::new(
                        "greptime_timestamp",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                },
                ColumnMetadata {
                    column_id: 1,
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        "greptime_value",
                        ConcreteDataType::float64_datatype(),
                        false,
                    ),
                },
            ],
            primary_key: vec![],
            options: [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
                .into_iter()
                .collect(),
            region_dir: self.default_region_dir(),
        };

        // create physical region
        self.metric()
            .handle_request(region_id, RegionRequest::Create(region_create_request))
            .await
            .unwrap();

        // create logical region
        let region_id = self.default_logical_region_id();
        let region_create_request = create_logical_region_request(
            &["job"],
            self.default_physical_region_id(),
            "test_metric_logical_region",
        );
        self.metric()
            .handle_request(region_id, RegionRequest::Create(region_create_request))
            .await
            .unwrap();
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

    /// Default region dir `test_metric_region`
    pub fn default_region_dir(&self) -> String {
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
        schema_version: 0,
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
    region_dir: &str,
) -> RegionCreateRequest {
    let mut column_metadatas = vec![
        ColumnMetadata {
            column_id: 0,
            semantic_type: SemanticType::Timestamp,
            column_schema: ColumnSchema::new(
                "greptime_timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        },
        ColumnMetadata {
            column_id: 1,
            semantic_type: SemanticType::Field,
            column_schema: ColumnSchema::new(
                "greptime_value",
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
        region_dir: region_dir.to_string(),
    }
}

/// Generate a row schema with given tag columns.
///
/// The result will also contains default timestamp and value column at beginning.
pub fn row_schema_with_tags(tags: &[&str]) -> Vec<PbColumnSchema> {
    let mut schema = vec![
        PbColumnSchema {
            column_name: "greptime_timestamp".to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as _,
            datatype_extension: None,
            options: None,
        },
        PbColumnSchema {
            column_name: "greptime_value".to_string(),
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

#[cfg(test)]
mod test {
    use object_store::services::Fs;
    use object_store::ObjectStore;
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

        let region_dir = "test_metric_region";
        // assert metadata region's dir
        let metadata_region_dir = join_dir(region_dir, METADATA_REGION_SUBDIR);
        let exist = object_store.is_exist(&metadata_region_dir).await.unwrap();
        assert!(exist);

        // assert data region's dir
        let data_region_dir = join_dir(region_dir, DATA_REGION_SUBDIR);
        let exist = object_store.is_exist(&data_region_dir).await.unwrap();
        assert!(exist);

        // check mito engine
        let metadata_region_id = utils::to_metadata_region_id(region_id);
        let _ = env.mito().get_metadata(metadata_region_id).await.unwrap();
        let data_region_id = utils::to_data_region_id(region_id);
        let _ = env.mito().get_metadata(data_region_id).await.unwrap();
    }
}
