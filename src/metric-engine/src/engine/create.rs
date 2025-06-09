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

mod extract_new_columns;

use std::collections::{HashMap, HashSet};

use api::v1::SemanticType;
use common_telemetry::info;
use common_time::{Timestamp, FOREVER};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use datatypes::value::Value;
use mito2::engine::MITO_ENGINE_NAME;
use object_store::util::join_dir;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    ALTER_PHYSICAL_EXTENSION_KEY, DATA_REGION_SUBDIR, DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
    DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY, METADATA_REGION_SUBDIR,
    METADATA_SCHEMA_KEY_COLUMN_INDEX, METADATA_SCHEMA_KEY_COLUMN_NAME,
    METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX, METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
    METADATA_SCHEMA_VALUE_COLUMN_INDEX, METADATA_SCHEMA_VALUE_COLUMN_NAME,
};
use store_api::mito_engine_options::{
    APPEND_MODE_KEY, MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING, SKIP_WAL_KEY, TTL_KEY,
};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionCreateRequest, RegionRequest};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::RegionId;

use crate::engine::create::extract_new_columns::extract_new_columns;
use crate::engine::options::{set_data_region_options, PhysicalRegionOptions};
use crate::engine::MetricEngineInner;
use crate::error::{
    ColumnTypeMismatchSnafu, ConflictRegionOptionSnafu, CreateMitoRegionSnafu,
    InternalColumnOccupiedSnafu, InvalidMetadataSnafu, MissingRegionOptionSnafu,
    MultipleFieldColumnSnafu, NoFieldColumnSnafu, ParseRegionIdSnafu, PhysicalRegionNotFoundSnafu,
    Result, SerializeColumnMetadataSnafu, UnexpectedRequestSnafu,
};
use crate::metrics::PHYSICAL_REGION_COUNT;
use crate::utils::{
    self, append_manifest_info, encode_manifest_info_to_extensions, to_data_region_id,
    to_metadata_region_id,
};

const DEFAULT_TABLE_ID_SKIPPING_INDEX_GRANULARITY: u32 = 1024;

impl MetricEngineInner {
    pub async fn create_regions(
        &self,
        mut requests: Vec<(RegionId, RegionCreateRequest)>,
        extension_return_value: &mut HashMap<String, Vec<u8>>,
    ) -> Result<AffectedRows> {
        if requests.is_empty() {
            return Ok(0);
        }

        for (_, request) in requests.iter() {
            Self::verify_region_create_request(request)?;
        }

        let first_request = &requests.first().unwrap().1;
        if first_request.is_physical_table() {
            ensure!(
                requests.len() == 1,
                UnexpectedRequestSnafu {
                    reason: "Physical table must be created with single request".to_string(),
                }
            );
            let (region_id, request) = requests.pop().unwrap();
            self.create_physical_region(region_id, request).await?;

            return Ok(0);
        } else if first_request
            .options
            .contains_key(LOGICAL_TABLE_METADATA_KEY)
        {
            if requests.len() == 1 {
                let request = &requests.first().unwrap().1;
                let physical_region_id = parse_physical_region_id(request)?;
                let mut manifest_infos = Vec::with_capacity(1);
                self.create_logical_regions(physical_region_id, requests, extension_return_value)
                    .await?;
                append_manifest_info(&self.mito, physical_region_id, &mut manifest_infos);
                encode_manifest_info_to_extensions(&manifest_infos, extension_return_value)?;
            } else {
                let grouped_requests =
                    group_create_logical_region_requests_by_physical_region_id(requests)?;
                let mut manifest_infos = Vec::with_capacity(grouped_requests.len());
                for (physical_region_id, requests) in grouped_requests {
                    self.create_logical_regions(
                        physical_region_id,
                        requests,
                        extension_return_value,
                    )
                    .await?;
                    append_manifest_info(&self.mito, physical_region_id, &mut manifest_infos);
                }
                encode_manifest_info_to_extensions(&manifest_infos, extension_return_value)?;
            }
        } else {
            return MissingRegionOptionSnafu {}.fail();
        }

        Ok(0)
    }

    /// Initialize a physical metric region at given region id.
    async fn create_physical_region(
        &self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<()> {
        let physical_region_options = PhysicalRegionOptions::try_from(&request.options)?;
        let (data_region_id, metadata_region_id) = Self::transform_region_id(region_id);

        // create metadata region
        let create_metadata_region_request = self.create_request_for_metadata_region(&request);
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
        let physical_columns = create_data_region_request
            .column_metadatas
            .iter()
            .map(|metadata| (metadata.column_schema.name.clone(), metadata.column_id))
            .collect::<HashMap<_, _>>();
        let time_index_unit = create_data_region_request
            .column_metadatas
            .iter()
            .find_map(|metadata| {
                if metadata.semantic_type == SemanticType::Timestamp {
                    metadata
                        .column_schema
                        .data_type
                        .as_timestamp()
                        .map(|data_type| data_type.unit())
                } else {
                    None
                }
            })
            .context(UnexpectedRequestSnafu {
                reason: "No time index column found",
            })?;
        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Create(create_data_region_request),
            )
            .await
            .with_context(|_| CreateMitoRegionSnafu {
                region_type: DATA_REGION_SUBDIR,
            })?;
        let primary_key_encoding = self.mito.get_primary_key_encoding(data_region_id).context(
            PhysicalRegionNotFoundSnafu {
                region_id: data_region_id,
            },
        )?;

        info!("Created physical metric region {region_id}, primary key encoding={primary_key_encoding}, physical_region_options={physical_region_options:?}");
        PHYSICAL_REGION_COUNT.inc();

        // remember this table
        self.state.write().unwrap().add_physical_region(
            data_region_id,
            physical_columns,
            primary_key_encoding,
            physical_region_options,
            time_index_unit,
        );

        Ok(())
    }

    /// Create multiple logical regions on the same physical region.
    async fn create_logical_regions(
        &self,
        physical_region_id: RegionId,
        requests: Vec<(RegionId, RegionCreateRequest)>,
        extension_return_value: &mut HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        let data_region_id = utils::to_data_region_id(physical_region_id);

        let unit = self
            .state
            .read()
            .unwrap()
            .physical_region_time_index_unit(physical_region_id)
            .context(PhysicalRegionNotFoundSnafu {
                region_id: data_region_id,
            })?;
        // Checks the time index unit of each request.
        for (_, request) in &requests {
            // Safety: verify_region_create_request() ensures that the request is valid.
            let time_index_column = request
                .column_metadatas
                .iter()
                .find(|col| col.semantic_type == SemanticType::Timestamp)
                .unwrap();
            let request_unit = time_index_column
                .column_schema
                .data_type
                .as_timestamp()
                .unwrap()
                .unit();
            ensure!(
                request_unit == unit,
                UnexpectedRequestSnafu {
                    reason: format!(
                        "Metric has differenttime unit ({:?}) than the physical region ({:?})",
                        request_unit, unit
                    ),
                }
            );
        }

        // Filters out the requests that the logical region already exists
        let requests = {
            let state = self.state.read().unwrap();
            let mut skipped = Vec::with_capacity(requests.len());
            let mut kept_requests = Vec::with_capacity(requests.len());

            for (region_id, request) in requests {
                if state.is_logical_region_exist(region_id) {
                    skipped.push(region_id);
                } else {
                    kept_requests.push((region_id, request));
                }
            }

            // log skipped regions
            if !skipped.is_empty() {
                info!(
                    "Skipped creating logical regions {skipped:?} because they already exist",
                    skipped = skipped
                );
            }
            kept_requests
        };

        // Finds new columns to add to physical region
        let mut new_column_names = HashSet::new();
        let mut new_columns = Vec::new();

        let index_option = {
            let state = &self.state.read().unwrap();
            let region_state = state
                .physical_region_states()
                .get(&data_region_id)
                .with_context(|| PhysicalRegionNotFoundSnafu {
                    region_id: data_region_id,
                })?;
            let physical_columns = region_state.physical_columns();

            extract_new_columns(
                &requests,
                physical_columns,
                &mut new_column_names,
                &mut new_columns,
            )?;
            region_state.options().index
        };

        // TODO(weny): we dont need to pass a mutable new_columns here.
        self.data_region
            .add_columns(data_region_id, new_columns, index_option)
            .await?;

        let physical_columns = self.data_region.physical_columns(data_region_id).await?;
        let physical_schema_map = physical_columns
            .iter()
            .map(|metadata| (metadata.column_schema.name.as_str(), metadata))
            .collect::<HashMap<_, _>>();
        let logical_regions = requests
            .iter()
            .map(|(region_id, _)| (*region_id))
            .collect::<Vec<_>>();
        let logical_region_columns = requests.iter().map(|(region_id, request)| {
            (
                *region_id,
                request
                    .column_metadatas
                    .iter()
                    .map(|metadata| {
                        // Safety: previous steps ensure the physical region exist
                        let column_metadata = *physical_schema_map
                            .get(metadata.column_schema.name.as_str())
                            .unwrap();
                        (metadata.column_schema.name.as_str(), column_metadata)
                    })
                    .collect::<HashMap<_, _>>(),
            )
        });

        let new_add_columns = new_column_names.iter().map(|name| {
            // Safety: previous steps ensure the physical region exist
            let column_metadata = *physical_schema_map.get(name).unwrap();
            (name.to_string(), column_metadata.column_id)
        });

        extension_return_value.insert(
            ALTER_PHYSICAL_EXTENSION_KEY.to_string(),
            ColumnMetadata::encode_list(&physical_columns).context(SerializeColumnMetadataSnafu)?,
        );

        // Writes logical regions metadata to metadata region
        self.metadata_region
            .add_logical_regions(physical_region_id, true, logical_region_columns)
            .await?;

        {
            let mut state = self.state.write().unwrap();
            state.add_physical_columns(data_region_id, new_add_columns);
            state.add_logical_regions(physical_region_id, logical_regions.clone());
        }
        for logical_region_id in logical_regions {
            self.metadata_region
                .open_logical_region(logical_region_id)
                .await;
        }

        Ok(())
    }

    /// Check if
    /// - internal columns are not occupied
    /// - required table option is present ([PHYSICAL_TABLE_METADATA_KEY] or
    ///   [LOGICAL_TABLE_METADATA_KEY])
    fn verify_region_create_request(request: &RegionCreateRequest) -> Result<()> {
        request.validate().context(InvalidMetadataSnafu)?;

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
            request.is_physical_table() || request.options.contains_key(LOGICAL_TABLE_METADATA_KEY),
            MissingRegionOptionSnafu {}
        );
        ensure!(
            !(request.is_physical_table()
                && request.options.contains_key(LOGICAL_TABLE_METADATA_KEY)),
            ConflictRegionOptionSnafu {}
        );

        // check if only one field column is declared, and all tag columns are string
        let mut field_col: Option<&ColumnMetadata> = None;
        for col in &request.column_metadatas {
            match col.semantic_type {
                SemanticType::Tag => ensure!(
                    col.column_schema.data_type == ConcreteDataType::string_datatype(),
                    ColumnTypeMismatchSnafu {
                        expect: ConcreteDataType::string_datatype(),
                        actual: col.column_schema.data_type.clone(),
                    }
                ),
                SemanticType::Field => {
                    if field_col.is_some() {
                        MultipleFieldColumnSnafu {
                            previous: field_col.unwrap().column_schema.name.clone(),
                            current: col.column_schema.name.clone(),
                        }
                        .fail()?;
                    }
                    field_col = Some(col)
                }
                SemanticType::Timestamp => {}
            }
        }
        let field_col = field_col.context(NoFieldColumnSnafu)?;

        // make sure the field column is float64 type
        ensure!(
            field_col.column_schema.data_type == ConcreteDataType::float64_datatype(),
            ColumnTypeMismatchSnafu {
                expect: ConcreteDataType::float64_datatype(),
                actual: field_col.column_schema.data_type.clone(),
            }
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
    pub fn create_request_for_metadata_region(
        &self,
        request: &RegionCreateRequest,
    ) -> RegionCreateRequest {
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
        let metadata_region_dir = join_dir(&request.region_dir, METADATA_REGION_SUBDIR);

        let options = region_options_for_metadata_region(request.options.clone());
        RegionCreateRequest {
            engine: MITO_ENGINE_NAME.to_string(),
            column_metadatas: vec![
                timestamp_column_metadata,
                key_column_metadata,
                value_column_metadata,
            ],
            primary_key: vec![METADATA_SCHEMA_KEY_COLUMN_INDEX as _],
            options,
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
        let mut primary_key = vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()];

        // concat region dir
        data_region_request.region_dir = join_dir(&request.region_dir, DATA_REGION_SUBDIR);

        // change nullability for tag columns
        data_region_request
            .column_metadatas
            .iter_mut()
            .for_each(|metadata| {
                if metadata.semantic_type == SemanticType::Tag {
                    metadata.column_schema.set_nullable();
                    primary_key.push(metadata.column_id);
                }
            });

        // add internal columns
        let [table_id_col, tsid_col] = Self::internal_column_metadata();
        data_region_request.column_metadatas.push(table_id_col);
        data_region_request.column_metadatas.push(tsid_col);
        data_region_request.primary_key = primary_key;

        // set data region options
        set_data_region_options(
            &mut data_region_request.options,
            self.config.experimental_sparse_primary_key_encoding,
        );

        data_region_request
    }

    /// Generate internal column metadata.
    ///
    /// Return `[table_id_col, tsid_col]`
    fn internal_column_metadata() -> [ColumnMetadata; 2] {
        // Safety: BloomFilter is a valid skipping index type
        let metric_name_col = ColumnMetadata {
            column_id: ReservedColumnId::table_id(),
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::uint32_datatype(),
                false,
            )
            .with_skipping_options(SkippingIndexOptions {
                granularity: DEFAULT_TABLE_ID_SKIPPING_INDEX_GRANULARITY,
                index_type: datatypes::schema::SkippingIndexType::BloomFilter,
            })
            .unwrap(),
        };
        let tsid_col = ColumnMetadata {
            column_id: ReservedColumnId::tsid(),
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            )
            .with_inverted_index(false),
        };
        [metric_name_col, tsid_col]
    }
}

/// Groups the create logical region requests by physical region id.
fn group_create_logical_region_requests_by_physical_region_id(
    requests: Vec<(RegionId, RegionCreateRequest)>,
) -> Result<HashMap<RegionId, Vec<(RegionId, RegionCreateRequest)>>> {
    let mut result = HashMap::with_capacity(requests.len());
    for (region_id, request) in requests {
        let physical_region_id = parse_physical_region_id(&request)?;
        result
            .entry(physical_region_id)
            .or_insert_with(Vec::new)
            .push((region_id, request));
    }

    Ok(result)
}

/// Parses the physical region id from the request.
fn parse_physical_region_id(request: &RegionCreateRequest) -> Result<RegionId> {
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

    Ok(physical_region_id)
}

/// Creates the region options for metadata region in metric engine.
pub(crate) fn region_options_for_metadata_region(
    mut original: HashMap<String, String>,
) -> HashMap<String, String> {
    // TODO(ruihang, weny): add whitelist for metric engine options.
    original.remove(APPEND_MODE_KEY);
    // Don't allow to set primary key encoding for metadata region.
    original.remove(MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING);
    original.insert(TTL_KEY.to_string(), FOREVER.to_string());
    original.remove(SKIP_WAL_KEY);
    original
}

#[cfg(test)]
mod test {
    use store_api::metric_engine_consts::{METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY};

    use super::*;
    use crate::config::EngineConfig;
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
                ColumnMetadata {
                    column_id: 2,
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        "column2".to_string(),
                        ConcreteDataType::float64_datatype(),
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
        MetricEngineInner::verify_region_create_request(&request).unwrap();
    }

    #[test]
    fn test_verify_region_create_request_options() {
        let mut request = RegionCreateRequest {
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
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        "val".to_string(),
                        ConcreteDataType::float64_datatype(),
                        false,
                    ),
                },
            ],
            region_dir: "test_dir".to_string(),
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: HashMap::new(),
        };
        MetricEngineInner::verify_region_create_request(&request).unwrap_err();

        let mut options = HashMap::new();
        options.insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), "value".to_string());
        request.options.clone_from(&options);
        MetricEngineInner::verify_region_create_request(&request).unwrap();

        options.insert(LOGICAL_TABLE_METADATA_KEY.to_string(), "value".to_string());
        request.options.clone_from(&options);
        MetricEngineInner::verify_region_create_request(&request).unwrap_err();

        options.remove(PHYSICAL_TABLE_METADATA_KEY).unwrap();
        request.options = options;
        MetricEngineInner::verify_region_create_request(&request).unwrap();
    }

    #[tokio::test]
    async fn test_create_request_for_physical_regions() {
        // original request
        let options: HashMap<_, _> = [
            ("ttl".to_string(), "60m".to_string()),
            ("skip_wal".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect();
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
            options,
            region_dir: "/test_dir".to_string(),
        };

        // set up
        let env = TestEnv::new().await;
        let engine = MetricEngine::try_new(env.mito(), EngineConfig::default()).unwrap();
        let engine_inner = engine.inner;

        // check create data region request
        let data_region_request = engine_inner.create_request_for_data_region(&request);
        assert_eq!(
            data_region_request.region_dir,
            "/test_dir/data/".to_string()
        );
        assert_eq!(data_region_request.column_metadatas.len(), 4);
        assert_eq!(
            data_region_request.primary_key,
            vec![ReservedColumnId::table_id(), ReservedColumnId::tsid(), 1]
        );
        assert!(data_region_request.options.contains_key("ttl"));

        // check create metadata region request
        let metadata_region_request = engine_inner.create_request_for_metadata_region(&request);
        assert_eq!(
            metadata_region_request.region_dir,
            "/test_dir/metadata/".to_string()
        );
        assert_eq!(
            metadata_region_request.options.get("ttl").unwrap(),
            "forever"
        );
        assert!(!metadata_region_request.options.contains_key("skip_wal"));
    }
}
