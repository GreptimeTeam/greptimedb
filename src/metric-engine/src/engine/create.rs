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
use common_query::native_histogram::is_native_histogram_value_schema;
use common_telemetry::info;
use common_time::{FOREVER, Timestamp};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use datatypes::value::Value;
use mito2::engine::MITO_ENGINE_NAME;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    ALTER_PHYSICAL_EXTENSION_KEY, DATA_REGION_SUBDIR, DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
    DATA_SCHEMA_TSID_COLUMN_NAME, DATA_SCHEMA_VALUE_INT_COLUMN_SUFFIX, LOGICAL_TABLE_METADATA_KEY,
    METADATA_REGION_SUBDIR, METADATA_SCHEMA_KEY_COLUMN_INDEX, METADATA_SCHEMA_KEY_COLUMN_NAME,
    METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX, METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
    METADATA_SCHEMA_VALUE_COLUMN_INDEX, METADATA_SCHEMA_VALUE_COLUMN_NAME,
    is_metric_engine_internal_column, is_metric_engine_value_int_column,
    metric_engine_value_int_column_name,
};
use store_api::mito_engine_options::{TTL_KEY, WAL_OPTIONS_KEY};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, PathType, RegionCreateRequest, RegionRequest};
use store_api::storage::RegionId;
use store_api::storage::consts::ReservedColumnId;

use crate::engine::MetricEngineInner;
use crate::engine::create::extract_new_columns::extract_new_columns;
use crate::engine::options::{PhysicalRegionOptions, set_data_region_options};
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
const DEFAULT_TABLE_ID_SKIPPING_INDEX_FALSE_POSITIVE_RATE: f64 = 0.01;

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
            self.create_physical_region(region_id, request, extension_return_value)
                .await?;

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
        extension_return_value: &mut HashMap<String, Vec<u8>>,
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
            .map(|metadata| (metadata.column_schema.name.clone(), metadata.clone()))
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
        let response = self
            .mito
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
        extension_return_value.extend(response.extensions);

        info!(
            "Created physical metric region {region_id}, primary key encoding={primary_key_encoding}, physical_region_options={physical_region_options:?}"
        );
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
            .map(|(region_id, _)| *region_id)
            .collect::<Vec<_>>();
        let logical_column_metadata = requests
            .iter()
            .map(|(region_id, request)| {
                (
                    *region_id,
                    request
                        .column_metadatas
                        .iter()
                        .map(|metadata| {
                            // Safety: previous steps ensure the physical region exist
                            let physical_metadata = *physical_schema_map
                                .get(metadata.column_schema.name.as_str())
                                .unwrap();
                            let mut column_metadata = physical_metadata.clone();
                            if metadata.semantic_type == SemanticType::Field {
                                column_metadata.column_schema = metadata.column_schema.clone();
                            }
                            (metadata.column_schema.name.clone(), column_metadata)
                        })
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<Vec<_>>();
        let logical_region_columns = logical_column_metadata.iter().map(|(region_id, columns)| {
            (
                *region_id,
                columns
                    .iter()
                    .map(|(name, column_metadata)| (name.as_str(), column_metadata))
                    .collect::<HashMap<_, _>>(),
            )
        });

        let new_add_columns = new_column_names.iter().map(|name| {
            // Safety: previous steps ensure the physical region exist
            let column_metadata = *physical_schema_map.get(name).unwrap();
            (name.to_string(), column_metadata.clone())
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

        let table_id_col_def = request.column_metadatas.iter().any(is_metric_name_col);
        let tsid_col_def = request.column_metadatas.iter().any(is_tsid_col);

        // check if internal columns are not occupied or defined in the request
        ensure!(
            !name_to_index.contains_key(DATA_SCHEMA_TABLE_ID_COLUMN_NAME) || table_id_col_def,
            InternalColumnOccupiedSnafu {
                column: DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
            }
        );
        ensure!(
            !name_to_index.contains_key(DATA_SCHEMA_TSID_COLUMN_NAME) || tsid_col_def,
            InternalColumnOccupiedSnafu {
                column: DATA_SCHEMA_TSID_COLUMN_NAME,
            }
        );
        for name in name_to_index.keys() {
            ensure!(
                !is_metric_engine_value_int_column(name)
                    || is_valid_physical_metric_value_int_column(request, name),
                InternalColumnOccupiedSnafu { column: name }
            );
        }

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

        // check if field columns are either a normal metric value or a native histogram.
        let mut field_cols = Vec::new();
        for col in &request.column_metadatas {
            // Verified in above steps.
            if is_metric_engine_internal_column(&col.column_schema.name) {
                continue;
            }
            match col.semantic_type {
                SemanticType::Tag => ensure!(
                    col.column_schema.data_type == ConcreteDataType::string_datatype(),
                    ColumnTypeMismatchSnafu {
                        expect: ConcreteDataType::string_datatype(),
                        actual: col.column_schema.data_type.clone(),
                    }
                ),
                SemanticType::Field => {
                    field_cols.push(col);
                }
                SemanticType::Timestamp => {}
            }
        }
        let [field_col] = field_cols.as_slice() else {
            if field_cols.is_empty() {
                NoFieldColumnSnafu.fail()?;
            }
            return MultipleFieldColumnSnafu {
                previous: field_cols[0].column_schema.name.clone(),
                current: field_cols[1].column_schema.name.clone(),
            }
            .fail();
        };

        if is_native_histogram_value_schema(
            &field_col.column_schema.name,
            &field_col.column_schema.data_type,
        ) {
            return Ok(());
        }

        // make sure the normal field column is float64 type
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

        let options = region_options_for_metadata_region(&request.options);
        RegionCreateRequest {
            engine: MITO_ENGINE_NAME.to_string(),
            column_metadatas: vec![
                timestamp_column_metadata,
                key_column_metadata,
                value_column_metadata,
            ],
            primary_key: vec![METADATA_SCHEMA_KEY_COLUMN_INDEX as _],
            options,
            table_dir: request.table_dir.clone(),
            path_type: PathType::Metadata,
            partition_expr_json: Some("".to_string()),
            requirements: request.requirements,
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

        data_region_request.table_dir = request.table_dir.clone();
        data_region_request.path_type = PathType::Data;

        let table_id_col_def = request.column_metadatas.iter().any(is_metric_name_col);
        let tsid_col_def = request.column_metadatas.iter().any(is_tsid_col);
        append_metric_value_int_columns(&mut data_region_request.column_metadatas);

        // change nullability for tag columns
        data_region_request
            .column_metadatas
            .iter_mut()
            .for_each(|metadata| {
                if metadata.semantic_type == SemanticType::Tag
                    && !is_metric_name_col(metadata)
                    && !is_tsid_col(metadata)
                {
                    metadata.column_schema.set_nullable();
                    primary_key.push(metadata.column_id);
                }
            });

        // add internal columns if not defined in the request
        if !table_id_col_def {
            data_region_request.column_metadatas.push(table_id_col());
        }
        if !tsid_col_def {
            data_region_request.column_metadatas.push(tsid_col());
        }
        data_region_request.primary_key = primary_key;

        // set data region options
        set_data_region_options(
            &mut data_region_request.options,
            self.config.sparse_primary_key_encoding,
        );

        data_region_request
    }
}

fn is_valid_physical_metric_value_int_column(request: &RegionCreateRequest, name: &str) -> bool {
    if !request.is_physical_table() {
        return false;
    }

    let Some(value_name) = name.strip_suffix(DATA_SCHEMA_VALUE_INT_COLUMN_SUFFIX) else {
        return false;
    };
    let find_column = |name| {
        request
            .column_metadatas
            .iter()
            .find(|metadata| metadata.column_schema.name == name)
    };
    let Some(value_column) = find_column(value_name) else {
        return false;
    };
    let Some(int_column) = find_column(name) else {
        return false;
    };

    value_column.semantic_type == SemanticType::Field
        && value_column.column_schema.data_type == ConcreteDataType::float64_datatype()
        && int_column.semantic_type == SemanticType::Field
        && int_column.column_schema.data_type == ConcreteDataType::int64_datatype()
}

fn append_metric_value_int_columns(column_metadatas: &mut Vec<ColumnMetadata>) {
    let mut next_column_id = column_metadatas
        .iter()
        .map(|metadata| metadata.column_id)
        .filter(|column_id| !ReservedColumnId::is_reserved(*column_id))
        .max()
        .unwrap_or(0)
        + 1;
    let existing_names = column_metadatas
        .iter()
        .map(|metadata| metadata.column_schema.name.clone())
        .collect::<HashSet<_>>();
    let mut int_columns = Vec::new();

    for metadata in column_metadatas.iter_mut() {
        if is_metric_engine_value_int_column(&metadata.column_schema.name) {
            metadata.column_schema.set_nullable();
            continue;
        }
        if metadata.semantic_type != SemanticType::Field
            || metadata.column_schema.data_type != ConcreteDataType::float64_datatype()
        {
            continue;
        }

        metadata.column_schema.set_nullable();

        let int_column_name = metric_engine_value_int_column_name(&metadata.column_schema.name);
        if existing_names.contains(&int_column_name) {
            continue;
        }

        int_columns.push(ColumnMetadata {
            column_id: next_column_id,
            semantic_type: SemanticType::Field,
            column_schema: ColumnSchema::new(
                int_column_name,
                ConcreteDataType::int64_datatype(),
                true,
            ),
        });
        next_column_id += 1;
    }

    column_metadatas.extend(int_columns);
}

fn table_id_col() -> ColumnMetadata {
    ColumnMetadata {
        column_id: ReservedColumnId::table_id(),
        semantic_type: SemanticType::Tag,
        column_schema: ColumnSchema::new(
            DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
            ConcreteDataType::uint32_datatype(),
            false,
        )
        .with_skipping_options(SkippingIndexOptions::new_unchecked(
            DEFAULT_TABLE_ID_SKIPPING_INDEX_GRANULARITY,
            DEFAULT_TABLE_ID_SKIPPING_INDEX_FALSE_POSITIVE_RATE,
            datatypes::schema::SkippingIndexType::BloomFilter,
        ))
        .unwrap(),
    }
}

fn tsid_col() -> ColumnMetadata {
    ColumnMetadata {
        column_id: ReservedColumnId::tsid(),
        semantic_type: SemanticType::Tag,
        column_schema: ColumnSchema::new(
            DATA_SCHEMA_TSID_COLUMN_NAME,
            ConcreteDataType::uint64_datatype(),
            false,
        )
        .with_inverted_index(false),
    }
}

/// Returns true if the column is the metric name column.
pub(crate) fn is_metric_name_col(column: &ColumnMetadata) -> bool {
    column.column_id == ReservedColumnId::table_id()
        && column.semantic_type == SemanticType::Tag
        && column.column_schema.data_type == ConcreteDataType::uint32_datatype()
        && column.column_schema.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
        && !column.column_schema.is_nullable()
}

/// Returns true if the column is the tsid column.
pub(crate) fn is_tsid_col(column: &ColumnMetadata) -> bool {
    column.column_id == ReservedColumnId::tsid()
        && column.semantic_type == SemanticType::Tag
        && column.column_schema.data_type == ConcreteDataType::uint64_datatype()
        && column.column_schema.name == DATA_SCHEMA_TSID_COLUMN_NAME
        && !column.column_schema.is_nullable()
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
    original: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut metadata_region_options = HashMap::new();
    metadata_region_options.insert(TTL_KEY.to_string(), FOREVER.to_string());

    if let Some(wal_options) = original.get(WAL_OPTIONS_KEY) {
        metadata_region_options.insert(WAL_OPTIONS_KEY.to_string(), wal_options.clone());
    }

    metadata_region_options
}

#[cfg(test)]
mod test {
    use common_meta::ddl::test_util::assert_column_name_and_id;
    use common_meta::ddl::utils::{parse_column_metadatas, parse_manifest_infos_from_extensions};
    use common_query::native_histogram::{NATIVE_HISTOGRAM_FIELD, native_histogram_value_type};
    use common_query::prelude::{greptime_timestamp, greptime_value};
    use store_api::metric_engine_consts::{METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY};
    use store_api::region_request::{BatchRegionDdlRequest, RegionRequirements};

    use super::*;
    use crate::config::EngineConfig;
    use crate::engine::MetricEngine;
    use crate::test_util::{TestEnv, create_logical_region_request};

    #[test]
    fn test_internal_column_metadata() {
        let table_id_col = table_id_col();
        let tsid_col = tsid_col();
        assert!(is_metric_name_col(&table_id_col));
        assert!(is_tsid_col(&tsid_col));
    }

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
            table_dir: "test_dir".to_string(),
            path_type: PathType::Bare,
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: HashMap::new(),
            partition_expr_json: Some("".to_string()),
            requirements: RegionRequirements::object_storage(),
        };
        let result = MetricEngineInner::verify_region_create_request(&request);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Internal column __table_id is reserved".to_string()
        );

        // allow reserved internal columns when defined properly
        let value_int_name = metric_engine_value_int_column_name("column2");
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
                ColumnMetadata {
                    column_id: 3,
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        &value_int_name,
                        ConcreteDataType::int64_datatype(),
                        false,
                    ),
                },
                table_id_col(),
                tsid_col(),
            ],
            table_dir: "test_dir".to_string(),
            path_type: PathType::Bare,
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
                .into_iter()
                .collect(),
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        MetricEngineInner::verify_region_create_request(&request).unwrap();
        append_metric_value_int_columns(&mut request.column_metadatas);
        assert!(request.column_metadatas[2].column_schema.is_nullable());
        assert!(request.column_metadatas[3].column_schema.is_nullable());

        request.options = [(LOGICAL_TABLE_METADATA_KEY.to_string(), String::new())]
            .into_iter()
            .collect();
        assert!(MetricEngineInner::verify_region_create_request(&request).is_err());

        request.options = [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
            .into_iter()
            .collect();
        request.column_metadatas[3].column_schema =
            ColumnSchema::new(&value_int_name, ConcreteDataType::string_datatype(), true);
        assert!(MetricEngineInner::verify_region_create_request(&request).is_err());

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
            table_dir: "test_dir".to_string(),
            path_type: PathType::Bare,
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
                .into_iter()
                .collect(),
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
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
            table_dir: "test_dir".to_string(),
            path_type: PathType::Bare,
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: HashMap::new(),
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
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

    #[test]
    fn test_verify_region_create_request_native_histogram_fields() {
        let native_histogram_columns = vec![
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
                semantic_type: SemanticType::Tag,
                column_schema: ColumnSchema::new("job", ConcreteDataType::string_datatype(), true),
            },
            ColumnMetadata {
                column_id: 2,
                semantic_type: SemanticType::Field,
                column_schema: ColumnSchema::new(
                    NATIVE_HISTOGRAM_FIELD,
                    native_histogram_value_type().clone(),
                    true,
                ),
            },
        ];
        let request = RegionCreateRequest {
            column_metadatas: native_histogram_columns,
            table_dir: "test_dir".to_string(),
            path_type: PathType::Bare,
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: [(
                LOGICAL_TABLE_METADATA_KEY.to_string(),
                "physical".to_string(),
            )]
            .into_iter()
            .collect(),
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        MetricEngineInner::verify_region_create_request(&request).unwrap();

        let request = RegionCreateRequest {
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
                        "value_a",
                        ConcreteDataType::float64_datatype(),
                        true,
                    ),
                },
                ColumnMetadata {
                    column_id: 2,
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        "value_b",
                        ConcreteDataType::float64_datatype(),
                        true,
                    ),
                },
            ],
            table_dir: "test_dir".to_string(),
            path_type: PathType::Bare,
            engine: METRIC_ENGINE_NAME.to_string(),
            primary_key: vec![],
            options: [(
                LOGICAL_TABLE_METADATA_KEY.to_string(),
                "physical".to_string(),
            )]
            .into_iter()
            .collect(),
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        assert!(MetricEngineInner::verify_region_create_request(&request).is_err());
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
            table_dir: "/test_dir".to_string(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: RegionRequirements::object_storage(),
        };

        // set up
        let env = TestEnv::new().await;
        let engine = MetricEngine::try_new(env.mito(), EngineConfig::default()).unwrap();
        let engine_inner = engine.inner;

        // check create data region request
        let data_region_request = engine_inner.create_request_for_data_region(&request);
        assert_eq!(data_region_request.table_dir, "/test_dir".to_string());
        assert_eq!(data_region_request.path_type, PathType::Data);
        assert_eq!(data_region_request.column_metadatas.len(), 4);
        assert_eq!(
            data_region_request.primary_key,
            vec![ReservedColumnId::table_id(), ReservedColumnId::tsid(), 1]
        );
        assert!(data_region_request.options.contains_key("ttl"));
        assert_eq!(
            data_region_request.requirements,
            RegionRequirements::object_storage()
        );

        // check create metadata region request
        let metadata_region_request = engine_inner.create_request_for_metadata_region(&request);
        assert_eq!(metadata_region_request.table_dir, "/test_dir".to_string());
        assert_eq!(metadata_region_request.path_type, PathType::Metadata);
        assert_eq!(
            metadata_region_request.options.get("ttl").unwrap(),
            "forever"
        );
        assert!(!metadata_region_request.options.contains_key("skip_wal"));
        assert_eq!(
            metadata_region_request.requirements,
            RegionRequirements::object_storage()
        );
    }

    #[tokio::test]
    async fn test_create_request_for_physical_regions_with_internal_columns() {
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
                ColumnMetadata {
                    column_id: 2,
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        "value",
                        ConcreteDataType::float64_datatype(),
                        false,
                    ),
                },
                table_id_col(),
                tsid_col(),
            ],
            primary_key: vec![0],
            options,
            table_dir: "/test_dir".to_string(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };

        let env = TestEnv::new().await;
        let engine = MetricEngine::try_new(env.mito(), EngineConfig::default()).unwrap();
        let engine_inner = engine.inner;

        let data_region_request = engine_inner.create_request_for_data_region(&request);
        assert_eq!(data_region_request.column_metadatas.len(), 6);
        assert_eq!(
            data_region_request.primary_key,
            vec![ReservedColumnId::table_id(), ReservedColumnId::tsid(), 1]
        );

        let table_id_count = data_region_request
            .column_metadatas
            .iter()
            .filter(|metadata| metadata.column_schema.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .count();
        let tsid_count = data_region_request
            .column_metadatas
            .iter()
            .filter(|metadata| metadata.column_schema.name == DATA_SCHEMA_TSID_COLUMN_NAME)
            .count();
        assert_eq!(table_id_count, 1);
        assert_eq!(tsid_count, 1);

        let tag_metadata = data_region_request
            .column_metadatas
            .iter()
            .find(|metadata| metadata.column_schema.name == "tag")
            .unwrap();
        assert!(tag_metadata.column_schema.is_nullable());

        let value_metadata = data_region_request
            .column_metadatas
            .iter()
            .find(|metadata| metadata.column_schema.name == "value")
            .unwrap();
        assert!(value_metadata.column_schema.is_nullable());

        let value_int_name = metric_engine_value_int_column_name("value");
        let value_int_metadata = data_region_request
            .column_metadatas
            .iter()
            .find(|metadata| metadata.column_schema.name == value_int_name)
            .unwrap();
        assert_eq!(value_int_metadata.column_id, 3);
        assert_eq!(
            value_int_metadata.column_schema.data_type,
            ConcreteDataType::int64_datatype()
        );
        assert!(value_int_metadata.column_schema.is_nullable());

        let table_id_metadata = data_region_request
            .column_metadatas
            .iter()
            .find(|metadata| metadata.column_schema.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .unwrap();
        assert!(is_metric_name_col(table_id_metadata));

        let tsid_metadata = data_region_request
            .column_metadatas
            .iter()
            .find(|metadata| metadata.column_schema.name == DATA_SCHEMA_TSID_COLUMN_NAME)
            .unwrap();
        assert!(is_tsid_col(tsid_metadata));
    }

    #[tokio::test]
    async fn test_create_logical_regions() {
        let env = TestEnv::new().await;
        let engine = env.metric();
        let physical_region_id1 = RegionId::new(1024, 0);
        let physical_region_id2 = RegionId::new(1024, 1);
        let logical_region_id1 = RegionId::new(1025, 0);
        let logical_region_id2 = RegionId::new(1025, 1);
        env.create_physical_region(physical_region_id1, "/test_dir1", vec![])
            .await;
        env.create_physical_region(physical_region_id2, "/test_dir2", vec![])
            .await;

        let region_create_request1 =
            create_logical_region_request(&["job"], physical_region_id1, "logical1");
        let region_create_request2 =
            create_logical_region_request(&["job"], physical_region_id2, "logical2");

        let response = engine
            .handle_batch_ddl_requests(BatchRegionDdlRequest::Create(vec![
                (logical_region_id1, region_create_request1),
                (logical_region_id2, region_create_request2),
            ]))
            .await
            .unwrap();

        let manifest_infos = parse_manifest_infos_from_extensions(&response.extensions).unwrap();
        assert_eq!(manifest_infos.len(), 2);
        let region_ids = manifest_infos.into_iter().map(|i| i.0).collect::<Vec<_>>();
        assert!(region_ids.contains(&physical_region_id1));
        assert!(region_ids.contains(&physical_region_id2));

        let column_metadatas =
            parse_column_metadatas(&response.extensions, ALTER_PHYSICAL_EXTENSION_KEY).unwrap();
        let value_int_name = metric_engine_value_int_column_name(greptime_value());
        assert_column_name_and_id(
            &column_metadatas,
            &[
                (greptime_timestamp(), 0),
                (greptime_value(), 1),
                (value_int_name.as_str(), 2),
                ("__table_id", ReservedColumnId::table_id()),
                ("__tsid", ReservedColumnId::tsid()),
                ("job", 3),
            ],
        );
    }
}
