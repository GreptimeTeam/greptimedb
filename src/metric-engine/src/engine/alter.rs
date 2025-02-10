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
mod validate;

use std::collections::{HashMap, HashSet};

use common_telemetry::error;
use extract_new_columns::extract_new_columns;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
use store_api::region_request::{AffectedRows, AlterKind, RegionAlterRequest};
use store_api::storage::RegionId;
use validate::validate_alter_region_requests;

use crate::engine::create::add_columns_to_physical_data_region;
use crate::engine::MetricEngineInner;
use crate::error::{
    LogicalRegionNotFoundSnafu, PhysicalRegionNotFoundSnafu, Result, SerializeColumnMetadataSnafu,
    UnexpectedRequestSnafu,
};
use crate::utils::to_data_region_id;

impl MetricEngineInner {
    pub async fn alter_regions(
        &self,
        mut requests: Vec<(RegionId, RegionAlterRequest)>,
        extension_return_value: &mut HashMap<String, Vec<u8>>,
    ) -> Result<AffectedRows> {
        if requests.is_empty() {
            return Ok(0);
        }

        let first_region_id = &requests.first().unwrap().0;
        if self.is_physical_region(*first_region_id) {
            ensure!(
                requests.len() == 1,
                UnexpectedRequestSnafu {
                    reason: "Physical table must be altered with single request".to_string(),
                }
            );
            let (region_id, request) = requests.pop().unwrap();
            self.alter_physical_region(region_id, request).await?;
        } else {
            self.alter_logical_regions(requests, extension_return_value)
                .await?;
        }
        Ok(0)
    }

    /// Alter multiple logical regions on the same physical region.
    pub async fn alter_logical_regions(
        &self,
        requests: Vec<(RegionId, RegionAlterRequest)>,
        extension_return_value: &mut HashMap<String, Vec<u8>>,
    ) -> Result<AffectedRows> {
        validate_alter_region_requests(&requests)?;

        let first_logical_region_id = requests[0].0;

        // Finds new columns to add
        let mut new_column_names = HashSet::new();
        let mut new_columns_to_add = vec![];

        let (physical_region_id, index_options) = {
            let state = &self.state.read().unwrap();
            let physical_region_id = state
                .get_physical_region_id(first_logical_region_id)
                .with_context(|| {
                    error!("Trying to alter an nonexistent region {first_logical_region_id}");
                    LogicalRegionNotFoundSnafu {
                        region_id: first_logical_region_id,
                    }
                })?;
            let region_state = state
                .physical_region_states()
                .get(&physical_region_id)
                .with_context(|| PhysicalRegionNotFoundSnafu {
                    region_id: physical_region_id,
                })?;
            let physical_columns = region_state.physical_columns();

            extract_new_columns(
                &requests,
                physical_columns,
                &mut new_column_names,
                &mut new_columns_to_add,
            )?;

            (physical_region_id, region_state.options().index)
        };
        let data_region_id = to_data_region_id(physical_region_id);

        let mut write_guards = HashMap::with_capacity(requests.len());
        for (region_id, _) in requests.iter() {
            if write_guards.contains_key(region_id) {
                continue;
            }
            let _write_guard = self
                .metadata_region
                .write_lock_logical_region(*region_id)
                .await;
            write_guards.insert(*region_id, _write_guard);
        }

        add_columns_to_physical_data_region(
            data_region_id,
            index_options,
            &mut new_columns_to_add,
            &self.data_region,
        )
        .await?;

        let physical_columns = self.data_region.physical_columns(data_region_id).await?;
        let physical_schema_map = physical_columns
            .iter()
            .map(|metadata| (metadata.column_schema.name.as_str(), metadata))
            .collect::<HashMap<_, _>>();

        let logical_region_columns = requests.iter().map(|(region_id, request)| {
            let AlterKind::AddColumns { columns } = &request.kind else {
                unreachable!()
            };
            (
                *region_id,
                columns
                    .iter()
                    .map(|col| {
                        let column_name = col.column_metadata.column_schema.name.as_str();
                        let column_metadata = *physical_schema_map.get(column_name).unwrap();
                        (column_name, column_metadata)
                    })
                    .collect::<HashMap<_, _>>(),
            )
        });

        let new_add_columns = new_columns_to_add.iter().map(|metadata| {
            // Safety: previous steps ensure the physical region exist
            let column_metadata = *physical_schema_map
                .get(metadata.column_schema.name.as_str())
                .unwrap();
            (
                metadata.column_schema.name.to_string(),
                column_metadata.column_id,
            )
        });

        // Writes logical regions metadata to metadata region
        self.metadata_region
            .add_logical_regions(physical_region_id, false, logical_region_columns)
            .await?;

        extension_return_value.insert(
            ALTER_PHYSICAL_EXTENSION_KEY.to_string(),
            ColumnMetadata::encode_list(&physical_columns).context(SerializeColumnMetadataSnafu)?,
        );

        let mut state = self.state.write().unwrap();
        state.add_physical_columns(data_region_id, new_add_columns);
        state.invalid_logical_regions_cache(requests.iter().map(|(region_id, _)| *region_id));

        Ok(0)
    }

    async fn alter_physical_region(
        &self,
        region_id: RegionId,
        request: RegionAlterRequest,
    ) -> Result<()> {
        self.data_region
            .alter_region_options(region_id, request)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::{AddColumn, SetRegionOption};

    use super::*;
    use crate::test_util::TestEnv;

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

        // alter physical region's option should work
        let alter_region_option_request = RegionAlterRequest {
            schema_version: 0,
            kind: AlterKind::SetRegionOptions {
                options: vec![SetRegionOption::Ttl(Some(Duration::from_secs(500).into()))],
            },
        };
        let result = engine_inner
            .alter_physical_region(physical_region_id, alter_region_option_request.clone())
            .await;
        assert!(result.is_ok());

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
            .alter_logical_regions(vec![(region_id, request)], &mut HashMap::new())
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
}
