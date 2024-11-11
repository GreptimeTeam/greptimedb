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

use common_telemetry::{error, info};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
use store_api::region_request::{AffectedRows, AlterKind, RegionAlterRequest};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{
    ForbiddenPhysicalAlterSnafu, LogicalRegionNotFoundSnafu, Result, SerializeColumnMetadataSnafu,
};
use crate::metrics::FORBIDDEN_OPERATION_COUNT;
use crate::utils::{to_data_region_id, to_metadata_region_id};

impl MetricEngineInner {
    /// Dispatch region alter request
    pub async fn alter_region(
        &self,
        region_id: RegionId,
        request: RegionAlterRequest,
        extension_return_value: &mut HashMap<String, Vec<u8>>,
    ) -> Result<AffectedRows> {
        let is_altering_physical_region = self.is_physical_region(region_id);

        let result = if is_altering_physical_region {
            self.alter_physical_region(region_id, request).await
        } else {
            let physical_region_id = self.alter_logical_region(region_id, request).await?;

            // Add physical table's column to extension map.
            // It's ok to overwrite existing key, as the latter come schema is more up-to-date
            let physical_columns = self
                .data_region
                .physical_columns(physical_region_id)
                .await?;
            extension_return_value.insert(
                ALTER_PHYSICAL_EXTENSION_KEY.to_string(),
                ColumnMetadata::encode_list(&physical_columns)
                    .context(SerializeColumnMetadataSnafu)?,
            );

            Ok(())
        };

        result.map(|_| 0)
    }

    /// Return the physical region id behind this logical region
    async fn alter_logical_region(
        &self,
        logical_region_id: RegionId,
        request: RegionAlterRequest,
    ) -> Result<RegionId> {
        let physical_region_id = {
            let state = &self.state.read().unwrap();
            state
                .get_physical_region_id(logical_region_id)
                .with_context(|| {
                    error!("Trying to alter an nonexistent region {logical_region_id}");
                    LogicalRegionNotFoundSnafu {
                        region_id: logical_region_id,
                    }
                })?
        };

        // only handle adding column
        let AlterKind::AddColumns { columns } = request.kind else {
            return Ok(physical_region_id);
        };

        // lock metadata region for this logical region id
        let _write_guard = self
            .metadata_region
            .write_lock_logical_region(logical_region_id)
            .await;

        let metadata_region_id = to_metadata_region_id(physical_region_id);
        let mut columns_to_add = vec![];
        // columns that already exist in physical region
        let mut existing_columns = vec![];

        let pre_existing_physical_columns = self
            .data_region
            .physical_columns(physical_region_id)
            .await?;

        let pre_exist_names = pre_existing_physical_columns
            .iter()
            .map(|col| col.column_schema.name.clone())
            .collect::<HashSet<_>>();

        // check pre-existing physical columns so if any columns to add is already exist,
        // we can skip it in physical alter operation
        // (but still need to update them in logical alter operation)
        for col in &columns {
            if !pre_exist_names.contains(&col.column_metadata.column_schema.name) {
                columns_to_add.push(col.column_metadata.clone());
            } else {
                existing_columns.push(col.column_metadata.clone());
            }
        }

        // alter data region
        let data_region_id = to_data_region_id(physical_region_id);
        self.add_columns_to_physical_data_region(
            data_region_id,
            logical_region_id,
            &mut columns_to_add,
        )
        .await?;

        // check physical region id again, since it may have been altered and column ids might be different than
        // the one we have
        let after_alter_phy_table = self
            .data_region
            .physical_columns(physical_region_id)
            .await?;
        let after_alter_cols = after_alter_phy_table
            .iter()
            .map(|col| (col.column_schema.name.clone(), col.clone()))
            .collect::<HashMap<_, _>>();

        // register columns to logical region
        // we need to use modified column metadata from physical region, since it may have been altered(especially column id)
        for col in columns {
            if let Some(metadata) = after_alter_cols.get(&col.column_metadata.column_schema.name) {
                self.metadata_region
                    .add_column(metadata_region_id, logical_region_id, metadata)
                    .await?;
            } else {
                // TODO(discord9): consider make this a hard error?
                error!(
                    "Column {} is not found in physical region",
                    col.column_metadata.column_schema.name
                );
            }
        }

        // invalid logical column cache
        self.state
            .write()
            .unwrap()
            .invalid_logical_column_cache(logical_region_id);

        Ok(physical_region_id)
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

#[cfg(test)]
mod test {
    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::AddColumn;

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
}
