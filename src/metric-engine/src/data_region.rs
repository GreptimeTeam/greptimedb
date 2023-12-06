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

use api::v1::SemanticType;
use common_telemetry::tracing::warn;
use mito2::engine::MitoEngine;
use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AddColumn, AffectedRows, AlterKind, RegionAlterRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::RegionId;

use crate::error::{
    ColumnTypeMismatchSnafu, MitoReadOperationSnafu, MitoWriteOperationSnafu, Result,
};
use crate::metrics::MITO_DDL_DURATION;
use crate::utils;

/// This is a generic handler like [MetricEngine](crate::engine::MetricEngine). It
/// will handle all the data related operations across physical tables. Thus
/// every operation should be associated to a [RegionId], which is the physical
/// table id + region sequence. This handler will transform the region group by
/// itself.
pub struct DataRegion {
    mito: MitoEngine,
}

impl DataRegion {
    pub fn new(mito: MitoEngine) -> Self {
        Self { mito }
    }

    /// Submit an alter request to underlying physical region.
    ///
    /// This method will change the semantic type of those given columns.
    /// [SemanticType::Tag] will become [SemanticType::Field]. The procedure framework
    /// ensures there is no concurrent conflict.
    ///
    /// Invoker don't need to set up or verify the column id. This method will adjust
    /// it using underlying schema.
    ///
    /// This method will also set the nullable marker to true.
    pub async fn add_columns(
        &self,
        region_id: RegionId,
        columns: Vec<ColumnMetadata>,
    ) -> Result<()> {
        let region_id = utils::to_data_region_id(region_id);

        // retrieve underlying version
        let region_metadata = self
            .mito
            .get_metadata(region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        let version = region_metadata.schema_version;

        // find the max column id
        let new_column_id_start = 1 + region_metadata
            .column_metadatas
            .iter()
            .filter_map(|c| {
                if ReservedColumnId::is_reserved(c.column_id) {
                    None
                } else {
                    Some(c.column_id)
                }
            })
            .max()
            .unwrap_or(0);

        // overwrite semantic type
        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(delta, mut c)| {
                if c.semantic_type == SemanticType::Tag {
                    c.semantic_type = SemanticType::Field;
                    if !c.column_schema.data_type.is_string() {
                        return ColumnTypeMismatchSnafu {
                            column_type: c.column_schema.data_type,
                        }
                        .fail();
                    }
                } else {
                    warn!(
                        "Column {} in region {region_id} is not a tag",
                        c.column_schema.name
                    );
                };

                c.column_id = new_column_id_start + delta as u32;

                c.column_schema = c.column_schema.with_nullable_set();

                Ok(AddColumn {
                    column_metadata: c,
                    location: None,
                })
            })
            .collect::<Result<_>>()?;

        // assemble alter request
        let alter_request = RegionRequest::Alter(RegionAlterRequest {
            schema_version: version,
            kind: AlterKind::AddColumns { columns },
        });

        // submit alter request
        {
            let _timer = MITO_DDL_DURATION.start_timer();
            self.mito
                .handle_request(region_id, alter_request)
                .await
                .context(MitoWriteOperationSnafu)?;
        }

        Ok(())
    }

    pub async fn write_data(
        &self,
        region_id: RegionId,
        request: RegionPutRequest,
    ) -> Result<AffectedRows> {
        let region_id = utils::to_data_region_id(region_id);
        self.mito
            .handle_request(region_id, RegionRequest::Put(request))
            .await
            .context(MitoWriteOperationSnafu)
    }

    pub async fn physical_columns(
        &self,
        physical_region_id: RegionId,
    ) -> Result<Vec<ColumnMetadata>> {
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        Ok(metadata.column_metadatas.clone())
    }
}

#[cfg(test)]
mod test {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;

    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_add_columns() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let current_version = env
            .mito()
            .get_metadata(utils::to_data_region_id(env.default_physical_region_id()))
            .await
            .unwrap()
            .schema_version;
        // TestEnv will create a logical region which changes the version to 1.
        assert_eq!(current_version, 1);

        let new_columns = vec![
            ColumnMetadata {
                column_id: 0,
                semantic_type: SemanticType::Tag,
                column_schema: ColumnSchema::new(
                    "tag2",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
            },
            ColumnMetadata {
                column_id: 0,
                semantic_type: SemanticType::Tag,
                column_schema: ColumnSchema::new(
                    "tag3",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
            },
        ];
        env.data_region()
            .add_columns(env.default_physical_region_id(), new_columns)
            .await
            .unwrap();

        let new_metadata = env
            .mito()
            .get_metadata(utils::to_data_region_id(env.default_physical_region_id()))
            .await
            .unwrap();
        let column_names = new_metadata
            .column_metadatas
            .iter()
            .map(|c| &c.column_schema.name)
            .collect::<Vec<_>>();
        let expected = vec![
            "greptime_timestamp",
            "greptime_value",
            "__table_id",
            "__tsid",
            "job",
            "tag2",
            "tag3",
        ];
        assert_eq!(column_names, expected);
    }

    // Only string is allowed for tag column
    #[tokio::test]
    async fn test_add_invalid_column() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let new_columns = vec![ColumnMetadata {
            column_id: 0,
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new("tag2", ConcreteDataType::int64_datatype(), false),
        }];
        let result = env
            .data_region()
            .add_columns(env.default_physical_region_id(), new_columns)
            .await;
        assert!(result.is_err());
    }
}
