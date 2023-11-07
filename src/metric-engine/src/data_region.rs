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
use store_api::region_request::{AddColumn, AlterKind, RegionAlterRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::error::{MitoReadOperationSnafu, MitoWriteOperationSnafu, Result};
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

        // overwrite semantic type
        let columns = columns
            .into_iter()
            .map(|mut c| {
                if c.semantic_type == SemanticType::Tag {
                    c.semantic_type = SemanticType::Field;
                } else {
                    warn!(
                        "Column {} in region {region_id} is not a tag",
                        c.column_schema.name
                    );
                };

                AddColumn {
                    column_metadata: c,
                    location: None,
                }
            })
            .collect();

        // assemble alter request
        let alter_request = RegionRequest::Alter(RegionAlterRequest {
            schema_version: version,
            kind: AlterKind::AddColumns { columns },
        });

        // submit alter request
        let timer = MITO_DDL_DURATION.start_timer();
        self.mito
            .handle_request(region_id, alter_request)
            .await
            .context(MitoWriteOperationSnafu)?;
        timer.stop_and_record();

        Ok(())
    }
}
