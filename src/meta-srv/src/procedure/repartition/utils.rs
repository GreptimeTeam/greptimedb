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

use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue};
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;

use crate::error::{self, Result};

/// Returns the `datanode_table_value`
///
/// Retry:
/// - Failed to retrieve the metadata of datanode table.
pub async fn get_datanode_table_value(
    table_metadata_manager: &TableMetadataManagerRef,
    table_id: TableId,
    datanode_id: u64,
) -> Result<DatanodeTableValue> {
    let datanode_table_value = table_metadata_manager
        .datanode_table_manager()
        .get(&DatanodeTableKey {
            datanode_id,
            table_id,
        })
        .await
        .context(error::TableMetadataManagerSnafu)
        .map_err(BoxedError::new)
        .with_context(|_| error::RetryLaterWithSourceSnafu {
            reason: format!("Failed to get DatanodeTable: {table_id}"),
        })?
        .context(error::DatanodeTableNotFoundSnafu {
            table_id,
            datanode_id,
        })?;
    Ok(datanode_table_value)
}
