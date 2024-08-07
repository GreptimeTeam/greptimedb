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

use common_catalog::consts::METRIC_ENGINE;
use common_error::ext::BoxedError;
use common_procedure::error::Error as ProcedureError;
use snafu::{ensure, location, OptionExt};
use store_api::metric_engine_consts::LOGICAL_TABLE_METADATA_KEY;
use table::metadata::TableId;

use crate::ddl::DetectingRegion;
use crate::error::{Error, Result, TableNotFoundSnafu, UnsupportedSnafu};
use crate::key::table_name::TableNameKey;
use crate::key::TableMetadataManagerRef;
use crate::peer::Peer;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::RegionRoute;
use crate::ClusterId;

/// Adds [Peer] context if the error is unretryable.
pub fn add_peer_context_if_needed(datanode: Peer) -> impl FnOnce(Error) -> Error {
    move |err| {
        if !err.is_retry_later() {
            return Error::OperateDatanode {
                location: location!(),
                peer: datanode,
                source: BoxedError::new(err),
            };
        }
        err
    }
}

pub fn handle_retry_error(e: Error) -> ProcedureError {
    if e.is_retry_later() {
        ProcedureError::retry_later(e)
    } else {
        ProcedureError::external(e)
    }
}

#[inline]
pub fn region_storage_path(catalog: &str, schema: &str) -> String {
    format!("{}/{}", catalog, schema)
}

/// Extracts catalog and schema from the path that created by [region_storage_path].
pub fn get_catalog_and_schema(path: &str) -> Option<(String, String)> {
    let mut split = path.split('/');
    Some((split.next()?.to_string(), split.next()?.to_string()))
}

pub async fn check_and_get_physical_table_id(
    table_metadata_manager: &TableMetadataManagerRef,
    tasks: &[CreateTableTask],
) -> Result<TableId> {
    let mut physical_table_name = None;
    for task in tasks {
        ensure!(
            task.create_table.engine == METRIC_ENGINE,
            UnsupportedSnafu {
                operation: format!("create table with engine {}", task.create_table.engine)
            }
        );
        let current_physical_table_name = task
            .create_table
            .table_options
            .get(LOGICAL_TABLE_METADATA_KEY)
            .context(UnsupportedSnafu {
                operation: format!(
                    "create table without table options {}",
                    LOGICAL_TABLE_METADATA_KEY,
                ),
            })?;
        let current_physical_table_name = TableNameKey::new(
            &task.create_table.catalog_name,
            &task.create_table.schema_name,
            current_physical_table_name,
        );

        physical_table_name = match physical_table_name {
            Some(name) => {
                ensure!(
                    name == current_physical_table_name,
                    UnsupportedSnafu {
                        operation: format!(
                            "create table with different physical table name {} and {}",
                            name, current_physical_table_name
                        )
                    }
                );
                Some(name)
            }
            None => Some(current_physical_table_name),
        };
    }
    // Safety: `physical_table_name` is `Some` here
    let physical_table_name = physical_table_name.unwrap();
    table_metadata_manager
        .table_name_manager()
        .get(physical_table_name)
        .await?
        .context(TableNotFoundSnafu {
            table_name: physical_table_name.to_string(),
        })
        .map(|table| table.table_id())
}

pub async fn get_physical_table_id(
    table_metadata_manager: &TableMetadataManagerRef,
    logical_table_name: TableNameKey<'_>,
) -> Result<TableId> {
    let logical_table_id = table_metadata_manager
        .table_name_manager()
        .get(logical_table_name)
        .await?
        .context(TableNotFoundSnafu {
            table_name: logical_table_name.to_string(),
        })
        .map(|table| table.table_id())?;

    table_metadata_manager
        .table_route_manager()
        .get_physical_table_id(logical_table_id)
        .await
}

/// Converts a list of [`RegionRoute`] to a list of [`DetectingRegion`].
pub fn convert_region_routes_to_detecting_regions(
    cluster_id: ClusterId,
    region_routes: &[RegionRoute],
) -> Vec<DetectingRegion> {
    region_routes
        .iter()
        .flat_map(|route| {
            route
                .leader_peer
                .as_ref()
                .map(|peer| (cluster_id, peer.id, route.region.id))
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_catalog_and_schema() {
        let test_catalog = "my_catalog";
        let test_schema = "my_schema";
        let path = region_storage_path(test_catalog, test_schema);
        let (catalog, schema) = get_catalog_and_schema(&path).unwrap();
        assert_eq!(catalog, test_catalog);
        assert_eq!(schema, test_schema);
    }
}
