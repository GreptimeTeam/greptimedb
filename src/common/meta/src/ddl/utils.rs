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

use std::collections::HashMap;
use std::fmt::Debug;

use api::region::RegionResponse;
use api::v1::region::sync_request::ManifestInfo;
use api::v1::region::{
    region_request, MetricManifestInfo, MitoManifestInfo, RegionRequest, RegionRequestHeader,
    SyncRequest,
};
use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE};
use common_error::ext::BoxedError;
use common_procedure::error::Error as ProcedureError;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info, warn};
use common_wal::options::WalOptions;
use futures::future::join_all;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, MANIFEST_INFO_EXTENSION_KEY};
use store_api::region_engine::RegionManifestInfo;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;
use table::table_reference::TableReference;

use crate::ddl::{DdlContext, DetectingRegion};
use crate::error::{
    self, Error, OperateDatanodeSnafu, ParseWalOptionsSnafu, Result, TableNotFoundSnafu,
    UnsupportedSnafu,
};
use crate::key::datanode_table::DatanodeTableValue;
use crate::key::table_name::TableNameKey;
use crate::key::TableMetadataManagerRef;
use crate::peer::Peer;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::{find_follower_regions, find_followers, RegionRoute};

/// Adds [Peer] context if the error is unretryable.
pub fn add_peer_context_if_needed(datanode: Peer) -> impl FnOnce(Error) -> Error {
    move |err| {
        error!(err; "Failed to operate datanode, peer: {}", datanode);
        if !err.is_retry_later() {
            return Err::<(), BoxedError>(BoxedError::new(err))
                .context(OperateDatanodeSnafu { peer: datanode })
                .unwrap_err();
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
        .with_context(|| TableNotFoundSnafu {
            table_name: TableReference::from(physical_table_name).to_string(),
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
        .with_context(|| TableNotFoundSnafu {
            table_name: TableReference::from(logical_table_name).to_string(),
        })
        .map(|table| table.table_id())?;

    table_metadata_manager
        .table_route_manager()
        .get_physical_table_id(logical_table_id)
        .await
}

/// Converts a list of [`RegionRoute`] to a list of [`DetectingRegion`].
pub fn convert_region_routes_to_detecting_regions(
    region_routes: &[RegionRoute],
) -> Vec<DetectingRegion> {
    region_routes
        .iter()
        .flat_map(|route| {
            route
                .leader_peer
                .as_ref()
                .map(|peer| (peer.id, route.region.id))
        })
        .collect::<Vec<_>>()
}

/// Parses [WalOptions] from serialized strings in hashmap.
pub fn parse_region_wal_options(
    serialized_options: &HashMap<RegionNumber, String>,
) -> Result<HashMap<RegionNumber, WalOptions>> {
    let mut region_wal_options = HashMap::with_capacity(serialized_options.len());
    for (region_number, wal_options) in serialized_options {
        let wal_option = serde_json::from_str::<WalOptions>(wal_options)
            .context(ParseWalOptionsSnafu { wal_options })?;
        region_wal_options.insert(*region_number, wal_option);
    }
    Ok(region_wal_options)
}

/// Extracts region wal options from [DatanodeTableValue]s.
pub fn extract_region_wal_options(
    datanode_table_values: &Vec<DatanodeTableValue>,
) -> Result<HashMap<RegionNumber, WalOptions>> {
    let mut region_wal_options = HashMap::new();
    for value in datanode_table_values {
        let serialized_options = &value.region_info.region_wal_options;
        let parsed_options = parse_region_wal_options(serialized_options)?;
        region_wal_options.extend(parsed_options);
    }
    Ok(region_wal_options)
}

/// The result of multiple operations.
///
/// - Ok: all operations are successful.
/// - PartialRetryable: if any operation is retryable and without non retryable error, the result is retryable.
/// - PartialNonRetryable: if any operation is non retryable, the result is non retryable.
/// - AllRetryable: all operations are retryable.
/// - AllNonRetryable: all operations are not retryable.
pub enum MultipleResults<T> {
    Ok(Vec<T>),
    PartialRetryable(Error),
    PartialNonRetryable(Error),
    AllRetryable(Error),
    AllNonRetryable(Error),
}

/// Handles the results of alter region requests.
///
/// For partial success, we need to check if the errors are retryable.
/// If all the errors are retryable, we return a retryable error.
/// Otherwise, we return the first error.
pub fn handle_multiple_results<T: Debug>(results: Vec<Result<T>>) -> MultipleResults<T> {
    if results.is_empty() {
        return MultipleResults::Ok(Vec::new());
    }
    let num_results = results.len();
    let mut retryable_results = Vec::new();
    let mut non_retryable_results = Vec::new();
    let mut ok_results = Vec::new();

    for result in results {
        match result {
            Ok(value) => ok_results.push(value),
            Err(err) => {
                if err.is_retry_later() {
                    retryable_results.push(err);
                } else {
                    non_retryable_results.push(err);
                }
            }
        }
    }

    common_telemetry::debug!(
        "retryable_results: {}, non_retryable_results: {}, ok_results: {}",
        retryable_results.len(),
        non_retryable_results.len(),
        ok_results.len()
    );

    if retryable_results.len() == num_results {
        return MultipleResults::AllRetryable(retryable_results.into_iter().next().unwrap());
    } else if non_retryable_results.len() == num_results {
        warn!("all non retryable results: {}", non_retryable_results.len());
        for err in &non_retryable_results {
            error!(err; "non retryable error");
        }
        return MultipleResults::AllNonRetryable(non_retryable_results.into_iter().next().unwrap());
    } else if ok_results.len() == num_results {
        return MultipleResults::Ok(ok_results);
    } else if !retryable_results.is_empty()
        && !ok_results.is_empty()
        && non_retryable_results.is_empty()
    {
        return MultipleResults::PartialRetryable(retryable_results.into_iter().next().unwrap());
    }

    warn!(
        "partial non retryable results: {}, retryable results: {}, ok results: {}",
        non_retryable_results.len(),
        retryable_results.len(),
        ok_results.len()
    );
    for err in &non_retryable_results {
        error!(err; "non retryable error");
    }
    // non_retryable_results.len() > 0
    MultipleResults::PartialNonRetryable(non_retryable_results.into_iter().next().unwrap())
}

/// Parses manifest infos from extensions.
pub fn parse_manifest_infos_from_extensions(
    extensions: &HashMap<String, Vec<u8>>,
) -> Result<Vec<(RegionId, RegionManifestInfo)>> {
    let data_manifest_version =
        extensions
            .get(MANIFEST_INFO_EXTENSION_KEY)
            .context(error::UnexpectedSnafu {
                err_msg: "manifest info extension not found",
            })?;
    let data_manifest_version =
        RegionManifestInfo::decode_list(data_manifest_version).context(error::SerdeJsonSnafu {})?;
    Ok(data_manifest_version)
}

/// Sync follower regions on datanodes.
pub async fn sync_follower_regions(
    context: &DdlContext,
    table_id: TableId,
    results: Vec<RegionResponse>,
    region_routes: &[RegionRoute],
    engine: &str,
) -> Result<()> {
    if engine != MITO_ENGINE && engine != METRIC_ENGINE {
        info!(
            "Skip submitting sync region requests for table_id: {}, engine: {}",
            table_id, engine
        );
        return Ok(());
    }

    let results = results
        .into_iter()
        .map(|response| parse_manifest_infos_from_extensions(&response.extensions))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<HashMap<_, _>>();

    let is_mito_engine = engine == MITO_ENGINE;

    let followers = find_followers(region_routes);
    if followers.is_empty() {
        return Ok(());
    }
    let mut sync_region_tasks = Vec::with_capacity(followers.len());
    for datanode in followers {
        let requester = context.node_manager.datanode(&datanode).await;
        let regions = find_follower_regions(region_routes, &datanode);
        for region in regions {
            let region_id = RegionId::new(table_id, region);
            let manifest_info = if is_mito_engine {
                let region_manifest_info =
                    results.get(&region_id).context(error::UnexpectedSnafu {
                        err_msg: format!("No manifest info found for region {}", region_id),
                    })?;
                ensure!(
                    region_manifest_info.is_mito(),
                    error::UnexpectedSnafu {
                        err_msg: format!("Region {} is not a mito region", region_id)
                    }
                );
                ManifestInfo::MitoManifestInfo(MitoManifestInfo {
                    data_manifest_version: region_manifest_info.data_manifest_version(),
                })
            } else {
                let region_manifest_info =
                    results.get(&region_id).context(error::UnexpectedSnafu {
                        err_msg: format!("No manifest info found for region {}", region_id),
                    })?;
                ensure!(
                    region_manifest_info.is_metric(),
                    error::UnexpectedSnafu {
                        err_msg: format!("Region {} is not a metric region", region_id)
                    }
                );
                ManifestInfo::MetricManifestInfo(MetricManifestInfo {
                    data_manifest_version: region_manifest_info.data_manifest_version(),
                    metadata_manifest_version: region_manifest_info
                        .metadata_manifest_version()
                        .unwrap_or_default(),
                })
            };
            let request = RegionRequest {
                header: Some(RegionRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                body: Some(region_request::Body::Sync(SyncRequest {
                    region_id: region_id.as_u64(),
                    manifest_info: Some(manifest_info),
                })),
            };

            let datanode = datanode.clone();
            let requester = requester.clone();
            sync_region_tasks.push(async move {
                requester
                    .handle(request)
                    .await
                    .map_err(add_peer_context_if_needed(datanode))
            });
        }
    }

    // Failure to sync region is not critical.
    // We try our best to sync the regions.
    if let Err(err) = join_all(sync_region_tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
    {
        error!(err; "Failed to sync follower regions on datanodes, table_id: {}", table_id);
    }
    info!("Sync follower regions on datanodes, table_id: {}", table_id);

    Ok(())
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
