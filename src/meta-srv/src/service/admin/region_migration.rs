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
use std::num::ParseIntError;
use std::str::FromStr;

use common_meta::peer::Peer;
use common_meta::{distributed_time_constants, ClusterId};
use serde::Serialize;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use tonic::codegen::http;

use super::HttpHandler;
use crate::cluster::MetaPeerClientRef;
use crate::error::{self, Error, Result};
use crate::lease::lookup_alive_datanode_peer;
use crate::procedure::region_migration::manager::{
    RegionMigrationManagerRef, RegionMigrationProcedureTask,
};

/// The handler of submitting migration task.
pub struct SubmitRegionMigrationTaskHandler {
    pub region_migration_manager: RegionMigrationManagerRef,
    pub meta_peer_client: MetaPeerClientRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubmitRegionMigrationTaskRequest {
    cluster_id: ClusterId,
    region_id: RegionId,
    from_peer_id: u64,
    to_peer_id: u64,
}

#[derive(Debug, Serialize)]
struct SubmitRegionMigrationTaskResponse {
    /// The `None` stands region has been migrated.
    procedure_id: Option<String>,
}

fn parse_num_parameter_with_default<T, F>(
    key: &str,
    params: &HashMap<String, String>,
    default_fn: F,
) -> Result<T>
where
    F: Fn(&str) -> Result<T>,
    T: FromStr<Err = ParseIntError>,
{
    let parse_result = if let Some(id) = params.get(key) {
        id.parse::<T>().context(error::ParseNumSnafu {
            err_msg: format!("invalid {key}: {id}"),
        })?
    } else {
        default_fn(key)?
    };

    Ok(parse_result)
}

impl TryFrom<&HashMap<String, String>> for SubmitRegionMigrationTaskRequest {
    type Error = Error;

    fn try_from(params: &HashMap<String, String>) -> Result<Self> {
        let cluster_id = parse_num_parameter_with_default("cluster_id", params, |_| Ok(0))?;

        let region_id: u64 = parse_num_parameter_with_default("region_id", params, |key| {
            error::MissingRequiredParameterSnafu { param: key }.fail()
        })?;

        let from_peer_id: u64 = parse_num_parameter_with_default("from_peer_id", params, |key| {
            error::MissingRequiredParameterSnafu { param: key }.fail()
        })?;

        let to_peer_id: u64 = parse_num_parameter_with_default("to_peer_id", params, |key| {
            error::MissingRequiredParameterSnafu { param: key }.fail()
        })?;

        Ok(SubmitRegionMigrationTaskRequest {
            cluster_id,
            region_id: RegionId::from_u64(region_id),
            from_peer_id,
            to_peer_id,
        })
    }
}

impl SubmitRegionMigrationTaskHandler {
    fn is_leader(&self) -> bool {
        self.meta_peer_client.is_leader()
    }

    /// Checks the peer is available.
    async fn lookup_peer(&self, cluster_id: ClusterId, peer_id: u64) -> Result<Option<Peer>> {
        lookup_alive_datanode_peer(
            cluster_id,
            peer_id,
            &self.meta_peer_client,
            distributed_time_constants::DATANODE_LEASE_SECS,
        )
        .await
    }

    /// Submits a region migration task, returns the procedure id.
    async fn handle_submit(
        &self,
        task: SubmitRegionMigrationTaskRequest,
    ) -> Result<SubmitRegionMigrationTaskResponse> {
        ensure!(
            self.is_leader(),
            error::UnexpectedSnafu {
                violated: "Trying to submit a region migration procedure to non-leader meta server"
            }
        );

        let SubmitRegionMigrationTaskRequest {
            cluster_id,
            region_id,
            from_peer_id,
            to_peer_id,
        } = task;

        let from_peer = self.lookup_peer(cluster_id, from_peer_id).await?.context(
            error::PeerUnavailableSnafu {
                peer_id: from_peer_id,
            },
        )?;
        let to_peer = self.lookup_peer(cluster_id, to_peer_id).await?.context(
            error::PeerUnavailableSnafu {
                peer_id: to_peer_id,
            },
        )?;
        let procedure_id = self
            .region_migration_manager
            .submit_procedure(RegionMigrationProcedureTask {
                cluster_id,
                region_id,
                from_peer,
                to_peer,
            })
            .await?;

        Ok(SubmitRegionMigrationTaskResponse {
            procedure_id: procedure_id.map(|id| id.to_string()),
        })
    }
}

#[async_trait::async_trait]
impl HttpHandler for SubmitRegionMigrationTaskHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let request = SubmitRegionMigrationTaskRequest::try_from(params)?;

        let response = self.handle_submit(request).await?;

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(serde_json::to_string(&response).with_context(|_| {
                error::SerializeToJsonSnafu {
                    input: format!("{response:?}"),
                }
            })?)
            .context(error::InvalidHttpBodySnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use crate::error;

    #[test]
    fn test_parse_migration_task_req() {
        use store_api::storage::RegionId;

        use crate::service::admin::region_migration::SubmitRegionMigrationTaskRequest;

        let params = HashMap::from([
            ("cluster_id".to_string(), "10".to_string()),
            (
                "region_id".to_string(),
                RegionId::new(1024, 1).as_u64().to_string(),
            ),
            ("from_peer_id".to_string(), "1".to_string()),
            ("to_peer_id".to_string(), "2".to_string()),
        ]);

        let task_req = SubmitRegionMigrationTaskRequest::try_from(&params).unwrap();

        assert_eq!(
            SubmitRegionMigrationTaskRequest {
                cluster_id: 10,
                region_id: RegionId::new(1024, 1),
                from_peer_id: 1,
                to_peer_id: 2,
            },
            task_req
        );

        let params = HashMap::from([
            (
                "region_id".to_string(),
                RegionId::new(1024, 1).as_u64().to_string(),
            ),
            ("from_peer_id".to_string(), "1".to_string()),
            ("to_peer_id".to_string(), "2".to_string()),
        ]);

        let task_req = SubmitRegionMigrationTaskRequest::try_from(&params).unwrap();

        assert_eq!(
            SubmitRegionMigrationTaskRequest {
                cluster_id: 0,
                region_id: RegionId::new(1024, 1),
                from_peer_id: 1,
                to_peer_id: 2,
            },
            task_req
        );

        let required_fields = [
            (
                "region_id".to_string(),
                RegionId::new(1024, 1).as_u64().to_string(),
            ),
            ("from_peer_id".to_string(), "1".to_string()),
            ("to_peer_id".to_string(), "2".to_string()),
        ];

        for i in 0..required_fields.len() {
            let params = required_fields[..i]
                .iter()
                .cloned()
                .collect::<HashMap<_, _>>();

            let err = SubmitRegionMigrationTaskRequest::try_from(&params).unwrap_err();
            assert_matches!(err, error::Error::MissingRequiredParameter { .. });
            assert!(err.to_string().contains(&required_fields[i].0));
        }
    }
}
