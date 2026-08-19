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

use api::v1::meta::{FlowStat, HeartbeatRequest, Role};
use common_meta::key::flow::flow_state::{FlowStateManager, FlowStateValue};
use common_telemetry::debug;
use snafu::ResultExt;

use crate::error::{FlowStateHandlerSnafu, Result};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

/// Extracts the flownode identity from a heartbeat request.
///
/// Prefers `header.member_id` — the canonical identity metasrv uses for
/// flownodes (see `get_node_id` in `src/meta-srv/src/service/heartbeat.rs`).
/// Falls back to `peer.id`, the operator-configured node id, which is also
/// unique within the cluster (a flownode requires `node_id` in its config, see
/// `src/cmd/src/flownode.rs`). Returns `None` when neither is present.
fn node_identity(req: &HeartbeatRequest) -> Option<u64> {
    req.header
        .as_ref()
        .map(|header| header.member_id)
        .or_else(|| req.peer.as_ref().map(|peer| peer.id))
}

pub struct FlowStateHandler {
    flow_state_manager: FlowStateManager,
}

impl FlowStateHandler {
    pub fn new(flow_state_manager: FlowStateManager) -> Self {
        Self { flow_state_manager }
    }
}

#[async_trait::async_trait]
impl HeartbeatHandler for FlowStateHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Flownode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        _ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        if let Some(FlowStat {
            flow_stat_size,
            flow_last_exec_time_map,
        }) = &req.flow_stat
        {
            let state_size = flow_stat_size
                .iter()
                .map(|(k, v)| (*k, *v as usize))
                .collect();
            let last_exec_time_map = flow_last_exec_time_map
                .iter()
                .map(|(k, v)| (*k, *v))
                .collect();
            // The release heartbeat wire format carries the two fields represented
            // by FlowStateValue.
            let value: FlowStateValue = FlowStateValue::new(state_size, last_exec_time_map);
            match node_identity(req) {
                Some(node_id) => {
                    // Merge by node so that reports from different flownodes
                    // don't overwrite each other in the global state.
                    self.flow_state_manager
                        .merge(node_id, value)
                        .await
                        .context(FlowStateHandlerSnafu)?;
                }
                // No usable identity in the request: ignore the report instead
                // of falling back to a whole-map replace, which would clobber
                // other nodes' reports.
                // Normal flownodes always carry header.member_id/peer.id; a
                // report without either indicates an old or malformed client.
                // Log at debug to avoid an anomalous sender spamming warn.
                None => {
                    debug!(
                        "Ignore flow state report without node identity (no header.member_id and no peer.id): {value:?}"
                    );
                }
            }
        }
        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::meta::{HeartbeatRequest, Peer, RequestHeader, Role};

    use super::*;
    use crate::handler::test_utils::TestEnv;

    #[test]
    fn test_node_identity_prefers_header_member_id() {
        let req = HeartbeatRequest {
            header: Some(RequestHeader::new(42, Role::Flownode, HashMap::new())),
            peer: Some(Peer {
                id: 99,
                addr: "127.0.0.1:4001".to_string(),
            }),
            ..Default::default()
        };
        assert_eq!(node_identity(&req), Some(42));
    }

    #[test]
    fn test_node_identity_falls_back_to_peer_id() {
        let req = HeartbeatRequest {
            header: None,
            peer: Some(Peer {
                id: 99,
                addr: "127.0.0.1:4001".to_string(),
            }),
            ..Default::default()
        };
        assert_eq!(node_identity(&req), Some(99));
    }

    #[test]
    fn test_node_identity_none_without_header_and_peer() {
        let req = HeartbeatRequest::default();
        assert_eq!(node_identity(&req), None);
    }

    fn flow_stat() -> api::v1::meta::FlowStat {
        api::v1::meta::FlowStat {
            flow_stat_size: HashMap::from([(1, 1024)]),
            flow_last_exec_time_map: HashMap::from([(1, 100)]),
        }
    }

    #[tokio::test]
    async fn test_handle_merges_reports_from_different_nodes() {
        let env = TestEnv::new();
        let ctx = env.ctx();
        let flow_state_manager = FlowStateManager::new(ctx.in_memory.clone().as_kv_backend_ref());
        let handler = FlowStateHandler::new(flow_state_manager);

        // Node 42 (header.member_id) reports flow 1.
        let req_a = HeartbeatRequest {
            header: Some(RequestHeader::new(42, Role::Flownode, HashMap::new())),
            peer: Some(Peer {
                id: 42,
                addr: "127.0.0.1:4001".to_string(),
            }),
            flow_stat: Some(flow_stat()),
            ..Default::default()
        };
        let mut ctx = env.ctx();
        let mut acc = HeartbeatAccumulator::default();
        handler.handle(&req_a, &mut ctx, &mut acc).await.unwrap();

        // Node 7 (only peer.id present) reports flow 2.
        let req_b = HeartbeatRequest {
            header: None,
            peer: Some(Peer {
                id: 7,
                addr: "127.0.0.1:4007".to_string(),
            }),
            flow_stat: Some(api::v1::meta::FlowStat {
                flow_stat_size: HashMap::from([(2, 2048)]),
                flow_last_exec_time_map: HashMap::from([(2, 200)]),
            }),
            ..Default::default()
        };
        let mut ctx = env.ctx();
        let mut acc = HeartbeatAccumulator::default();
        handler.handle(&req_b, &mut ctx, &mut acc).await.unwrap();

        let value = handler.flow_state_manager.get().await.unwrap().unwrap();
        assert_eq!(value.last_exec_time_map.get(&1), Some(&100));
        assert_eq!(value.last_exec_time_map.get(&2), Some(&200));
    }

    #[tokio::test]
    async fn test_handle_ignores_report_without_identity() {
        let env = TestEnv::new();
        let ctx = env.ctx();
        let flow_state_manager = FlowStateManager::new(ctx.in_memory.clone().as_kv_backend_ref());
        let handler = FlowStateHandler::new(flow_state_manager);

        // No header and no peer: the report must be ignored and no KV written.
        let req = HeartbeatRequest {
            header: None,
            peer: None,
            flow_stat: Some(flow_stat()),
            ..Default::default()
        };
        let mut ctx = env.ctx();
        let mut acc = HeartbeatAccumulator::default();
        handler.handle(&req, &mut ctx, &mut acc).await.unwrap();

        assert!(handler.flow_state_manager.get().await.unwrap().is_none());
    }
}
