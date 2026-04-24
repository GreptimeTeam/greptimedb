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

use api::v1::meta::heartbeat_request::NodeWorkloads;
use api::v1::meta::{ErrorCode, ResponseHeader};
use common_meta::cluster::{NodeInfo, NodeStatus};
use common_meta::peer::Peer;
use common_time::util::SystemTimer;
use tonic::{Code, Status};

pub(crate) fn is_unreachable(status: &Status) -> bool {
    status.code() == Code::Unavailable || status.code() == Code::DeadlineExceeded
}

pub(crate) fn is_not_leader(header: &Option<ResponseHeader>) -> bool {
    let Some(header) = header else {
        return false;
    };

    let Some(err) = header.error.as_ref() else {
        return false;
    };

    err.code == ErrorCode::NotLeader as i32
}

fn is_active_node(
    timer: &impl SystemTimer,
    node: &NodeInfo,
    active_duration: std::time::Duration,
) -> bool {
    let now = timer.current_time_millis();
    let elapsed = now.saturating_sub(node.last_activity_ts) as u64;
    elapsed < active_duration.as_millis() as u64
}

pub(crate) fn alive_frontends(
    timer: &impl SystemTimer,
    nodes: Vec<NodeInfo>,
    active_duration: std::time::Duration,
) -> Vec<Peer> {
    nodes
        .into_iter()
        .filter(|node| is_active_node(timer, node, active_duration))
        .filter_map(|node| matches!(node.status, NodeStatus::Frontend(_)).then_some(node.peer))
        .collect()
}

pub(crate) fn alive_datanodes(
    timer: &impl SystemTimer,
    nodes: Vec<NodeInfo>,
    active_duration: std::time::Duration,
    filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
) -> Vec<Peer> {
    let filter = filter.unwrap_or(|_| true);

    nodes
        .into_iter()
        .filter(|node| is_active_node(timer, node, active_duration))
        .filter_map(|node| match node.status {
            NodeStatus::Datanode(status) => {
                let workloads = NodeWorkloads::Datanode(status.workloads.clone());
                filter(&workloads).then_some(node.peer)
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn alive_flownodes(
    timer: &impl SystemTimer,
    nodes: Vec<NodeInfo>,
    active_duration: std::time::Duration,
    filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
) -> Vec<Peer> {
    let filter = filter.unwrap_or(|_| true);

    nodes
        .into_iter()
        .filter(|node| is_active_node(timer, node, active_duration))
        .filter_map(|node| match node.status {
            NodeStatus::Flownode(status) => {
                let workloads = NodeWorkloads::Flownode(status.workloads.clone());
                filter(&workloads).then_some(node.peer)
            }
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use api::v1::meta::heartbeat_request::NodeWorkloads;
    use api::v1::meta::{DatanodeWorkloads, FlownodeWorkloads, Peer};
    use common_meta::cluster::{
        DatanodeStatus, FlownodeStatus, FrontendStatus, MetasrvStatus, NodeInfo, NodeStatus, Role,
    };
    use common_time::util::SystemTimer;

    use super::*;

    struct MockSystemTimer(i64);

    impl MockSystemTimer {
        fn new(now: i64) -> Self {
            Self(now)
        }
    }

    impl SystemTimer for MockSystemTimer {
        fn current_time_millis(&self) -> i64 {
            self.0
        }

        fn current_time_rfc3339(&self) -> String {
            "1970-01-01T00:00:00Z".to_string()
        }
    }

    fn node_info(role: Role, id: u64, addr: &str, last_activity_ts: i64) -> NodeInfo {
        let status = match role {
            Role::Frontend => NodeStatus::Frontend(FrontendStatus {}),
            Role::Datanode => NodeStatus::Datanode(DatanodeStatus {
                rcus: 0,
                wcus: 0,
                leader_regions: 0,
                follower_regions: 0,
                workloads: DatanodeWorkloads { types: vec![] },
            }),
            Role::Flownode => NodeStatus::Flownode(FlownodeStatus {
                workloads: FlownodeWorkloads { types: vec![] },
            }),
            Role::Metasrv => NodeStatus::Metasrv(MetasrvStatus { is_leader: false }),
        };

        NodeInfo {
            peer: Peer::new(id, addr),
            last_activity_ts,
            status,
            version: String::new(),
            git_commit: String::new(),
            start_time_ms: 0,
            total_cpu_millicores: 0,
            total_memory_bytes: 0,
            cpu_usage_millicores: 0,
            memory_usage_bytes: 0,
            hostname: String::new(),
        }
    }

    fn ingest_only(workloads: &NodeWorkloads) -> bool {
        matches!(
            workloads,
            NodeWorkloads::Datanode(DatanodeWorkloads { types }) if types.as_slice() == [1]
        )
    }

    fn empty_flownode_workloads(workloads: &NodeWorkloads) -> bool {
        matches!(
            workloads,
            NodeWorkloads::Flownode(FlownodeWorkloads { types }) if types.is_empty()
        )
    }

    #[test]
    fn test_alive_frontends_filters_by_activity_and_role() {
        let timer = MockSystemTimer::new(100);
        let peers = alive_frontends(
            &timer,
            vec![
                node_info(Role::Frontend, 1, "127.0.0.1:3001", 95),
                node_info(Role::Frontend, 2, "127.0.0.1:3002", 89),
                node_info(Role::Datanode, 3, "127.0.0.1:4001", 99),
            ],
            Duration::from_millis(10),
        );

        assert_eq!(
            vec![1],
            peers.into_iter().map(|peer| peer.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_alive_datanodes_filters_by_activity_and_workload() {
        let timer = MockSystemTimer::new(100);
        let mut first = node_info(Role::Datanode, 1, "127.0.0.1:4001", 95);
        let mut second = node_info(Role::Datanode, 2, "127.0.0.1:4002", 95);
        let stale = node_info(Role::Datanode, 3, "127.0.0.1:4003", 89);

        if let NodeStatus::Datanode(status) = &mut first.status {
            status.workloads = DatanodeWorkloads { types: vec![1] };
        }
        if let NodeStatus::Datanode(status) = &mut second.status {
            status.workloads = DatanodeWorkloads { types: vec![2] };
        }

        let peers = alive_datanodes(
            &timer,
            vec![first, second, stale],
            Duration::from_millis(10),
            Some(ingest_only),
        );

        assert_eq!(
            vec![1],
            peers.into_iter().map(|peer| peer.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_alive_flownodes_uses_empty_workload_semantics() {
        let timer = MockSystemTimer::new(100);
        let peers = alive_flownodes(
            &timer,
            vec![
                node_info(Role::Flownode, 1, "127.0.0.1:5001", 95),
                node_info(Role::Flownode, 2, "127.0.0.1:5002", 89),
                node_info(Role::Frontend, 3, "127.0.0.1:3001", 99),
            ],
            Duration::from_millis(10),
            Some(empty_flownode_workloads),
        );

        assert_eq!(
            vec![1],
            peers.into_iter().map(|peer| peer.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_alive_flownodes_filters_by_workloads() {
        let timer = MockSystemTimer::new(100);
        let mut first = node_info(Role::Flownode, 1, "127.0.0.1:5001", 95);
        let mut second = node_info(Role::Flownode, 2, "127.0.0.1:5002", 95);

        if let NodeStatus::Flownode(status) = &mut first.status {
            status.workloads = FlownodeWorkloads { types: vec![7] };
        }
        if let NodeStatus::Flownode(status) = &mut second.status {
            status.workloads = FlownodeWorkloads { types: vec![8] };
        }

        fn workload_type_is_7(workloads: &NodeWorkloads) -> bool {
            matches!(
                workloads,
                NodeWorkloads::Flownode(FlownodeWorkloads { types }) if types.as_slice() == [7]
            )
        }

        let peers = alive_flownodes(
            &timer,
            vec![first, second],
            Duration::from_millis(10),
            Some(workload_type_is_7),
        );

        assert_eq!(
            vec![1],
            peers.into_iter().map(|peer| peer.id).collect::<Vec<_>>()
        );
    }
}
