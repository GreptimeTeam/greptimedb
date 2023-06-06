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

use datanode::datanode::DatanodeOptions;
use frontend::frontend::FrontendOptions;
use lazy_static::lazy_static;
use meta_srv::metasrv::MetaSrvOptions;
use regex::Regex;
use servers::http::HttpOptions;

use super::node::NodeInfo;
use super::process::{list_processes_by_name, ProcessInfo};
use crate::cluster::bare::node::Node;
use crate::cluster::Cluster;
use crate::error::Result;
/// `DebugCluster` only returns existing node infos on the local machine.
#[derive(Debug, Clone)]
pub struct DebugCluster;

const GREPTIME_DB_EXEC_FILENAME: &str = "greptime";

lazy_static! {
    static ref DATANODE_NODE_ID: Regex = Regex::new("^--node-id=([0-9]+)$").unwrap();
}

#[async_trait::async_trait]
impl Cluster<Node> for DebugCluster {
    async fn apply(&self) -> Result<()> {
        Ok(())
    }

    async fn delete(&self) -> Result<()> {
        Ok(())
    }

    async fn nodes(&self) -> Result<Vec<Node>> {
        let processes = list_processes_by_name(GREPTIME_DB_EXEC_FILENAME.to_string())?;
        // TODO(weny): add etcd nodes?
        let mut nodes = Vec::with_capacity(processes.len());
        for proc in processes {
            let node = new_debug_node(proc.try_into()?);
            nodes.push(node);
        }

        Ok(nodes)
    }
}

fn parse_node_id(val: &str) -> u64 {
    let node_id = DATANODE_NODE_ID.captures(val).unwrap()[1].to_string();
    node_id.parse().unwrap()
}

fn new_debug_node(process: ProcessInfo) -> Node {
    let info = match process.cmdline[1].as_str() {
        "datanode" => {
            // ["greptime", "datanode", "start", "--node-id=0", "--metasrv-addr=0.0.0.0:3002", "--rpc-addr=0.0.0.0:14100", "--http-addr=0.0.0.0:14300"]
            let node_id = parse_node_id(&process.cmdline[3]);
            let options = Box::new(DatanodeOptions {
                node_id: Some(node_id),
                rpc_addr: "0.0.0.0:14100".to_string(),
                http_opts: HttpOptions {
                    addr: "0.0.0.0:14300".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            });
            NodeInfo::Datanode { options }
        }
        "metasrv" => {
            // ["greptime", "metasrv", "start", "--store-addr", "127.0.0.1:2379", "--server-addr", "0.0.0.0:3002", "--http-addr", "0.0.0.0:14001"]
            let options = Box::new(MetaSrvOptions {
                server_addr: "0.0.0.0:3002".to_string(),
                http_opts: HttpOptions {
                    addr: "0.0.0.0:14001".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            });
            NodeInfo::Metasrv { options }
        }
        "frontend" => {
            // ["greptime", "frontend", "start", "--metasrv-addr=0.0.0.0:3002"]
            let options = Box::<FrontendOptions>::default();
            NodeInfo::Frontend { options }
        }
        _ => unreachable!("unknown: {}", process.cmdline[1].as_str()),
    };

    Node {
        info,
        process: Some(process),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_node_id;

    #[test]
    fn test_parse_node_id() {
        let node_id = parse_node_id("--node-id=1");
        assert_eq!(node_id, 1)
    }
}
