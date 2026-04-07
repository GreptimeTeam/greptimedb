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

use std::collections::BTreeMap;

use kube::Api;

use crate::error::Result;
use crate::utils::crd::common::{Mode, Selector, SelectorBuilder};
use crate::utils::crd::network::{
    Action, Direction, NetworkChaos, NetworkChaosSpecBuilder, TargetBuilder,
};

fn build_datanode_selector(namespace: &str, cluster_name: &str) -> Selector {
    let mut selector = BTreeMap::new();
    selector.insert(
        "app.greptime.io/component".into(),
        format!("{cluster_name}-datanode"),
    );

    SelectorBuilder::default()
        .namespaces(vec![namespace.to_string()])
        .label_selectors(selector)
        .build()
        .unwrap()
}

fn build_metasrv_selector(namespace: &str, cluster_name: &str) -> Selector {
    let mut selector = BTreeMap::new();
    selector.insert(
        "app.greptime.io/component".into(),
        format!("{cluster_name}-meta"),
    );

    SelectorBuilder::default()
        .namespaces(vec![namespace.to_string()])
        .label_selectors(selector)
        .build()
        .unwrap()
}

/// Injects a network partition between a datanode pod and metasrv.
pub async fn inject_datanode_metasrv_network_partition(
    client: kube::Client,
    namespace: &str,
    cluster_name: &str,
    duration_secs: usize,
) -> Result<String> {
    let selector = build_datanode_selector(namespace, cluster_name);
    let target = TargetBuilder::default()
        .mode(Mode::All)
        .selector(build_metasrv_selector(namespace, cluster_name))
        .build()
        .unwrap();

    let spec = NetworkChaosSpecBuilder::default()
        .action(Action::Partition)
        .direction(Direction::Both)
        .mode(Mode::All)
        .selector(selector)
        .target(target)
        .duration(format!("{duration_secs}s"))
        .build()
        .unwrap();

    let chaos_name = "datanode-metasrv-network-partition".to_string();
    let cr = NetworkChaos::new(&chaos_name, spec);
    let api: Api<NetworkChaos> = Api::namespaced(client, namespace);
    api.create(&Default::default(), &cr).await.unwrap();

    Ok(chaos_name)
}

/// Recovers network chaos by deleting the associated NetworkChaos resource.
pub async fn recover_network_chaos(
    client: kube::Client,
    namespace: &str,
    name: &str,
) -> Result<()> {
    let api: Api<NetworkChaos> = Api::namespaced(client, namespace);
    api.delete(name, &Default::default()).await.unwrap();
    Ok(())
}
