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
use crate::utils::crd::common::{Mode, SelectorBuilder};
use crate::utils::crd::pod::{Action, PodChaos, PodChaosSpecBuilder};

/// Injects a pod failure into a specific datanode within a Kubernetes cluster.
///
/// This function constructs a `PodChaos` custom resource to simulate a pod failure for the specified
/// datanode, and creates this resource in the Kubernetes cluster using the provided client.
pub async fn inject_datanode_pod_failure(
    client: kube::Client,
    namespace: &str,
    cluster_name: &str,
    datanode_id: u64,
    duration_secs: usize,
) -> Result<String> {
    let mut selector = BTreeMap::new();
    let pod_name = format!("{}-datanode-{}", cluster_name, datanode_id);
    selector.insert(
        "statefulset.kubernetes.io/pod-name".into(),
        pod_name.clone(),
    );
    let selector = SelectorBuilder::default()
        .label_selectors(selector)
        .build()
        .unwrap();

    let spec = PodChaosSpecBuilder::default()
        .duration(format!("{duration_secs}s"))
        .selector(selector)
        .action(Action::PodFailure)
        .mode(Mode::One)
        .build()
        .unwrap();
    let chaos_name = format!("{pod_name}-pod-failure");
    let cr = PodChaos::new(&chaos_name, spec);
    let api: Api<PodChaos> = Api::namespaced(client, namespace);
    api.create(&Default::default(), &cr).await.unwrap();

    Ok(chaos_name)
}

/// Recovers a pod from a failure by deleting the associated PodChaos resource.
///
/// This function deletes the PodChaos custom resource with the specified name, effectively
/// recovering the pod from the injected failure.
pub async fn recover_pod_failure(client: kube::Client, namespace: &str, name: &str) -> Result<()> {
    let api: Api<PodChaos> = Api::namespaced(client, namespace);
    api.delete(name, &Default::default()).await.unwrap();
    Ok(())
}
