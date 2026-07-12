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

use derive_builder::Builder;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::utils::crd::common::{Mode, Selector};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Action {
    PodFailure,
    PodKill,
    ContainerKill,
}

#[derive(
    CustomResource, Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Builder, JsonSchema,
)]
#[kube(
    group = "chaos-mesh.org",
    version = "v1alpha1",
    namespaced,
    kind = "PodChaos",
    plural = "podchaos",
    singular = "podchaos",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct PodChaosSpec {
    // Specifies the fault type to inject. The supported types include pod-failure, pod-kill, and container-kill.
    action: Action,
    // Specifies the mode of the experiment.
    // The mode options include one (selecting a random Pod),
    // all (selecting all eligible Pods),
    // fixed (selecting a specified number of eligible Pods),
    // fixed-percent (selecting a specified percentage of Pods from the eligible Pods),
    // and random-max-percent (selecting the maximum percentage of Pods from the eligible Pods).
    mode: Mode,
    // Provides parameters for the mode configuration, depending on mode.
    // For example, when mode is set to `fixed-percent`, value specifies the percentage of Pods.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    value: Option<String>,
    // Specifies the target Pod.
    selector: Selector,
    // When you configure action to `container-kill`,
    // this configuration is mandatory to specify the target container name for injecting faults.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    #[builder(setter(into), default = "Vec::new()")]
    container_names: Vec<String>,
    // When you configure action to `pod-kill`,
    // this configuration is mandatory to specify the duration before deleting Pod.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    grace_period: Option<i64>,
    // Specifies the duration of the experiment.
    #[builder(setter(into), default)]
    duration: String,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::utils::crd::common::SelectorBuilder;

    #[test]
    fn test_crd_serialization() {
        let mut fs = BTreeMap::new();
        fs.insert("app.kubernetes.io/component".into(), "tikv".into());
        let selector = SelectorBuilder::default()
            .field_selectors(fs)
            .build()
            .unwrap();

        let spec = PodChaosSpecBuilder::default()
            .duration("10s")
            .selector(selector)
            .action(Action::PodKill)
            .mode(Mode::One)
            .build()
            .unwrap();
        let crd = PodChaos::new("my-pod-chaos", spec);
        let serialized = serde_yaml::to_string(&crd).unwrap();
        let expected = r#"apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: my-pod-chaos
spec:
  action: pod-kill
  mode: one
  selector:
    fieldSelectors:
      app.kubernetes.io/component: tikv
  duration: 10s
"#;
        assert_eq!(expected, serialized);
    }
}
