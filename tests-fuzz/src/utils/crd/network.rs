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

use super::common::{Mode, Selector};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Builder, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    mode: Mode,
    selector: Selector,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Action {
    Netem,
    Delay,
    Loss,
    Corrupt,
    Partition,
    Bandwidth,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Direction {
    From,
    To,
    Both,
}

#[derive(
    CustomResource, Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Builder, JsonSchema,
)]
#[kube(
    group = "chaos-mesh.org",
    version = "v1alpha1",
    namespaced,
    kind = "NetworkChaos",
    plural = "networkchaos",
    singular = "networkchaos",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct NetworkChaosSpec {
    // Indicates the specific fault type. Available types include:
    // netem, delay (network delay), loss (packet loss), duplicate (packet duplicating),
    // corrupt (packet corrupt), partition (network partition), and bandwidth (network bandwidth limit).
    // After you specify action field,
    // refer to Description for action-related fields for other necessary field configuration.
    action: Action,
    // Used in combination with direction, making Chaos only effective for some packets.
    #[builder(setter(into), default = "None")]
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<Target>,
    // Indicates the direction of target packets. Available value include `from` (the packets from target),
    // `to` (the packets to target), and `both` ( the packets from or to target).
    // This parameter makes Chaos only take effect for a specific direction of packets.
    #[builder(setter(into), default = "None")]
    #[serde(skip_serializing_if = "Option::is_none")]
    direction: Option<Direction>,
    // Specifies the mode of the experiment. The mode options include one (selecting a random Pod),
    // all (selecting all eligible Pods), fixed (selecting a specified number of eligible Pods),
    // fixed-percent (selecting a specified percentage of Pods from the eligible Pods),
    // and random-max-percent (selecting the maximum percentage of Pods from the eligible Pods).
    mode: Mode,
    // Provides a parameter for the mode configuration, depending on mode.
    // For example, when mode is set to fixed-percent, value specifies the percentage of Pods.
    #[builder(setter(into), default = "None")]
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    // Specifies the target Pod.
    selector: Selector,
    // Indicates the network targets except for Kubernetes, which can be IPv4 addresses or domains.
    // This parameter only works with `direction: to`.
    #[builder(setter(into), default = "Vec::new()")]
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    external_targets: Vec<String>,
    // Specifies the affected network interface
    #[builder(setter(into), default = "None")]
    #[serde(skip_serializing_if = "Option::is_none")]
    device: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    delay: Option<Delay>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    loss: Option<Loss>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    duplicate: Option<Duplicate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    corrupt: Option<Corrupt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    bandwidth: Option<Bandwidth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into), default = "None")]
    duration: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize, Builder, JsonSchema)]
#[serde(default)]
#[builder(default)]
pub struct Delay {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    // Indicates the network latency.
    latency: Option<String>,
    // Indicates the correlation between the current latency and the previous one.
    // Range of value: [0, 100]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    correlation: Option<String>,
    // Indicates the range of the network latency.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    jitter: Option<String>,
    // Indicates the status of network packet reordering
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    reorder: Option<Reorder>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema, Builder)]
#[serde(default)]
#[builder(default)]
pub struct Reorder {
    // Indicates the probability to reorder
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    reorder: Option<String>,
    // Indicates the correlation between this time's length of delay time
    // and the previous time's length of delay time. Range of value: [0, 100]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    correlation: Option<String>,
    // Indicates the gap before and after packet reordering.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    gap: Option<i32>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema, Builder)]
#[serde(default)]
#[builder(default, setter(into, strip_option))]
pub struct Loss {
    // Indicates the probability of packet loss.
    // Range of value: [0, 100].
    #[serde(skip_serializing_if = "Option::is_none")]
    loss: Option<String>,
    // Indicates the correlation between the probability of current packet loss
    // and the previous time's packet loss. Range of value: [0, 100].
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema, Builder)]
#[serde(default)]
#[builder(default, setter(into, strip_option))]
pub struct Duplicate {
    // Indicates the probability of packet duplicating.
    // Range of value: [0, 100]
    #[serde(skip_serializing_if = "Option::is_none")]
    duplicate: Option<String>,
    // Indicates the correlation between the probability of current packet duplicating
    // and the previous time's packet duplicating. Range of value: [0, 100]
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema, Builder)]
#[serde(default)]
#[builder(default, setter(into, strip_option))]
pub struct Corrupt {
    // Indicates the probability of packet corruption. Range of value: [0, 100]
    #[serde(skip_serializing_if = "Option::is_none")]
    corrupt: Option<String>,
    // Indicates the correlation between the probability of current packet corruption
    // and the previous time's packet corruption. Range of value: [0, 100]
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema, Builder)]
#[serde(default)]
#[builder(default)]
pub struct Bandwidth {
    // Indicates the rate of bandwidth limit. e.g., 1mbps.
    rate: String,
    // Indicates the number of bytes waiting in queue.
    limit: u32,
    // Indicates the maximum number of bytes that can be sent instantaneously.
    buffer: u32,
    // Indicates the maximum consumption of `bucket` (usually not set)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    peakrate: Option<u64>,
    // Indicates the size of peakrate bucket (usually not set).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option))]
    minburst: Option<u32>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Action, Direction, NetworkChaosSpecBuilder, TargetBuilder};
    use crate::utils::crd::common::{Mode, SelectorBuilder};
    use crate::utils::crd::network::{DelayBuilder, NetworkChaos};

    #[test]
    fn test_serde() {
        let mut fs = BTreeMap::new();
        fs.insert("app".into(), "datanode".into());
        let selector = SelectorBuilder::default()
            .namespaces(vec!["default".into()])
            .label_selectors(fs)
            .build()
            .unwrap();
        let target = TargetBuilder::default()
            .mode(Mode::All)
            .selector(selector.clone())
            .build()
            .unwrap();
        let delay = DelayBuilder::default()
            .latency("100ms")
            .correlation("100")
            .jitter("0ms")
            .build()
            .unwrap();
        let spec = NetworkChaosSpecBuilder::default()
            .action(Action::Delay)
            .target(target)
            .direction(Direction::Both)
            .mode(Mode::One)
            .selector(selector)
            .delay(delay)
            .duration(Some("10s".to_string()))
            .build()
            .unwrap();
        let crd = NetworkChaos::new("my-delay", spec);
        let ser = serde_yaml::to_string(&crd).unwrap();

        let expected = r#"apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: my-delay
spec:
  action: delay
  target:
    mode: all
    selector:
      namespaces:
      - default
      labelSelectors:
        app: datanode
  direction: Both
  mode: one
  selector:
    namespaces:
    - default
    labelSelectors:
      app: datanode
  delay:
    latency: 100ms
    correlation: '100'
    jitter: 0ms
  duration: 10s
"#;
        assert_eq!(expected, ser);
    }
}
