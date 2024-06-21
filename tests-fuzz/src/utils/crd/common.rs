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

use derive_builder::Builder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    One,
    All,
    Fixed,
    FixedPercent,
    RandomMaxPercent,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default, Builder, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
#[builder(setter(into, strip_option), default)]
pub struct Selector {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    namespaces: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    label_selectors: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    expression_selectors: Vec<ExpressionSelector>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    annotation_selectors: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    field_selectors: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pod_phase_selectors: BTreeMap<String, String>,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ExpressionSelector {
    key: String,
    operator: String,
    values: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::SelectorBuilder;

    #[test]
    fn test_serde() {
        let mut fs = BTreeMap::new();
        fs.insert("app.kubernetes.io/component".into(), "tikv".into());
        let selector = SelectorBuilder::default()
            .field_selectors(fs)
            .build()
            .unwrap();
        let ser = serde_yaml::to_string(&selector).unwrap();
        assert_eq!(
            r#"fieldSelectors:
  app.kubernetes.io/component: tikv
"#,
            ser
        );
    }
}
