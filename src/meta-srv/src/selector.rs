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

mod common;
pub mod lease_based;
pub mod load_based;
pub mod round_robin;
#[cfg(test)]
pub(crate) mod test_utils;
mod weight_compute;
mod weighted_choose;
use serde::{Deserialize, Serialize};

use crate::error;
use crate::error::Result;

pub type Namespace = u64;

#[async_trait::async_trait]
pub trait Selector: Send + Sync {
    type Context;
    type Output;

    async fn select(
        &self,
        ns: Namespace,
        ctx: &Self::Context,
        opts: SelectorOptions,
    ) -> Result<Self::Output>;
}

#[derive(Debug)]
pub struct SelectorOptions {
    /// Minimum number of selected results.
    pub min_required_items: usize,
    /// Whether duplicates are allowed in the selected result, default false.
    pub allow_duplication: bool,
}

impl Default for SelectorOptions {
    fn default() -> Self {
        Self {
            min_required_items: 1,
            allow_duplication: false,
        }
    }
}

/// [`SelectorType`] refers to the load balancer used when creating tables.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(try_from = "String")]
pub enum SelectorType {
    /// The current load balancing is based on the number of regions on each datanode node;
    /// the more regions, the higher the load (it may be changed to Capacity Units(CU)
    /// calculation in the future).
    LoadBased,
    /// This one randomly selects from all available (in lease) nodes. Its characteristic
    /// is simplicity and fast.
    LeaseBased,
    /// This one selects the node in a round-robin way.
    /// In most cases, it's recommended and is the default option. If you're unsure which
    /// to choose, using it is usually correct.
    #[default]
    RoundRobin,
}

impl TryFrom<&str> for SelectorType {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "load_based" | "LoadBased" => Ok(SelectorType::LoadBased),
            "lease_based" | "LeaseBased" => Ok(SelectorType::LeaseBased),
            "round_robin" | "RoundRobin" => Ok(SelectorType::RoundRobin),
            other => error::UnsupportedSelectorTypeSnafu {
                selector_type: other,
            }
            .fail(),
        }
    }
}

impl TryFrom<String> for SelectorType {
    type Error = error::Error;

    fn try_from(value: String) -> Result<Self> {
        SelectorType::try_from(value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::SelectorType;
    use crate::error::Result;

    #[test]
    fn test_default_selector_type() {
        assert_eq!(SelectorType::RoundRobin, SelectorType::default());
    }

    #[test]
    fn test_convert_str_to_selector_type() {
        let lease_based = "lease_based";
        let selector_type = lease_based.try_into().unwrap();
        assert_eq!(SelectorType::LeaseBased, selector_type);
        let lease_based = "LeaseBased";
        let selector_type = lease_based.try_into().unwrap();
        assert_eq!(SelectorType::LeaseBased, selector_type);

        let load_based = "load_based";
        let selector_type = load_based.try_into().unwrap();
        assert_eq!(SelectorType::LoadBased, selector_type);
        let load_based = "LoadBased";
        let selector_type = load_based.try_into().unwrap();
        assert_eq!(SelectorType::LoadBased, selector_type);

        let unknown = "unknown";
        let selector_type: Result<SelectorType> = unknown.try_into();
        assert!(selector_type.is_err());
    }
}
