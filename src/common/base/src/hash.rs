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

use std::hash::BuildHasher;

use ahash::RandomState;
use serde::{Deserialize, Serialize};

/// A random state with fixed seeds.
///
/// This is used to ensure that the hash values are consistent across
/// different processes, and easy to serialize and deserialize.
#[derive(Debug)]
pub struct FixedRandomState {
    state: RandomState,
}

impl FixedRandomState {
    // some random seeds
    const RANDOM_SEED_0: u64 = 0x517cc1b727220a95;
    const RANDOM_SEED_1: u64 = 0x428a2f98d728ae22;
    const RANDOM_SEED_2: u64 = 0x7137449123ef65cd;
    const RANDOM_SEED_3: u64 = 0xb5c0fbcfec4d3b2f;

    pub fn new() -> Self {
        Self {
            state: ahash::RandomState::with_seeds(
                Self::RANDOM_SEED_0,
                Self::RANDOM_SEED_1,
                Self::RANDOM_SEED_2,
                Self::RANDOM_SEED_3,
            ),
        }
    }
}

impl Default for FixedRandomState {
    fn default() -> Self {
        Self::new()
    }
}

impl BuildHasher for FixedRandomState {
    type Hasher = ahash::AHasher;

    fn build_hasher(&self) -> Self::Hasher {
        self.state.build_hasher()
    }

    fn hash_one<T: std::hash::Hash>(&self, x: T) -> u64 {
        self.state.hash_one(x)
    }
}

impl Serialize for FixedRandomState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for FixedRandomState {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::new())
    }
}

pub fn partition_rule_version(expr_json: Option<&str>) -> u64 {
    let expr = expr_json.unwrap_or_default();
    if expr.is_empty() {
        return 0;
    }
    FixedRandomState::new().hash_one(expr)
}
