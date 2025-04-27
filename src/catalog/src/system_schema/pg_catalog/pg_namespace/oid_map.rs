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
use std::sync::Arc;

use dashmap::DashMap;
use rustc_hash::FxSeededState;

pub type PGNamespaceOidMapRef = Arc<PGNamespaceOidMap>;
// Workaround to convert schema_name to a numeric id,
// remove this when we have numeric schema id in greptime
pub struct PGNamespaceOidMap {
    oid_map: DashMap<String, u32>,

    // Rust use SipHasher by default, which provides resistance against DOS attacks.
    // This will produce different hash value between each greptime instance. This will
    // cause the sqlness test fail. We need a deterministic hash here to provide
    // same oid for the same schema name with best effort and DOS attacks aren't concern here.
    hasher: FxSeededState,
}

impl PGNamespaceOidMap {
    pub fn new() -> Self {
        Self {
            oid_map: DashMap::new(),
            hasher: FxSeededState::with_seed(0), // PLEASE DO NOT MODIFY THIS SEED VALUE!!!
        }
    }

    fn oid_is_used(&self, oid: u32) -> bool {
        self.oid_map.iter().any(|e| *e.value() == oid)
    }

    pub fn get_oid(&self, schema_name: &str) -> u32 {
        if let Some(oid) = self.oid_map.get(schema_name) {
            *oid
        } else {
            let mut oid = self.hasher.hash_one(schema_name) as u32;
            while self.oid_is_used(oid) {
                oid = self.hasher.hash_one(oid) as u32;
            }
            self.oid_map.insert(schema_name.to_string(), oid);
            oid
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn oid_is_stable() {
        let oid_map_1 = PGNamespaceOidMap::new();
        let oid_map_2 = PGNamespaceOidMap::new();

        let schema = "schema";
        let oid = oid_map_1.get_oid(schema);

        // oid keep stable in the same instance
        assert_eq!(oid, oid_map_1.get_oid(schema));

        // oid keep stable between different instances
        assert_eq!(oid, oid_map_2.get_oid(schema));
    }

    #[test]
    fn oid_collision() {
        let oid_map = PGNamespaceOidMap::new();

        let key1 = "3178510";
        let key2 = "4215648";

        // insert them into oid_map
        let oid1 = oid_map.get_oid(key1);
        let oid2 = oid_map.get_oid(key2);

        // they should have different id
        assert_ne!(oid1, oid2);
    }
}
