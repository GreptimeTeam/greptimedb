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
                oid = self.hasher.hash_one(schema_name) as u32;
            }
            self.oid_map.insert(schema_name.to_string(), oid);
            oid
        }
    }
}
