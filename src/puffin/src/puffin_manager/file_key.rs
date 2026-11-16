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

use std::sync::Arc;

use uuid::Uuid;

/// Stable storage identity shared by file keys from one accessor.
/// Fields stay separate so separators in paths or store names cannot cause collisions.
/// Canonical names can use [`Self::new`]; backends whose metadata does not uniquely
/// identify their endpoint or configuration use [`Self::unique`] and share that scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PuffinStorageScope {
    scheme: String,
    name: String,
    root: String,
    instance_id: Uuid,
}

impl PuffinStorageScope {
    /// Identifies the object store backend, name and root.
    /// Reconstructed accessors must use the same canonical values for the same storage.
    /// The name must distinguish backends with different endpoints or configurations.
    pub fn new(scheme: String, name: String, root: String) -> Self {
        Self {
            scheme,
            name,
            root,
            instance_id: Uuid::nil(),
        }
    }

    /// Creates isolated identity when store metadata alone cannot distinguish backends.
    /// Accessors for the same live backend share this scope rather than recreating it.
    pub fn unique(scheme: String, name: String, root: String) -> Self {
        Self {
            scheme,
            name,
            root,
            instance_id: Uuid::new_v4(),
        }
    }
}

/// Immutable file identity for caches; the handle must include its physical version.
/// Equal storage scopes compare by value even when independently allocated.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PuffinFileKey<H> {
    scope: Arc<PuffinStorageScope>,
    path: Arc<str>,
    handle: H,
}

impl<H> PuffinFileKey<H> {
    /// Combines the accessor's resolved file path and versioned handle with its store scope.
    pub fn new(scope: Arc<PuffinStorageScope>, path: String, handle: H) -> Self {
        Self {
            scope,
            path: path.into(),
            handle,
        }
    }

    /// Converts the handle representation while preserving the same resolved storage location.
    pub fn map_handle<T>(self, map: impl FnOnce(H) -> T) -> PuffinFileKey<T> {
        PuffinFileKey {
            scope: self.scope,
            path: self.path,
            handle: map(self.handle),
        }
    }

    /// Returns the versioned handle for cache invalidation and file bookkeeping.
    pub fn handle(&self) -> &H {
        &self.handle
    }

    /// Conservative key size including the shared scope, but excluding any heap
    /// allocations owned by the generic handle. Shared scope is charged per key.
    pub fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<PuffinStorageScope>()
            + self.scope.scheme.capacity()
            + self.scope.name.capacity()
            + self.scope.root.capacity()
            + self.path.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn scope(parts: [&str; 3]) -> Arc<PuffinStorageScope> {
        let [scheme, name, root] = parts.map(str::to_string);
        Arc::new(PuffinStorageScope::new(scheme, name, root))
    }

    #[test]
    fn test_reconstructed_keys_share_identity() {
        let parts = ["s3", "bucket", "prefix/"];
        let original = PuffinFileKey::new(scope(parts), "table/file".into(), (17_u64, 2_u32));
        let reconstructed = PuffinFileKey::new(scope(parts), "table/file".into(), (17_u64, 2_u32));
        let keys = HashSet::from([original.clone()]);
        assert!(keys.contains(&reconstructed));
        assert!(keys.contains(&original.clone()));
        assert_eq!(&(17_u64, 2_u32), original.handle());
        assert!(!keys.contains(&PuffinFileKey::new(
            scope(parts),
            "table/file".into(),
            (17_u64, 3_u32)
        )));
        assert!(!keys.contains(&PuffinFileKey::new(
            scope(parts),
            "table/file".into(),
            (18_u64, 2_u32)
        )));
    }

    #[test]
    fn test_unique_scopes_require_explicit_sharing() {
        let first = Arc::new(PuffinStorageScope::unique(
            "s3".into(),
            "bucket".into(),
            "/".into(),
        ));
        let second = Arc::new(PuffinStorageScope::unique(
            "s3".into(),
            "bucket".into(),
            "/".into(),
        ));
        let key = PuffinFileKey::new(first.clone(), "file".into(), 1_u64);
        assert_eq!(key, PuffinFileKey::new(first, "file".into(), 1_u64));
        assert_ne!(key, PuffinFileKey::new(second, "file".into(), 1_u64));
    }

    #[test]
    fn test_storage_components_are_independent() {
        let parts = ["s3", "bucket", "prefix/"];
        let original = PuffinFileKey::new(scope(parts), "table/file".into(), 17_u64);
        for index in 0..parts.len() {
            let mut changed = parts;
            changed[index] = "different";
            assert_ne!(
                original,
                PuffinFileKey::new(scope(changed), "table/file".into(), 17_u64)
            );
        }
        assert_ne!(
            original,
            PuffinFileKey::new(scope(parts), "other/file".into(), 17_u64)
        );
        assert_ne!(
            PuffinFileKey::new(scope(["s3", "a:b", "c"]), "d".into(), 17_u64),
            PuffinFileKey::new(scope(["s3", "a", "b:c"]), "d".into(), 17_u64),
        );
    }
}
