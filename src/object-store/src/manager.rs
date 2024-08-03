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

use std::collections::HashMap;
use std::sync::Arc;

use crate::ObjectStore;

pub type ObjectStoreManagerRef = Arc<ObjectStoreManager>;

/// Manages multiple object stores so that users can configure a storage for each table.
/// This struct certainly have one default object store, and can have zero or more custom object stores.
#[derive(Debug)]
pub struct ObjectStoreManager {
    stores: HashMap<String, ObjectStore>,
    default_object_store: ObjectStore,
}

impl ObjectStoreManager {
    /// Creates a new manager from the object store used as a default one.
    pub fn new(name: &str, object_store: ObjectStore) -> Self {
        ObjectStoreManager {
            stores: [(name.to_lowercase(), object_store.clone())].into(),
            default_object_store: object_store,
        }
    }

    /// Adds an object store to the manager.
    pub fn add(&mut self, name: &str, object_store: ObjectStore) {
        self.stores.insert(name.to_lowercase(), object_store);
    }

    /// Finds an object store corresponding to the name.
    pub fn find(&self, name: &str) -> Option<&ObjectStore> {
        self.stores.get(&name.to_lowercase())
    }

    pub fn default_object_store(&self) -> &ObjectStore {
        &self.default_object_store
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::{create_temp_dir, TempDir};

    use super::ObjectStoreManager;
    use crate::services::Fs as Builder;
    use crate::ObjectStore;

    fn new_object_store(dir: &TempDir) -> ObjectStore {
        let store_dir = dir.path().to_str().unwrap();
        let builder = Builder::default().root(store_dir);
        ObjectStore::new(builder).unwrap().finish()
    }

    #[test]
    fn test_manager_behavior() {
        let dir = create_temp_dir("default");
        let mut manager = ObjectStoreManager::new("Default", new_object_store(&dir));

        assert!(manager.find("default").is_some());
        assert!(manager.find("Default").is_some());
        assert!(manager.find("Gcs").is_none());
        assert!(manager.find("gcs").is_none());

        let dir = create_temp_dir("default");
        manager.add("Gcs", new_object_store(&dir));

        // Should not overwrite the default object store with the new one.
        assert!(manager.find("default").is_some());
        assert!(manager.find("Gcs").is_some());
        assert!(manager.find("gcs").is_some());
    }
}
