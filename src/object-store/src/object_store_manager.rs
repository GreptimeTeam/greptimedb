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

use snafu::ensure;

use crate::error::Result;
use crate::{error, ObjectStore};

#[derive(Clone)]
pub struct ObjectStoreManager {
    stores: HashMap<String, ObjectStore>,
    global_object_store: String,
}

impl ObjectStoreManager {
    pub fn try_new(
        stores: HashMap<String, ObjectStore>,
        global_object_store: &str,
    ) -> Result<Self> {
        ensure!(
            stores.keys().any(|key| key == global_object_store),
            error::GlobalStorageNotFoundSnafu {
                global_object_store: global_object_store.to_string()
            }
        );

        Ok(ObjectStoreManager {
            stores,
            global_object_store: global_object_store.to_string(),
        })
    }
    pub fn find(&self, name: &str) -> Option<ObjectStore> {
        self.stores.get(name).cloned()
    }
    pub fn global_object_store(&self) -> ObjectStore {
        // Safety: We've already checked in the new method whether this exists.
        self.stores.get(&self.global_object_store).cloned().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_test_util::temp_dir::{create_temp_dir, TempDir};

    use super::ObjectStoreManager;
    use crate::error::Error;
    use crate::services::Fs as Builder;
    use crate::ObjectStore;

    fn new_object_store(dir: &TempDir) -> ObjectStore {
        let store_dir = dir.path().to_str().unwrap();
        let mut builder = Builder::default();
        let _ = builder.root(store_dir);
        ObjectStore::new(builder).unwrap().finish()
    }

    #[test]
    fn test_new_returns_err_when_global_store_not_exist() {
        let dir = create_temp_dir("new");
        let object_store = new_object_store(&dir);
        let stores: HashMap<String, ObjectStore> = vec![
            ("File".to_string(), object_store.clone()),
            ("S3".to_string(), object_store.clone()),
        ]
        .into_iter()
        .collect();

        assert!(matches!(
            ObjectStoreManager::try_new(stores, "Gcs"),
            Err(Error::GlobalStorageNotFound { .. })
        ));
    }

    #[test]
    fn test_new_returns_ok() {
        let dir = create_temp_dir("new");
        let object_store = new_object_store(&dir);
        let stores: HashMap<String, ObjectStore> = vec![
            ("File".to_string(), object_store.clone()),
            ("S3".to_string(), object_store.clone()),
        ]
        .into_iter()
        .collect();
        let object_store_manager = ObjectStoreManager::try_new(stores, "File").unwrap();
        assert_eq!(object_store_manager.stores.len(), 2);
        assert!(object_store_manager.find("File").is_some());
        assert!(object_store_manager.find("S3").is_some());
        assert!(object_store_manager.find("Gcs").is_none());

        // Panic should not happen.
        object_store_manager.global_object_store();
    }
}
