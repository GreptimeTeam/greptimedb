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
use std::time::Duration;

use common_telemetry::debug;
use common_test_util::temp_dir::create_temp_dir;
use datanode::store::new_object_store;
use object_store::config::ObjectStoreConfig;
use object_store::manager::{ObjectStoreManager, ObjectStoreManagerRef};
use object_store::raw::{Access, Layer, LayeredAccess, OpDelete, OpList, RpDelete, RpList, oio};
use object_store::{ObjectStore, ObjectStoreBuilder};
use tempfile::TempDir;
use tokio::time::sleep;

use crate::test_util::{
    FileDirGuard, StorageGuard, StorageType, TempDirGuard, TestGuard, get_test_store_config,
};

/// A layer that injects delays into storage operations for testing race conditions
/// and timing-related issues in GC integration tests.
#[derive(Clone)]
pub struct DelayLayer {
    list_delay: Duration,
    delete_delay: Duration,
    list_per_file_delay: Duration,
}

impl DelayLayer {
    /// Create a new DelayLayer with specified delays for different operations
    pub fn new(
        list_delay: Duration,
        delete_delay: Duration,
        list_per_file_delay: Duration,
    ) -> Self {
        Self {
            list_delay,
            delete_delay,
            list_per_file_delay,
        }
    }

    /// Create a DelayLayer that only delays list operations
    pub fn with_list_delay(delay: Duration) -> Self {
        Self::new(delay, Duration::ZERO, Duration::ZERO)
    }

    /// Create a DelayLayer that only delays delete operations
    pub fn with_delete_delay(delay: Duration) -> Self {
        Self::new(Duration::ZERO, delay, Duration::ZERO)
    }

    /// Create a DelayLayer that only delays per-file operations during listing
    pub fn with_per_file_delay(delay: Duration) -> Self {
        Self::new(Duration::ZERO, Duration::ZERO, delay)
    }
}

impl<A: Access> Layer<A> for DelayLayer {
    type LayeredAccess = DelayedAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        DelayedAccessor {
            inner,
            list_delay: self.list_delay,
            delete_delay: self.delete_delay,
            list_per_file_delay: self.list_per_file_delay,
        }
    }
}

/// The delayed accessor that wraps the inner accessor and injects delays
#[derive(Debug)]
pub struct DelayedAccessor<A> {
    inner: A,
    list_delay: Duration,
    delete_delay: Duration,
    list_per_file_delay: Duration,
}

impl<A: Access> LayeredAccess for DelayedAccessor<A> {
    type Inner = A;
    type Lister = DelayedLister<A::Lister>;
    type Deleter = DelayedDeleter<A::Deleter>;
    type Reader = A::Reader;
    type Writer = A::Writer;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn read(
        &self,
        path: &str,
        args: object_store::raw::OpRead,
    ) -> impl Future<Output = object_store::Result<(object_store::raw::RpRead, Self::Reader)>>
    + object_store::raw::MaybeSend {
        self.inner.read(path, args)
    }

    fn write(
        &self,
        path: &str,
        args: object_store::raw::OpWrite,
    ) -> impl Future<Output = object_store::Result<(object_store::raw::RpWrite, Self::Writer)>>
    + object_store::raw::MaybeSend {
        self.inner.write(path, args)
    }

    async fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister), object_store::Error> {
        // Inject delay before listing operation
        debug!("Delaying list operation by {:?}", self.list_delay);
        sleep(self.list_delay).await;

        let (rp_list, lister) = self.inner.list(path, args).await?;
        Ok((
            rp_list,
            DelayedLister {
                inner: lister,
                per_file_delay: self.list_per_file_delay,
            },
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter), object_store::Error> {
        // Inject delay before delete operations
        sleep(self.delete_delay).await;

        let (rp_delete, deleter) = self.inner.delete().await?;
        Ok((
            rp_delete,
            DelayedDeleter {
                inner: deleter,
                delete_delay: self.delete_delay,
            },
        ))
    }
}

/// A lister that adds per-file delays during listing operations
pub struct DelayedLister<L> {
    inner: L,
    per_file_delay: Duration,
}

impl<L: oio::List> oio::List for DelayedLister<L> {
    async fn next(&mut self) -> Result<Option<oio::Entry>, object_store::Error> {
        // Add per-file delay before returning each entry
        sleep(self.per_file_delay).await;
        self.inner.next().await
    }
}

/// A deleter that adds delays during delete operations
pub struct DelayedDeleter<D> {
    inner: D,
    delete_delay: Duration,
}

impl<D: oio::Delete> oio::Delete for DelayedDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<(), object_store::Error> {
        // Delete operations are typically batched, so we delay in flush
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize, object_store::Error> {
        // Inject delay before actual deletion (when batch is flushed)
        sleep(self.delete_delay).await;
        self.inner.flush().await
    }
}

/// Helper function to create an file system object store with delay layer for testing
pub fn create_fs_test_store_with_delays(
    list_delay: Duration,
    delete_delay: Duration,
    list_per_file_delay: Duration,
) -> (object_store::ObjectStore, TempDir) {
    use object_store::services::Fs;

    let temp_dir = common_test_util::temp_dir::create_temp_dir("delayed_store");
    let builder = Fs::default().root(temp_dir.path().to_str().unwrap());

    let store = object_store::ObjectStore::new(builder)
        .unwrap()
        .layer(DelayLayer::new(
            list_delay,
            delete_delay,
            list_per_file_delay,
        ))
        .finish();

    (store, temp_dir)
}

/// Helper function to create an object store with delay layer using the same logic as new_object_store
pub async fn new_object_store_with_delays(
    store_type: &StorageType,
    data_home: &str,
    delay_layer: DelayLayer,
) -> Result<(ObjectStore, TempDirGuard), Box<dyn std::error::Error + Send + Sync>> {
    // Get the test store configuration based on storage type
    let (store_config, temp_dir_guard) = crate::test_util::get_test_store_config(store_type);

    // First create the base object store using the same logic as datanode::store::new_object_store
    let base_store = new_object_store(store_config, data_home).await?;

    // Apply the delay layer to the base store
    let delayed_store = base_store.layer(delay_layer);

    Ok((delayed_store, temp_dir_guard))
}

/// Helper function to create an object store manager with delay layer
pub async fn build_object_store_manager_with_delays(
    store_type: &StorageType,
    data_home: &str,
    delay_layer: DelayLayer,
) -> Result<(ObjectStoreManagerRef, TempDirGuard), Box<dyn std::error::Error + Send + Sync>> {
    let (delayed_store, temp_dir) =
        new_object_store_with_delays(&store_type, data_home, delay_layer).await?;

    // Create an object store manager with the delayed store
    let manager = Arc::new(ObjectStoreManager::new("default", delayed_store));

    Ok((manager, temp_dir))
}

/// Convenience function to create an object store manager with delay layer for testing
/// Uses default filesystem configuration
pub async fn create_test_object_store_manager_with_delays(
    store_type: &StorageType,
    test_name: &str,
    list_delay: Duration,
    delete_delay: Duration,
    list_per_file_delay: Duration,
) -> Result<(ObjectStoreManagerRef, TestGuard), Box<dyn std::error::Error + Send + Sync>> {
    let data_home_dir = create_temp_dir(&format!("gt_home_{}", &test_name));
    let data_home = data_home_dir.path().to_str().unwrap().to_string();
    let delay_layer = DelayLayer::new(list_delay, delete_delay, list_per_file_delay);
    let (mgr, dir_guard) =
        build_object_store_manager_with_delays(&store_type, &data_home, delay_layer).await?;
    Ok((
        mgr,
        TestGuard {
            home_guard: FileDirGuard::new(data_home_dir),
            storage_guards: vec![StorageGuard(dir_guard)],
        },
    ))
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_delay_layer_basic() {
        let (store, _temp_dir) = create_fs_test_store_with_delays(
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_millis(10),
        );

        // Test write operation (should not be delayed)
        let start = std::time::Instant::now();
        store.write("test.txt", "hello world").await.unwrap();
        let write_duration = start.elapsed();
        assert!(write_duration < Duration::from_millis(50)); // Should be fast

        // Test list operation (should be delayed)
        let start = std::time::Instant::now();
        let mut lister = store.lister("/").await.unwrap();
        let _ = lister.next().await.unwrap(); // This should trigger per-file delay
        let list_duration = start.elapsed();
        assert!(list_duration >= Duration::from_millis(100)); // Should have list delay

        // Test delete operation (should be delayed)
        let start = std::time::Instant::now();
        store.delete("test.txt").await.unwrap();
        let delete_duration = start.elapsed();
        assert!(delete_duration >= Duration::from_millis(50)); // Should have delete delay
    }

    #[tokio::test]
    async fn test_delay_layer_with_zero_delays() {
        let (store, _temp_dir) =
            create_fs_test_store_with_delays(Duration::ZERO, Duration::ZERO, Duration::ZERO);

        // All operations should be fast
        let start = std::time::Instant::now();
        store.write("test.txt", "hello world").await.unwrap();
        let _ = store.lister("/").await.unwrap();
        store.delete("test.txt").await.unwrap();
        let total_duration = start.elapsed();

        assert!(total_duration < Duration::from_millis(100)); // Should be very fast
    }

    #[tokio::test]
    async fn test_delay_layer_convenience_constructors() {
        // Test list-only delay
        let (store, _temp_dir) = create_fs_test_store_with_delays(
            Duration::from_millis(200),
            Duration::ZERO,
            Duration::ZERO,
        );

        let start = std::time::Instant::now();
        let _ = store.lister("/").await.unwrap();
        let duration = start.elapsed();
        assert!(duration >= Duration::from_millis(200));

        // Test delete-only delay
        let (store, _temp_dir) = create_fs_test_store_with_delays(
            Duration::ZERO,
            Duration::from_millis(150),
            Duration::ZERO,
        );

        store.write("test.txt", "data").await.unwrap();
        let start = std::time::Instant::now();
        store.delete("test.txt").await.unwrap();
        let duration = start.elapsed();
        assert!(duration >= Duration::from_millis(150));
    }
}
