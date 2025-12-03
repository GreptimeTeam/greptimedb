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

use object_store::{Entry, Lister, ObjectStore};
use tokio::time::sleep;
use tokio_stream::StreamExt;

/// A wrapper around ObjectStore that injects delays into operations for testing race conditions
/// and timing-related issues in GC operations.
#[derive(Clone)]
pub struct DelayedObjectStore {
    inner: ObjectStore,
    list_delay: Duration,
    delete_delay: Duration,
    list_per_file_delay: Duration,
}

impl DelayedObjectStore {
    /// Create a new DelayedObjectStore with specified delays
    pub fn new(
        inner: ObjectStore,
        list_delay: Duration,
        delete_delay: Duration,
        list_per_file_delay: Duration,
    ) -> Self {
        Self {
            inner,
            list_delay,
            delete_delay,
            list_per_file_delay,
        }
    }

    /// Create a DelayedObjectStore with only list delays (convenience method)
    pub fn with_list_delay(inner: ObjectStore, list_delay: Duration) -> Self {
        Self::new(inner, list_delay, Duration::ZERO, Duration::from_millis(10))
    }

    /// Create a DelayedObjectStore with only delete delays (convenience method)
    pub fn with_delete_delay(inner: ObjectStore, delete_delay: Duration) -> Self {
        Self::new(inner, Duration::ZERO, delete_delay, Duration::ZERO)
    }

    /// Wrap the list operation with delay injection
    pub async fn list(&self, path: &str) -> object_store::Result<Vec<Entry>> {
        // Inject delay before listing
        sleep(self.list_delay).await;

        // Perform actual listing
        let mut lister = self.inner.lister_with(path).await?;
        let mut entries = Vec::new();

        while let Some(entry) = lister.next().await {
            let entry = entry?;

            // Add per-file delay for large listings
            sleep(self.list_per_file_delay).await;

            entries.push(entry);
        }

        Ok(entries)
    }

    /// Wrap the list_with operation with delay injection
    pub fn lister_with(&self, path: &str) -> DelayedLister {
        DelayedLister::new(
            self.inner.clone(),
            path.to_string(),
            self.list_delay,
            self.list_per_file_delay,
        )
    }

    /// Wrap the delete operation with delay injection
    pub async fn delete(&self, path: &str) -> object_store::Result<()> {
        // Inject delay before deletion
        sleep(self.delete_delay).await;

        // Perform actual deletion
        self.inner.delete(path).await
    }

    /// Get the inner ObjectStore for operations that don't need delays
    pub fn inner(&self) -> &ObjectStore {
        &self.inner
    }

    /// Convert back to the inner ObjectStore
    pub fn into_inner(self) -> ObjectStore {
        self.inner
    }
}

/// A delayed lister that wraps the original lister and injects delays
pub struct DelayedLister {
    inner: ObjectStore,
    path: String,
    list_delay: Duration,
    list_per_file_delay: Duration,
    started: bool,
}

impl DelayedLister {
    fn new(
        inner: ObjectStore,
        path: String,
        list_delay: Duration,
        list_per_file_delay: Duration,
    ) -> Self {
        Self {
            inner,
            path,
            list_delay,
            list_per_file_delay,
            started: false,
        }
    }

    /// Get the next entry with delay injection
    pub async fn next(&mut self) -> object_store::Result<Option<Entry>> {
        // Inject delay on first call
        if !self.started {
            sleep(self.list_delay).await;
            self.started = true;
        }

        // Create a new lister for this call
        let mut lister = self.inner.lister_with(&self.path).await?;

        // Get the next entry
        if let Some(entry) = lister.next().await {
            let entry = entry?;

            // Add per-file delay
            sleep(self.list_per_file_delay).await;

            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Convert to a stream for easier consumption
    pub fn into_stream(self) -> impl futures::Stream<Item = object_store::Result<Entry>> {
        futures::stream::unfold(self, |mut lister| async move {
            match lister.next().await {
                Ok(Some(entry)) => Some((Ok(entry), lister)),
                Ok(None) => None,
                Err(e) => Some((Err(e), lister)),
            }
        })
    }
}

/// Extension trait to easily convert ObjectStore to DelayedObjectStore
pub trait DelayedObjectStoreExt {
    /// Wrap this ObjectStore with delays
    fn with_delays(
        self,
        list_delay: Duration,
        delete_delay: Duration,
        list_per_file_delay: Duration,
    ) -> DelayedObjectStore;
}

impl DelayedObjectStoreExt for ObjectStore {
    fn with_delays(
        self,
        list_delay: Duration,
        delete_delay: Duration,
        list_per_file_delay: Duration,
    ) -> DelayedObjectStore {
        DelayedObjectStore::new(self, list_delay, delete_delay, list_per_file_delay)
    }
}

#[cfg(test)]
mod tests {
    use object_store::services::Memory;
    use tokio::time::Instant;

    use super::*;

    #[tokio::test]
    async fn test_list_delay() {
        let memory_store = ObjectStore::new(Memory::default()).unwrap().finish();

        // Create some test files
        memory_store
            .write("test/file1.txt", "content1")
            .await
            .unwrap();
        memory_store
            .write("test/file2.txt", "content2")
            .await
            .unwrap();

        let delayed_store =
            DelayedObjectStore::with_list_delay(memory_store, Duration::from_millis(100));

        let start = Instant::now();
        let entries = delayed_store.list("test/").await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(entries.len(), 2);
        assert!(elapsed >= Duration::from_millis(100));
        assert!(elapsed >= Duration::from_millis(120)); // 100ms base + 2*10ms per file
    }

    #[tokio::test]
    async fn test_delete_delay() {
        let memory_store = ObjectStore::new(Memory::default()).unwrap().finish();

        // Create a test file
        memory_store
            .write("test/file.txt", "content")
            .await
            .unwrap();

        let delayed_store =
            DelayedObjectStore::with_delete_delay(memory_store, Duration::from_millis(50));

        let start = Instant::now();
        delayed_store.delete("test/file.txt").await.unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_lister_stream() {
        let memory_store = ObjectStore::new(Memory::default()).unwrap().finish();

        // Create some test files
        for i in 0..5 {
            let path = format!("test/file{}.txt", i);
            let content = format!("content{}", i);
            memory_store.write(&path, content).await.unwrap();
        }

        let delayed_store =
            DelayedObjectStore::with_list_delay(memory_store, Duration::from_millis(50));

        let mut lister = delayed_store.lister_with("test/");
        let mut count = 0;

        while let Some(entry) = lister.next().await.unwrap() {
            assert!(entry.path().starts_with("test/file"));
            count += 1;
        }

        assert_eq!(count, 5);
    }
}
