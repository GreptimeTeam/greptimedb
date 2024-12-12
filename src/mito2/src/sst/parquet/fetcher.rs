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

use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common_telemetry::error;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use super::helper::fetch_byte_ranges;
use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::CacheManagerRef;
use crate::error::{self, Result};
use crate::metrics::READ_STAGE_ELAPSED;
use crate::sst::file::FileId;

pub type ParquetFetchResultFut = BoxFuture<'static, Result<Vec<Bytes>>>;

pub type FetcherRef = Arc<dyn Fetcher>;

pub trait Fetcher: Send + Sync {
    fn fetch(&self, ranges: Vec<Range<u64>>) -> ParquetFetchResultFut;

    fn run(&self);
}

#[derive(Clone)]
pub struct FetcherImpl {
    inner: Arc<Mutex<FetcherPendingTasks>>,
    context: Arc<FetcherContext>,
}

pub type FetcherPendingTask = (Vec<Range<u64>>, oneshot::Sender<Result<Vec<Bytes>>>);

pub struct FetcherPendingTasks {
    tasks: Vec<FetcherPendingTask>,
}

pub struct FetcherContext {
    object_store: ObjectStore,
    cache_manager: Option<CacheManagerRef>,
    region_id: RegionId,
    file_id: FileId,
    file_path: String,
}

impl FetcherContext {
    /// Fetches data from write cache.
    /// Returns `None` if the data is not in the cache.
    async fn fetch_ranges_from_write_cache(
        &self,
        key: IndexKey,
        ranges: &[Range<u64>],
    ) -> Option<Vec<Bytes>> {
        if let Some(cache) = self.cache_manager.as_ref()?.write_cache() {
            return cache.file_cache().read_ranges(key, ranges).await;
        }
        None
    }

    /// Try to fetch data from WriteCache,
    /// if not in WriteCache, fetch data from object store directly.
    async fn fetch_range(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let key = IndexKey::new(self.region_id, self.file_id, FileType::Parquet);
        match self.fetch_ranges_from_write_cache(key, ranges).await {
            Some(data) => Ok(data),
            None => {
                // Fetch data from object store.
                let _timer = READ_STAGE_ELAPSED
                    .with_label_values(&["cache_miss_read"])
                    .start_timer();
                let data = fetch_byte_ranges(&self.file_path, self.object_store.clone(), ranges)
                    .await
                    .context(error::OpenDalSnafu)?;
                Ok(data)
            }
        }
    }
}

impl FetcherImpl {
    pub fn new(
        object_store: ObjectStore,
        cache_manager: Option<CacheManagerRef>,
        region_id: RegionId,
        file_id: FileId,
        file_path: String,
    ) -> Self {
        Self {
            context: Arc::new(FetcherContext {
                object_store,
                cache_manager,
                region_id,
                file_id,
                file_path,
            }),
            inner: Arc::new(Mutex::new(FetcherPendingTasks { tasks: vec![] })),
        }
    }
}

impl Fetcher for FetcherImpl {
    fn fetch(&self, ranges: Vec<Range<u64>>) -> ParquetFetchResultFut {
        let mut inner = self.inner.lock().unwrap();
        let (tx, rx) = oneshot::channel();
        inner.tasks.push((ranges, tx));
        Box::pin(async move { rx.await.context(error::RecvSnafu)? })
    }

    fn run(&self) {
        let pending_ranges = self
            .inner
            .lock()
            .unwrap()
            .tasks
            .drain(..)
            .collect::<Vec<_>>();
        let ctx = self.context.clone();

        tokio::spawn(async move {
            // TODO(weny): merge ranges into one request.
            for (ranges, tx) in pending_ranges {
                let data = ctx.fetch_range(&ranges).await;
                if let Err(e) = tx.send(data) {
                    error!(e; "Failed to dispatch fetched data");
                }
            }
        });
    }
}
