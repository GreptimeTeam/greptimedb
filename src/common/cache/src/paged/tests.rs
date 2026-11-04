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

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use common_base::range_read::Metadata;
use tokio::sync::Semaphore;

use super::*;

#[derive(Default)]
struct Reader {
    reads: Mutex<Vec<Range<u64>>>,
    batches: Mutex<Vec<Vec<Range<u64>>>>,
    gate: Option<Semaphore>,
    error: Option<io::ErrorKind>,
}

impl Reader {
    fn gated() -> Self {
        Self {
            gate: Some(Semaphore::new(0)),
            ..Default::default()
        }
    }

    fn count(&self) -> usize {
        self.reads.lock().unwrap().len()
    }

    fn release(&self) {
        self.gate.as_ref().unwrap().add_permits(64);
    }
}

#[async_trait]
impl RangeReader for Reader {
    async fn metadata(&self) -> io::Result<Metadata> {
        Ok(Metadata { content_length: 16 })
    }

    async fn read_vec(&self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        self.batches.lock().unwrap().push(ranges.to_vec());
        future::try_join_all(ranges.iter().map(|range| self.read(range.clone()))).await
    }

    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        self.reads.lock().unwrap().push(range.clone());
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        if let Some(kind) = self.error {
            return Err(io::Error::new(kind, "backend failed"));
        }
        Ok(Bytes::from(range.map(|n| n as u8).collect::<Vec<_>>()))
    }
}

struct Observation {
    loaded: AtomicUsize,
    bytes: crate::metrics::CacheSizeMetrics,
}

impl Observation {
    fn bytes(&self) -> i64 {
        use prometheus::core::Collector;
        self.bytes.collect()[0].get_metric()[0]
            .get_gauge()
            .as_ref()
            .unwrap()
            .value() as i64
    }
}

fn cache() -> (PagedCache<u64>, Arc<Observation>) {
    let observation = Arc::new(Observation {
        loaded: AtomicUsize::new(0),
        bytes: crate::metrics::CacheSizeMetrics::new(
            prometheus::Opts::new("test_page_bytes", "test"),
            &[],
        )
        .unwrap(),
    });
    let events = observation.clone();
    let observer = PageCacheObserver {
        bytes: observation.bytes.bind(&[]),
        capacity: crate::metrics::CacheSizeMetrics::new(
            prometheus::Opts::new("test_page_capacity_bytes", "test"),
            &[],
        )
        .unwrap()
        .bind(&[]),
        loads: None,
        events: Arc::new(move |event| {
            if let PageEvent::Loaded { .. } = event {
                events.loaded.fetch_add(1, Ordering::Relaxed);
            }
        }),
    };
    (
        PagedCache::new("pages", 1024, 4, |_, bytes| bytes.len() as u32, observer),
        observation,
    )
}

async fn start_read<F: Future>(mut future: Pin<&mut F>, reader: &Reader, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(future.as_mut()).is_pending());
            if reader.count() >= count {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_partial_hits_overlaps_and_eof_preserve_order() {
    let (cache, observation) = cache();
    let reader = Reader::default();
    let (first, stats) = cache.fetch(1, 10, &[2..6, 4..5], &reader).await.unwrap();
    assert_eq!(
        first,
        vec![Bytes::from_static(&[2, 3, 4, 5]), Bytes::from_static(&[4])]
    );
    assert_eq!((stats.cache_hit, stats.cache_miss), (0, 2));
    assert_eq!(
        (
            stats.requested_bytes,
            stats.unique_requested_bytes,
            stats.aligned_bytes
        ),
        (5, 4, 8)
    );
    let (second, stats) = cache
        .fetch(1, 10, &[6..10, 1..3, 10..10], &reader)
        .await
        .unwrap();
    assert_eq!(
        second,
        vec![
            Bytes::from_static(&[6, 7, 8, 9]),
            Bytes::from_static(&[1, 2]),
            Bytes::new()
        ]
    );
    assert_eq!((stats.cache_hit, stats.cache_miss), (2, 1));
    assert_eq!((stats.cached_bytes, stats.cached_fragments), (4, 2));
    assert_eq!(observation.loaded.load(Ordering::Relaxed), 3);
    assert_eq!(
        cache
            .known_file_size(&1, std::slice::from_ref(&(8..9)))
            .await,
        Some(10)
    );
    assert_eq!(*reader.reads.lock().unwrap(), vec![0..4, 4..8, 8..10]);
    cache.run_pending_tasks().await;
    assert_eq!(observation.bytes(), 10);
    cache.invalidate_if(|file| *file == 1).unwrap();
    cache.run_pending_tasks().await;
    assert!(!cache.contains(&1, 0).await);
    assert_eq!(observation.bytes(), 0);
    // Returned values remain valid after invalidation, while cache accounting is released.
    assert_eq!(&second[0][..], &[6, 7, 8, 9]);
}

#[tokio::test]
async fn test_overlapping_fetches_share_pages_and_only_read_missing_bytes() {
    let (cache, observation) = cache();
    let reader = Reader::gated();
    let first_range = 0..8;
    let first_ranges = std::slice::from_ref(&first_range);
    let second_range = 4..12;
    let second_ranges = std::slice::from_ref(&second_range);
    let mut first = Box::pin(cache.fetch(1, 16, first_ranges, &reader));
    start_read(first.as_mut(), &reader, 2).await;
    let mut second = Box::pin(cache.fetch(1, 16, second_ranges, &reader));
    start_read(second.as_mut(), &reader, 3).await;
    reader.release();
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first, second)
    })
    .await
    .unwrap();
    assert_eq!(&first.unwrap().0[0][..], &(0..8).collect::<Vec<u8>>());
    assert_eq!(&second.unwrap().0[0][..], &(4..12).collect::<Vec<u8>>());
    assert_eq!(*reader.reads.lock().unwrap(), vec![0..4, 4..8, 8..12]);
    assert_eq!(observation.loaded.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn test_cancelling_owner_allows_waiter_to_finish() {
    let (cache, observation) = cache();
    let reader = Reader::gated();
    let range = 0..4;
    let ranges = std::slice::from_ref(&range);
    let mut owner = Box::pin(cache.fetch(1, 16, ranges, &reader));
    start_read(owner.as_mut(), &reader, 1).await;
    let mut waiter = Box::pin(cache.fetch(1, 16, ranges, &reader));
    assert!(futures::poll!(&mut waiter).is_pending());
    assert_eq!(reader.count(), 1);
    drop(owner);
    reader.release();
    let result = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&result.0[0][..], &[0, 1, 2, 3]);
    assert_eq!(reader.count(), 2);
    assert_eq!(observation.loaded.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_cancelling_waiter_does_not_cancel_owner() {
    let (cache, _) = cache();
    let reader = Reader::gated();
    let range = 0..4;
    let ranges = std::slice::from_ref(&range);
    let mut owner = Box::pin(cache.fetch(1, 16, ranges, &reader));
    start_read(owner.as_mut(), &reader, 1).await;
    let mut waiter = Box::pin(cache.fetch(1, 16, ranges, &reader));
    assert!(futures::poll!(&mut waiter).is_pending());
    drop(waiter);
    reader.release();
    let result = tokio::time::timeout(Duration::from_secs(5), owner)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&result.0[0][..], &[0, 1, 2, 3]);
    assert_eq!(reader.count(), 1);
}

#[tokio::test]
async fn test_backend_error_is_shared_and_not_cached() {
    let (cache, observation) = cache();
    let reader = Reader {
        error: Some(io::ErrorKind::BrokenPipe),
        ..Reader::gated()
    };
    let range = 0..4;
    let ranges = std::slice::from_ref(&range);
    let mut owner = Box::pin(cache.fetch(1, 16, ranges, &reader));
    start_read(owner.as_mut(), &reader, 1).await;
    let mut waiter = Box::pin(cache.fetch(1, 16, ranges, &reader));
    assert!(futures::poll!(&mut waiter).is_pending());
    reader.release();
    let (owner, waiter) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(owner, waiter)
    })
    .await
    .unwrap();
    for result in [owner, waiter] {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("backend failed"));
    }
    assert_eq!(reader.count(), 1);
    assert_eq!(observation.loaded.load(Ordering::Relaxed), 0);
    // The failed load was not cached.
    let healthy = Reader::default();
    assert!(cache.fetch(1, 16, ranges, &healthy).await.is_ok());
    assert_eq!(healthy.count(), 1);
}

#[tokio::test]
async fn test_completed_page_rejects_conflicting_file_size() {
    let (cache, _) = cache();
    let reader = Reader::default();
    let range = 0..4;
    let ranges = std::slice::from_ref(&range);
    cache.fetch(1, 16, ranges, &reader).await.unwrap();
    assert!(cache.fetch(1, 12, ranges, &reader).await.is_err());
    assert_eq!(reader.count(), 1);
    // A different identity remains independent.
    assert!(cache.fetch(2, 12, ranges, &reader).await.is_ok());
}

#[tokio::test]
async fn test_shared_load_rejects_conflicting_file_size() {
    let (cache, _) = cache();
    let reader = Reader::gated();
    let range = 0..4;
    let ranges = std::slice::from_ref(&range);
    let mut owner = Box::pin(cache.fetch(1, 16, ranges, &reader));
    start_read(owner.as_mut(), &reader, 1).await;
    let mut conflicting = Box::pin(cache.fetch(1, 12, ranges, &reader));
    assert!(futures::poll!(&mut conflicting).is_pending());
    reader.release();
    let (owner, conflicting) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(owner, conflicting)
    })
    .await
    .unwrap();
    assert!(owner.is_ok());
    assert!(conflicting.is_err());
    assert_eq!(reader.count(), 1);
}

#[tokio::test]
async fn test_empty_and_invalid_requests_do_not_issue_reads() {
    let (cache, _) = cache();
    let reader = Reader::default();
    assert!(cache.fetch(1, 0, &[], &reader).await.unwrap().0.is_empty());
    assert_eq!(
        cache.fetch(1, 0, &[0..0, 0..0], &reader).await.unwrap().0,
        vec![Bytes::new(), Bytes::new()]
    );
    for ranges in [vec![Range { start: 2, end: 1 }], vec![0..17, 0..0]] {
        assert!(cache.fetch(1, 16, &ranges, &reader).await.is_err());
    }
    assert_eq!(reader.count(), 0);
}

// Keep the Send contract visible before a caller erases this future in async-trait.
fn send_fetch<'a, K, R>(
    cache: &'a PagedCache<K>,
    file: K,
    ranges: &'a [Range<u64>],
    reader: &'a R,
) -> impl Future<Output = Result<(Vec<Bytes>, PageFetchStats), Arc<io::Error>>> + Send + 'a
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    R: RangeReader + ?Sized,
{
    cache.fetch(file, 16, ranges, reader)
}

#[tokio::test]
async fn test_generic_page_fetch_has_a_send_future() {
    let (cache, _) = cache();
    let reader = Reader::default();
    let range = 1..7;
    let ranges = std::slice::from_ref(&range);
    let future: futures::future::BoxFuture<'_, _> =
        Box::pin(send_fetch(&cache, 1, ranges, &reader));
    let (bytes, _) = future.await.unwrap();
    assert_eq!(bytes, vec![Bytes::from_static(&[1, 2, 3, 4, 5, 6])]);
}

#[tokio::test]
async fn test_direct_page_rejects_wrong_lengths_without_caching() {
    struct FixedReader(usize);
    #[async_trait]
    impl RangeReader for FixedReader {
        async fn metadata(&self) -> io::Result<Metadata> {
            unreachable!("fetch already has the immutable file size")
        }
        async fn read(&self, _: Range<u64>) -> io::Result<Bytes> {
            Ok(Bytes::from(vec![7; self.0]))
        }
    }
    for response_size in [3, 5] {
        let (cache, observation) = cache();
        let range = 0..4;
        let ranges = std::slice::from_ref(&range);
        let error = cache
            .fetch(1, 8, ranges, &FixedReader(response_size))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(observation.loaded.load(Ordering::Relaxed), 0);
        assert!(!cache.contains(&1, 0).await);
        let reader = Reader::default();
        assert!(cache.fetch(1, 8, ranges, &reader).await.is_ok());
        assert_eq!(reader.count(), 1);
    }
}

#[tokio::test]
async fn test_direct_page_detaches_shared_backing_buffer() {
    struct SliceReader(Bytes);
    #[async_trait]
    impl RangeReader for SliceReader {
        async fn metadata(&self) -> io::Result<Metadata> {
            unreachable!("fetch already has the immutable file size")
        }
        async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
            Ok(self.0.slice(range.start as usize..range.end as usize))
        }
    }
    let (cache, observation) = cache();
    let reader = SliceReader(Bytes::from(vec![7; 1024]));
    let range = 0..4;
    let (output, _) = cache
        .fetch(1, 1024, std::slice::from_ref(&range), &reader)
        .await
        .unwrap();
    assert_eq!(output[0].as_ref(), [7; 4]);
    assert_ne!(output[0].as_ptr(), reader.0.as_ptr());
    cache.run_pending_tasks().await;
    assert_eq!(observation.bytes(), 4);
}

#[tokio::test]
async fn test_direct_page_larger_than_eight_mib_is_not_split() {
    let (base, _) = cache();
    let page_size = 9 * 1024 * 1024;
    let cache = PagedCache::new(
        "large_pages",
        page_size * 2,
        page_size,
        |_: &(u64, u64), bytes: &Bytes| bytes.len() as u32,
        base.observer.clone(),
    );
    let reader = Reader::default();
    let range = 0..1;
    let (output, _) = cache
        .fetch(1, page_size, std::slice::from_ref(&range), &reader)
        .await
        .unwrap();
    assert_eq!(output[0].as_ref(), [0]);
    assert_eq!(
        reader.reads.lock().unwrap().as_slice(),
        std::slice::from_ref(&(0..page_size))
    );
}

#[test]
fn test_unique_requested_bytes() {
    for (ranges, expected) in [
        (vec![], 0),
        (vec![4..4, 0..0], 0),
        (vec![8..12, 2..6, 4..10, 2..6], 10),
        (vec![0..4, 4..8, 12..16], 12),
        (vec![0..u64::MAX, 1..2], u64::MAX),
    ] {
        assert_eq!(unique_requested_bytes(&ranges), expected);
    }
}

#[tokio::test]
async fn test_missing_pages_use_batch_reads() {
    let (cache, _) = cache();
    let reader = Reader::default();
    let range = 0..512;
    let ranges = std::slice::from_ref(&range);
    let (bytes, _) = cache.fetch(1, 512, ranges, &reader).await.unwrap();
    assert_eq!(
        bytes[0],
        Bytes::from((0..512).map(|n| n as u8).collect::<Vec<_>>())
    );
    assert_eq!(
        *reader.batches.lock().unwrap(),
        vec![(0..128).map(|n| n * 4..n * 4 + 4).collect::<Vec<_>>()]
    );
    cache.fetch(1, 512, ranges, &reader).await.unwrap();
    assert_eq!(reader.batches.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn test_shared_error_cancels_other_pending_batch() {
    let (cache, _) = cache();
    let failed = Reader {
        error: Some(io::ErrorKind::BrokenPipe),
        ..Reader::gated()
    };
    let blocked = Reader::gated();
    let owner_range = 0..4;
    let waiter_range = 0..8;
    let mut owner = Box::pin(cache.fetch(1, 16, std::slice::from_ref(&owner_range), &failed));
    start_read(owner.as_mut(), &failed, 1).await;
    let mut waiter = Box::pin(cache.fetch(1, 16, std::slice::from_ref(&waiter_range), &blocked));
    start_read(waiter.as_mut(), &blocked, 1).await;
    failed.release();
    assert_eq!(owner.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    // The unrelated page stays blocked; the shared error must cancel its I/O.
    let error = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    let healthy = Reader::default();
    assert!(
        cache
            .fetch(1, 16, std::slice::from_ref(&waiter_range), &healthy)
            .await
            .is_ok()
    );
}
