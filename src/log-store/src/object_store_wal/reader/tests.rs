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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_wal::config::object_store::ObjectStoreWalConfig;
use object_store::services::Memory;
use store_api::logstore::LogStore;

use super::*;
use crate::object_store_wal::ObjectStoreLogStore;
use crate::object_store_wal::format::{ChainLink, Header, Record, encode_object};
use crate::object_store_wal::io::ListedObject;

const PREFIX: &str = "wal/datanodes/1/epochs/0";

fn region() -> RegionId {
    RegionId::new(1, 1)
}

fn record(seq: u64, position: u64) -> Record {
    Record {
        region_id: region(),
        entry_id: entry_id(seq, position),
        payload: Bytes::from(format!("{seq}/{position}")),
    }
}

async fn put(
    io: &ObjectStoreIo,
    seq: u64,
    epoch: u64,
    prev: Option<(u64, u64)>,
    boundary: u64,
    records: &[Record],
) {
    let object = encode_object(
        Header {
            object_seq: seq,
            epoch,
            prev: prev.map(|(object_seq, epoch)| ChainLink { object_seq, epoch }),
            reclaim_boundary: boundary,
        },
        records,
    )
    .unwrap();
    io.put_if_absent(seq, object.bytes).await.unwrap();
}

/// Implements only read capabilities. Reads can be parked to measure concurrent
/// index loads and cancellation deterministically.
struct ObservedIo {
    io: ObjectStoreIo,
    gets: Mutex<BTreeMap<u64, usize>>,
    lists: Mutex<Vec<u64>>,
    gate: Option<Arc<Semaphore>>,
    active: AtomicUsize,
    peak: AtomicUsize,
}

struct ActiveRead<'a>(&'a AtomicUsize);
impl Drop for ActiveRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ObservedIo {
    async fn begin_read(&self, seq: u64) -> ActiveRead<'_> {
        *self.gets.lock().unwrap().entry(seq).or_default() += 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let guard = ActiveRead(&self.active);
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        guard
    }
}

#[async_trait::async_trait]
impl WalObjectRead for ObservedIo {
    async fn get(&self, seq: u64) -> Result<Bytes> {
        let _guard = self.begin_read(seq).await;
        self.io.get(seq).await
    }

    async fn get_range(&self, seq: u64, offset: u64, len: u64) -> Result<Bytes> {
        let _guard = self.begin_read(seq).await;
        self.io.get_range(seq, offset, len).await
    }

    async fn list(&self) -> Result<Vec<ListedObject>> {
        self.lists.lock().unwrap().push(0);
        self.io.list().await
    }

    async fn list_from(&self, seq: u64) -> Result<Vec<ListedObject>> {
        self.lists.lock().unwrap().push(seq);
        self.io.list_from(seq).await
    }

    fn object_path(&self, seq: u64) -> String {
        self.io.object_path(seq)
    }

    fn storage(&self) -> String {
        self.io.storage()
    }

    fn prefix(&self) -> &str {
        self.io.prefix()
    }
}

fn observed(io: ObjectStoreIo, gate: Option<Arc<Semaphore>>) -> Arc<ObservedIo> {
    Arc::new(ObservedIo {
        io,
        gets: Mutex::new(BTreeMap::new()),
        lists: Mutex::new(Vec::new()),
        gate,
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    })
}

fn reader(io: Arc<dyn WalObjectRead>, budget: Arc<Semaphore>) -> Arc<ObjectStoreWalReader> {
    Arc::new(ObjectStoreWalReader {
        io,
        indexes: CachedObject::cache(),
        footer_budget: budget,
        prefetch_budget: Arc::new(Semaphore::new(prefetch::READ_PREFETCH_BYTES)),
    })
}

fn fixture() -> (
    ObjectStore,
    ObjectStoreIo,
    Arc<ObservedIo>,
    Arc<ObjectStoreWalReader>,
) {
    let store = ObjectStore::new(Memory::default()).unwrap();
    let io = ObjectStoreIo::new(store.clone(), PREFIX).unwrap();
    let observed = observed(io.clone(), None);
    let reader = reader(
        observed.clone(),
        Arc::new(Semaphore::new(FOOTER_CONCURRENCY)),
    );
    (store, io, observed, reader)
}

async fn collect(
    reader: &ObjectStoreWalReader,
    after: u64,
    upper: WalReadUpperBound,
) -> Result<Vec<Entry>> {
    Ok(reader
        .read(region(), after, upper)
        .await?
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect())
}

fn expected(records: &[Record]) -> Vec<Entry> {
    records
        .iter()
        .map(|record| {
            Entry::Naive(NaiveEntry {
                provider: Provider::object_store_provider(region(), PREFIX.to_string()),
                region_id: region(),
                entry_id: record.entry_id,
                data: record.payload.to_vec(),
            })
        })
        .collect()
}

#[tokio::test]
async fn test_reader_reads_existing_store_without_modifying_it() {
    let store = ObjectStore::new(Memory::default()).unwrap();
    let writer =
        ObjectStoreLogStore::try_new(store.clone(), &ObjectStoreWalConfig::default(), 1, 0)
            .await
            .unwrap();
    let provider = Provider::object_store_provider(region(), PREFIX.to_string());
    let entry = writer
        .entry(b"durable".to_vec(), 0, region(), &provider)
        .unwrap();
    writer.append_batch(vec![entry]).await.unwrap();
    let last = writer.latest_entry_id(&provider).unwrap();
    let io = ObjectStoreIo::new(store.clone(), PREFIX).unwrap();
    let before = io.list().await.unwrap();
    let registry = ObjectStoreWalReaders::new(store);
    let reader = registry
        .reader(&NodeWalPrefix::try_new(PREFIX).unwrap())
        .unwrap();
    assert!(Arc::ptr_eq(
        &reader,
        &registry
            .reader(&NodeWalPrefix::try_new(&format!("{PREFIX}/")).unwrap())
            .unwrap()
    ));
    for upper in [
        WalReadUpperBound::LastEntry(last),
        WalReadUpperBound::Listed(reader.list_upper_bound(region()).await.unwrap()),
    ] {
        assert_eq!(
            collect(&reader, 0, upper).await.unwrap(),
            vec![Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id: region(),
                entry_id: last,
                data: b"durable".to_vec(),
            })]
        );
    }
    assert_eq!(before, io.list().await.unwrap());
    writer.stop().await.unwrap();
}

#[tokio::test]
async fn test_bounds_region_filtering_and_list_only_readiness() {
    let (_, io, observed, reader) = fixture();
    put(&io, 0, 1, None, 0, &[]).await;
    let wanted = vec![record(1, 1), record(1, 2), record(1, 3)];
    let mut mixed = wanted.clone();
    mixed.push(Record {
        region_id: RegionId::new(1, 2),
        entry_id: entry_id(1, 1),
        payload: Bytes::from_static(b"other"),
    });
    put(&io, 1, 1, Some((0, 1)), 0, &mixed).await;
    let bound = reader.list_upper_bound(region()).await.unwrap();
    assert_eq!(bound, entry_id(1, POSITION_LIMIT - 1));
    assert!(observed.gets.lock().unwrap().is_empty());
    for upper in [
        WalReadUpperBound::LastEntry(entry_id(1, 2)),
        WalReadUpperBound::Listed(entry_id(1, 2)),
    ] {
        assert_eq!(
            collect(&reader, entry_id(1, 1), upper).await.unwrap(),
            expected(&wanted[1..2])
        );
    }
    assert!(
        reader
            .read(region(), bound, WalReadUpperBound::Listed(bound))
            .await
            .is_err()
    );
    assert!(
        reader
            .read(region(), u64::MAX, WalReadUpperBound::Listed(bound))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_last_entry_anchors_its_chain_and_skips_history_before_after() {
    let (_, io, observed, reader) = fixture();
    // The predecessor of object 5 is below after's sequence; it may be absent.
    put(&io, 5, 1, Some((2, 1)), 0, &[record(5, 1)]).await;
    put(&io, 7, 1, Some((5, 1)), 0, &[record(7, 1)]).await;
    // A newer, unrelated chain must not select away the known L.
    put(&io, 8, 9, None, 0, &[record(8, 1)]).await;
    assert_eq!(
        collect(
            &reader,
            entry_id(4, 1),
            WalReadUpperBound::LastEntry(entry_id(7, 1))
        )
        .await
        .unwrap(),
        expected(&[record(5, 1), record(7, 1)])
    );
    assert_eq!(*observed.lists.lock().unwrap(), vec![4]);
}

#[tokio::test]
async fn test_last_entry_requires_actual_entry_and_complete_chain() {
    for missing in [0, 1, 2] {
        let (_, io, _, reader) = fixture();
        put(&io, 0, 1, None, 0, &[]).await;
        if missing != 1 {
            put(&io, 1, 1, Some((0, 1)), 0, &[record(1, 1)]).await;
        }
        if missing != 2 {
            put(&io, 2, 1, Some((1, 1)), 0, &[record(2, 1)]).await;
        }
        let last = entry_id(2, if missing == 0 { 2 } else { 1 });
        assert!(
            collect(&reader, 0, WalReadUpperBound::LastEntry(last))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn test_last_entry_includes_after_object_and_checks_link_epoch() {
    let (_, io, _, reader) = fixture();
    put(&io, 3, 1, Some((1, 1)), 0, &[record(3, 1), record(3, 2)]).await;
    assert_eq!(
        collect(
            &reader,
            entry_id(3, 1),
            WalReadUpperBound::LastEntry(entry_id(3, 2))
        )
        .await
        .unwrap(),
        expected(&[record(3, 2)])
    );
    put(&io, 4, 1, Some((3, 2)), 0, &[record(4, 1)]).await;
    assert!(
        collect(
            &reader,
            entry_id(3, 1),
            WalReadUpperBound::LastEntry(entry_id(4, 1))
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn test_listed_range_rejects_fallback_even_for_other_regions() {
    let (_, io, _, reader) = fixture();
    put(&io, 0, 1, None, 0, &[]).await;
    put(&io, 1, 1, Some((0, 1)), 0, &[record(1, 1)]).await;
    // The newer epoch has no entry of the requested region and its predecessor
    // is missing. Filtering by region before comparing epochs would hide it.
    put(&io, 5, 4, Some((3, 4)), 0, &[]).await;
    let error = collect(
        &reader,
        0,
        WalReadUpperBound::Listed(entry_id(5, POSITION_LIMIT - 1)),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("fall back"), "{error}");
    // Known-L recovery does not require choosing the newest chain.
    assert_eq!(
        collect(&reader, 0, WalReadUpperBound::LastEntry(entry_id(1, 1)))
            .await
            .unwrap(),
        expected(&[record(1, 1)])
    );
}

#[tokio::test]
async fn test_empty_and_missing_history_fail_but_empty_region_tail_is_valid() {
    let (_, io, _, reader) = fixture();
    assert!(reader.list_upper_bound(region()).await.is_err());
    assert!(
        collect(&reader, 0, WalReadUpperBound::Listed(entry_id(1, 1)))
            .await
            .is_err()
    );
    put(&io, 0, 1, None, 0, &[]).await;
    put(&io, 1, 1, Some((0, 1)), 0, &[record(1, 1)]).await;
    assert!(
        collect(
            &reader,
            entry_id(4, 1),
            WalReadUpperBound::Listed(entry_id(5, 1))
        )
        .await
        .is_err()
    );
    // No entry above after, and the LIST bound need not itself be an entry.
    assert!(
        collect(
            &reader,
            entry_id(1, 1),
            WalReadUpperBound::Listed(entry_id(1, POSITION_LIMIT - 1))
        )
        .await
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn test_refresh_discovers_new_objects_and_reuses_only_present_indexes() {
    let (_, io, observed, reader) = fixture();
    put(&io, 0, 1, None, 0, &[]).await;
    put(&io, 1, 1, Some((0, 1)), 0, &[record(1, 1)]).await;
    let upper = WalReadUpperBound::Listed(reader.list_upper_bound(region()).await.unwrap());
    assert_eq!(
        collect(&reader, 0, upper).await.unwrap(),
        expected(&[record(1, 1)])
    );
    let first = observed.gets.lock().unwrap().clone();
    put(&io, 2, 1, Some((1, 1)), 0, &[record(2, 1)]).await;
    assert_eq!(
        collect(
            &reader,
            entry_id(1, 1),
            WalReadUpperBound::Listed(entry_id(2, 1))
        )
        .await
        .unwrap(),
        expected(&[record(2, 1)])
    );
    // Neither the old index nor its data was fetched again.
    assert_eq!(observed.gets.lock().unwrap()[&0], first[&0]);
    assert_eq!(observed.gets.lock().unwrap()[&1], first[&1]);
    assert_eq!(*observed.lists.lock().unwrap(), vec![0, 0, 0]);
    io.delete(1).await.unwrap();
    assert!(
        collect(&reader, 0, WalReadUpperBound::LastEntry(entry_id(2, 1)))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_missing_and_corrupt_data_fail_after_prepare() {
    for corrupt in [false, true] {
        let (store, io, _, reader) = fixture();
        put(&io, 0, 1, None, 0, &[]).await;
        put(&io, 1, 1, Some((0, 1)), 0, &[record(1, 1)]).await;
        let stream = reader
            .read(region(), 0, WalReadUpperBound::LastEntry(entry_id(1, 1)))
            .await
            .unwrap();
        if corrupt {
            let mut bytes = io.get(1).await.unwrap().to_vec();
            bytes[crate::object_store_wal::format::HEADER_LEN + 12] ^= 1;
            store.write(&io.object_path(1), bytes).await.unwrap();
        } else {
            io.delete(1).await.unwrap();
        }
        assert!(stream.try_collect::<Vec<_>>().await.is_err());
    }
}

#[tokio::test]
async fn test_reclaim_boundary_allows_missing_old_history() {
    let (_, io, _, reader) = fixture();
    put(&io, 5, 1, Some((2, 1)), 5, &[record(5, 1)]).await;
    for upper in [
        WalReadUpperBound::LastEntry(entry_id(5, 1)),
        WalReadUpperBound::Listed(entry_id(5, 1)),
    ] {
        assert_eq!(
            collect(&reader, 0, upper).await.unwrap(),
            expected(&[record(5, 1)])
        );
    }
}

#[tokio::test]
async fn test_footer_limit_is_shared_and_cancelled_refresh_releases_permits() {
    let (_, io, _, _) = fixture();
    for seq in 0..40 {
        put(
            &io,
            seq,
            1,
            seq.checked_sub(1).map(|seq| (seq, 1)),
            0,
            &[record(seq, 1)],
        )
        .await;
    }
    let gate = Arc::new(Semaphore::new(0));
    let observed = observed(io, Some(gate.clone()));
    let budget = Arc::new(Semaphore::new(FOOTER_CONCURRENCY));
    let first = reader(observed.clone(), budget.clone());
    let second = reader(observed.clone(), budget.clone());
    let task = tokio::spawn(async move {
        first
            .prepare(region(), 0, WalReadUpperBound::Listed(entry_id(39, 1)))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while observed.active.load(Ordering::SeqCst) != FOOTER_CONCURRENCY {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(budget.available_permits(), 0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(budget.available_permits(), FOOTER_CONCURRENCY);
    assert_eq!(observed.active.load(Ordering::SeqCst), 0);
    let third = reader(observed.clone(), budget);
    gate.add_permits(100);
    let (a, b) = tokio::join!(
        second.prepare(region(), 0, WalReadUpperBound::Listed(entry_id(39, 1))),
        third.prepare(region(), 0, WalReadUpperBound::Listed(entry_id(39, 1))),
    );
    a.unwrap();
    b.unwrap();
    assert!(observed.peak.load(Ordering::SeqCst) <= FOOTER_CONCURRENCY);
}

fn assert_missing_history(error: Error, storage: &str) {
    assert_eq!(error.status_code(), StatusCode::InvalidArguments);
    assert!(!error.to_string().contains("accept_missing_history"));
    assert!(error.to_string().contains("Check the source node prefix"));
    match error {
        Error::MissingSourceWalHistory {
            region_id,
            prefix,
            storage: actual,
            reason,
            ..
        } => {
            assert_eq!(region_id, region());
            assert_eq!(prefix, PREFIX);
            assert_eq!(actual, storage);
            assert!(!reason.is_empty());
        }
        error => panic!("expected missing history, got {error}"),
    }
}

#[tokio::test]
async fn test_missing_history_error_context() {
    let (_, io, _, reader) = fixture();
    assert_missing_history(
        reader.list_upper_bound(region()).await.unwrap_err(),
        &io.storage(),
    );
    for upper in [
        WalReadUpperBound::Listed(entry_id(1, 1)),
        WalReadUpperBound::LastEntry(entry_id(1, 1)),
    ] {
        assert_missing_history(collect(&reader, 0, upper).await.unwrap_err(), &io.storage());
    }
    // The listing reaches the bound, but the surviving chain has no history
    // covering the region's watermark.
    put(&io, 5, 1, None, 5, &[]).await;
    assert_missing_history(
        collect(
            &reader,
            entry_id(4, 1),
            WalReadUpperBound::Listed(entry_id(5, 1)),
        )
        .await
        .unwrap_err(),
        &io.storage(),
    );
}

#[tokio::test]
async fn test_list_upper_bound_checks_object_sequence_limit() {
    for seq in [
        OBJECT_SEQ_LIMIT - 1,
        OBJECT_SEQ_LIMIT,
        10_000_000_000_000_000_000,
    ] {
        let (store, io, observed, reader) = fixture();
        // Only the key is needed for readiness; no header or footer is read.
        store
            .write(&io.object_path(seq), Bytes::new())
            .await
            .unwrap();
        let bound = reader.list_upper_bound(region()).await;
        if seq < OBJECT_SEQ_LIMIT {
            assert_eq!(bound.unwrap(), u64::MAX);
        } else {
            assert!(
                matches!(bound.unwrap_err(), Error::CorruptedWalObject { reason, .. } if reason.contains(&io.object_path(seq)) && reason.contains("outside the entry-id range"))
            );
        }
        assert!(observed.gets.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_same_reader_prepares_regions_concurrently() {
    let (_, io, _, _) = fixture();
    put(&io, 1, 1, None, 0, &[record(1, 1)]).await;
    let gate = Arc::new(Semaphore::new(0));
    let observed = observed(io, Some(gate.clone()));
    let budget = Arc::new(Semaphore::new(FOOTER_CONCURRENCY));
    let reader = reader(observed.clone(), budget.clone());
    let upper = WalReadUpperBound::Listed(entry_id(1, 1));
    let first = tokio::spawn({
        let reader = reader.clone();
        async move { reader.prepare(region(), 0, upper).await }
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while observed.active.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Poll the second preparation into its pending load while the first GET is
    // parked. Its LIST must finish independently, but its footer load is shared.
    let second = reader.prepare(RegionId::new(1, 2), 0, upper);
    tokio::pin!(second);
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(*observed.lists.lock().unwrap(), vec![0, 0]);
    assert_eq!(observed.active.load(Ordering::SeqCst), 1);
    assert_eq!(budget.available_permits(), FOOTER_CONCURRENCY - 1);
    gate.add_permits(1);
    assert_eq!(
        first.await.unwrap().unwrap().region_max_entry_id(region()),
        Some(entry_id(1, 1))
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .unwrap()
            .unwrap()
            .region_max_entry_id(RegionId::new(1, 2)),
        None
    );
    assert_eq!(*observed.gets.lock().unwrap(), BTreeMap::from([(1, 1)]));
    assert_eq!(budget.available_permits(), FOOTER_CONCURRENCY);
}

#[tokio::test]
async fn test_last_entry_evicts_indexes_as_watermark_advances() {
    let (_, io, _, reader) = fixture();
    for seq in 1..=6 {
        put(
            &io,
            seq,
            1,
            (seq > 1).then_some((seq - 1, 1)),
            0,
            &[record(seq, 1), record(seq, 2)],
        )
        .await;
    }
    let upper = WalReadUpperBound::LastEntry(entry_id(6, 2));
    reader.prepare(region(), 0, upper).await.unwrap();
    let listed = io.list().await.unwrap();
    for start in 2..=6 {
        let old = Arc::downgrade(
            &reader
                .indexes
                .get(&listed[(start - 2) as usize])
                .await
                .unwrap(),
        );
        io.delete(start - 1).await.unwrap();
        reader
            .prepare(region(), entry_id(start, 1), upper)
            .await
            .unwrap();
        reader.indexes.run_pending_tasks().await;
        assert_eq!(
            cached_sequences(&reader, &listed).await,
            (start..=6).collect::<Vec<_>>()
        );
        assert!(old.upgrade().is_none(), "obsolete index remains allocated");
    }
    let last = Arc::downgrade(&reader.indexes.get(&listed[5]).await.unwrap());
    io.delete(6).await.unwrap();
    assert!(matches!(
        reader.prepare(region(), entry_id(6, 1), upper).await,
        Err(Error::MissingSourceWalHistory { .. })
    ));
    reader.indexes.run_pending_tasks().await;
    assert_eq!(reader.indexes.entry_count(), 0);
    assert!(last.upgrade().is_none());
}

#[tokio::test]
async fn test_last_entry_refetches_evicted_indexes_for_lower_watermark() {
    let (_, io, observed, reader) = fixture();
    put(&io, 1, 1, None, 0, &[record(1, 1)]).await;
    put(&io, 2, 1, Some((1, 1)), 0, &[record(2, 1), record(2, 2)]).await;
    let upper = WalReadUpperBound::LastEntry(entry_id(2, 2));
    reader.prepare(region(), 0, upper).await.unwrap();
    reader
        .prepare(region(), entry_id(2, 1), upper)
        .await
        .unwrap();
    let listed = io.list().await.unwrap();
    reader.indexes.run_pending_tasks().await;
    assert_eq!(cached_sequences(&reader, &listed).await, vec![2]);
    let before = observed.gets.lock().unwrap().clone();
    let catalog = reader.prepare(region(), 0, upper).await.unwrap();
    assert_eq!(catalog.region_max_entry_id(region()), Some(entry_id(2, 2)));
    let after = observed.gets.lock().unwrap().clone();
    assert_eq!(after[&1], before[&1] + 1);
    assert_eq!(after[&2], before[&2]);
    assert_eq!(cached_sequences(&reader, &listed).await, vec![1, 2]);
}

#[test]
fn test_node_prefix_validates_layout_and_exposes_provider_identity() {
    let registry = ObjectStoreWalReaders::new(ObjectStore::new(Memory::default()).unwrap());
    assert!(NodeWalPrefix::try_new("wal/datanodes/0/epochs/0").is_ok());
    for root in ["wal", "archive/team/wal"] {
        let config = ObjectStoreWalConfig {
            prefix: root.to_string(),
            ..Default::default()
        };
        let expected = config.node_prefix(12, 34);
        let prefix = NodeWalPrefix::try_new(&expected).unwrap();
        let trailing = NodeWalPrefix::try_new(&format!("{expected}/")).unwrap();
        assert_eq!(prefix, trailing);
        assert_eq!(prefix.as_str(), expected);
        let first = registry.reader(&prefix).unwrap();
        assert_eq!(first.prefix(), expected);
        assert!(Arc::ptr_eq(&first, &registry.reader(&trailing).unwrap()));
    }
    for invalid in [
        "",
        "wal",
        "wal/datanodes/1",
        "wal/datanodes/1/epochs/2/objects",
        "datanodes/1/epochs/2",
        "/wal/datanodes/1/epochs/2",
        "wal//datanodes/1/epochs/2",
        "wal/../datanodes/1/epochs/2",
        "wal/datanodes/a/epochs/2",
        "wal/datanodes/1/epochs/a",
        "wal/datanodes/+1/epochs/2",
        "wal/datanodes/01/epochs/2",
        "wal/datanodes/0/epochs/02",
        "wal/datanodes/00/epochs/0",
        "wal/datanodes/1/epochs/18446744073709551616",
    ] {
        assert!(
            matches!(
                NodeWalPrefix::try_new(invalid),
                Err(Error::InvalidWalObjectStore { .. })
            ),
            "{invalid}"
        );
    }
}

async fn cached_sequences(reader: &ObjectStoreWalReader, listed: &[ListedObject]) -> Vec<u64> {
    let mut sequences = Vec::new();
    for object in listed {
        if reader.indexes.get(object).await.is_some() {
            sequences.push(object.object_seq);
        }
    }
    sequences
}

#[tokio::test]
async fn test_changed_listing_metadata_refetches_index() {
    let (store, io, observed, reader) = fixture();
    put(&io, 1, 1, None, 0, &[record(1, 1)]).await;
    reader
        .prepare(region(), 0, WalReadUpperBound::LastEntry(entry_id(1, 1)))
        .await
        .unwrap();
    let before = observed.gets.lock().unwrap()[&1];
    let replacement = encode_object(
        Header {
            object_seq: 1,
            epoch: 1,
            prev: None,
            reclaim_boundary: 0,
        },
        &[record(1, 1), record(1, 2)],
    )
    .unwrap();
    store
        .write(&io.object_path(1), replacement.bytes)
        .await
        .unwrap();
    let catalog = reader
        .prepare(region(), 0, WalReadUpperBound::LastEntry(entry_id(1, 2)))
        .await
        .unwrap();
    assert_eq!(catalog.region_max_entry_id(region()), Some(entry_id(1, 2)));
    assert_eq!(observed.gets.lock().unwrap()[&1], before + 1);
}
