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

//! Object store access for WAL objects: a deterministic key layout and
//! conditional creates that make a retry of the same object a no-op.

use async_stream::try_stream;
use bytes::Bytes;
use common_telemetry::warn;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::{Entry, ErrorKind, ObjectStore};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

use crate::error::{
    CorruptedWalObjectSnafu, InvalidWalObjectStoreSnafu, Result, WalObjectConflictSnafu,
    WalObjectStoreSnafu,
};

/// Width of the zero-padded object sequence in an object key, wide enough for
/// [`u64::MAX`] so that lexicographic and numeric order agree.
const OBJECT_SEQ_WIDTH: usize = 20;
const OBJECT_SUFFIX: &str = ".wal";
const PROBE_CONTENT: &[u8] = b"conditional create probe";
/// Objects a listing may hold ahead of the reads that take them, four LIST
/// pages of S3, so the listing stays pages ahead of the reads while what it
/// holds is bounded.
const LISTING_AHEAD_OBJECTS: usize = 4096;

/// Outcome of a conditional create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PutResult {
    /// This call created the object.
    Created,
    /// The object already held the same content, so the call was a retry.
    AlreadyPresent,
}

/// An object discovered by a LIST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListedObject {
    pub(crate) object_seq: u64,
    pub(crate) path: String,
    /// Length of the object in bytes, as reported by the listing.
    pub(crate) size: u64,
}

/// Reads and writes the WAL objects under one prefix.
#[derive(Clone)]
pub(crate) struct ObjectStoreIo {
    store: ObjectStore,
    object_prefix: String,
    /// Holds the keys of [`Self::probe_conditional_create`], outside
    /// `object_prefix` so that no listing of the objects returns one.
    probe_prefix: String,
}

impl ObjectStoreIo {
    /// Binds `store` to the objects under `prefix`. The store must support
    /// conditional creates, which is what keeps a retried write from
    /// overwriting a durable object.
    pub(crate) fn new(store: ObjectStore, prefix: impl AsRef<str>) -> Result<Self> {
        let prefix = normalize_prefix(prefix.as_ref())?;
        ensure!(
            store.info().capability().write_with_if_not_exists,
            InvalidWalObjectStoreSnafu {
                reason: "object store does not support conditional create",
            }
        );
        Ok(Self {
            store,
            object_prefix: format!("{prefix}/objects/"),
            probe_prefix: format!("{prefix}/probes/"),
        })
    }

    /// Checks that the store refuses a conditional create of an object that
    /// exists. The capability checked by [`Self::new`] is declared per
    /// service, and a store behind the S3 service may accept the condition and
    /// overwrite the object anyway, which would let a retried write replace a
    /// durable object. The probe creates a key of its own, unique to this
    /// call, twice, and then deletes it.
    pub(crate) async fn probe_conditional_create(&self) -> Result<()> {
        let path = format!("{}{}", self.probe_prefix, Uuid::new_v4());
        let create = || {
            self.store
                .write_with(&path, PROBE_CONTENT)
                .if_not_exists(true)
        };
        create().await.context(WalObjectStoreSnafu {
            operation: "write",
            path: path.clone(),
        })?;
        let recreated = create().await;
        if let Err(error) = self.store.delete(&path).await {
            warn!(error; "Failed to delete the conditional create probe {path} of the object store WAL");
        }
        match recreated {
            // The first create stored this key, so `AlreadyExists` can only
            // refer to it.
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error).context(WalObjectStoreSnafu {
                operation: "write",
                path,
            }),
            Ok(_) => InvalidWalObjectStoreSnafu {
                reason: format!(
                    "the object store ({}) overwrote the existing object {path} on a conditional create, but the object store WAL requires conditional create support; a versioned bucket or the S3-compatible endpoint of GCS can cause this",
                    self.storage()
                ),
            }
            .fail(),
        }
    }

    /// Creates the object `object_seq` unless it already exists. A retry with
    /// the same content is accepted, different content is rejected.
    ///
    /// When the store reports that the object exists but the read that
    /// compares its content fails, the read failure is returned, so that its
    /// retry hint says whether comparing again may settle the outcome. Any
    /// other create failure is returned as the write failure it is.
    pub(crate) async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
        let path = self.object_path(object_seq);
        let write_result = self
            .store
            .write_with(&path, content.clone())
            .if_not_exists(true)
            .await;

        match write_result {
            Ok(_) => Ok(PutResult::Created),
            Err(error) => match self.store.read(&path).await {
                Ok(existing) if existing.to_bytes() == content => Ok(PutResult::AlreadyPresent),
                Ok(_) => WalObjectConflictSnafu { path }.fail(),
                Err(read_error) if reports_existing_object(&error) => {
                    Err(read_error).context(WalObjectStoreSnafu {
                        operation: "read",
                        path,
                    })
                }
                Err(_) => Err(error).context(WalObjectStoreSnafu {
                    operation: "write",
                    path,
                }),
            },
        }
    }

    /// Reads the object `object_seq`.
    pub(crate) async fn get(&self, object_seq: u64) -> Result<Bytes> {
        let path = self.object_path(object_seq);
        self.store
            .read(&path)
            .await
            .map(|content| content.to_bytes())
            .context(WalObjectStoreSnafu {
                operation: "read",
                path,
            })
    }

    /// Reads `len` bytes of the object `object_seq` starting at `offset`. The
    /// range must lie inside the object; one that reaches past its end fails.
    pub(crate) async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
        let path = self.object_path(object_seq);
        let end = offset
            .checked_add(len)
            .with_context(|| CorruptedWalObjectSnafu {
                reason: format!("byte range {offset}..{len} overflows the object"),
            })?;
        self.store
            .read_with(&path)
            .range(offset..end)
            .await
            .map(|content| content.to_bytes())
            .context(WalObjectStoreSnafu {
                operation: "read",
                path,
            })
    }

    /// Deletes the object `object_seq`. Deleting an object that does not
    /// exist succeeds.
    pub(crate) async fn delete(&self, object_seq: u64) -> Result<()> {
        let path = self.object_path(object_seq);
        self.store.delete(&path).await.context(WalObjectStoreSnafu {
            operation: "delete",
            path,
        })
    }

    /// Lists the objects under the prefix, ordered by object sequence. Keys
    /// that do not follow the object layout are ignored.
    pub(crate) async fn list(&self) -> Result<Vec<ListedObject>> {
        let entries = self
            .store
            .list(&self.object_prefix)
            .await
            .with_context(|_| WalObjectStoreSnafu {
                operation: "list",
                path: self.object_prefix.clone(),
            })?;
        let mut objects = entries
            .iter()
            .filter_map(|entry| self.listed_object(entry))
            .collect::<Vec<_>>();
        objects.sort_unstable_by_key(|object| object.object_seq);
        Ok(objects)
    }

    /// Lists the objects under the prefix like [`list`](Self::list), but from
    /// a task of its own and in the order the object store returns them, so
    /// the listing goes on while the caller consumes the objects listed so
    /// far. A listing that fails, however it fails, ends the stream with an
    /// error, before any object it listed that the caller has not taken yet;
    /// dropping the stream stops the listing.
    pub(crate) fn list_ahead(&self) -> BoxStream<'static, Result<ListedObject>> {
        let (listing, rx) = self.clone().listing();
        let listing = AbortOnDropHandle::new(common_runtime::spawn_global(listing));
        settle_listing(rx, listing, self.object_prefix.clone())
    }

    /// Returns the listing of the objects under the prefix and the receiver it
    /// sends them to; it waits while the receiver holds
    /// [`LISTING_AHEAD_OBJECTS`] of them.
    fn listing(
        self,
    ) -> (
        impl Future<Output = object_store::Result<()>> + Send + 'static,
        mpsc::Receiver<ListedObject>,
    ) {
        let (tx, rx) = mpsc::channel(LISTING_AHEAD_OBJECTS);
        let listing = async move {
            let mut lister = self.store.lister(&self.object_prefix).await?;
            while let Some(entry) = lister.try_next().await? {
                // The receiver is gone only once the stream was dropped.
                if let Some(object) = self.listed_object(&entry)
                    && tx.send(object).await.is_err()
                {
                    break;
                }
            }
            Ok::<_, object_store::Error>(())
        };
        (listing, rx)
    }

    pub(crate) fn object_path(&self, object_seq: u64) -> String {
        format!(
            "{}{object_seq:0OBJECT_SEQ_WIDTH$}{OBJECT_SUFFIX}",
            self.object_prefix
        )
    }

    /// Describes the object store the objects live in by its scheme, its
    /// bucket when it has one and its root, none of which is a credential.
    pub(crate) fn storage(&self) -> String {
        let info = self.store.info();
        let name = info.name();
        if name.is_empty() {
            format!("{}, root {:?}", info.scheme(), info.root())
        } else {
            format!("{}, bucket {name:?}, root {:?}", info.scheme(), info.root())
        }
    }

    fn listed_object(&self, entry: &Entry) -> Option<ListedObject> {
        self.parse_object_seq(entry.path())
            .map(|object_seq| ListedObject {
                object_seq,
                path: entry.path().to_string(),
                size: entry.metadata().content_length(),
            })
    }

    fn parse_object_seq(&self, path: &str) -> Option<u64> {
        let value = path
            .strip_prefix(&self.object_prefix)?
            .strip_suffix(OBJECT_SUFFIX)?;
        if value.len() != OBJECT_SEQ_WIDTH || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        value.parse().ok()
    }
}

/// Yields the objects `listing` sends over `rx`. Once the listing finished it
/// is settled before any further object is yielded, so its failure goes
/// before the objects it listed that the caller has not taken yet.
fn settle_listing(
    mut rx: mpsc::Receiver<ListedObject>,
    listing: AbortOnDropHandle<object_store::Result<()>>,
    path: String,
) -> BoxStream<'static, Result<ListedObject>> {
    let mut listing = Some(listing);
    Box::pin(try_stream! {
        loop {
            let object = rx.recv().await;
            if let Some(listing) =
                listing.take_if(|listing| object.is_none() || listing.is_finished())
            {
                listing
                    .await
                    .unwrap_or_else(|error| {
                        let stopped = "the listing task did not complete";
                        Err(object_store::Error::new(ErrorKind::Unexpected, stopped)
                            .set_source(error))
                    })
                    .context(WalObjectStoreSnafu { operation: "list", path: &path })?;
            }
            let Some(object) = object else {
                break;
            };
            yield object;
        }
    })
}

/// Returns true when a conditional create failed because the object exists.
/// Only the precondition failure says so: a store may report `AlreadyExists`
/// for an unrelated path, such as a parent that is a file.
fn reports_existing_object(error: &object_store::Error) -> bool {
    error.kind() == ErrorKind::ConditionNotMatch
}

fn normalize_prefix(prefix: &str) -> Result<String> {
    let prefix = prefix.trim();
    ensure!(
        !prefix.is_empty() && !prefix.starts_with('/'),
        InvalidWalObjectStoreSnafu {
            reason: format!("object prefix {prefix:?} is empty or absolute"),
        }
    );
    let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
    ensure!(
        !prefix
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == ".."),
        InvalidWalObjectStoreSnafu {
            reason: format!("object prefix {prefix:?} has an empty or relative component"),
        }
    );
    Ok(prefix.to_string())
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common_error::ext::{ErrorExt, RetryHint};
    use common_test_util::temp_dir::create_temp_dir;
    use futures::poll;
    use object_store::layers::mock::{self, MockLayerBuilder, oio};
    use object_store::secure_fs::SecureFsRoot;
    use object_store::services::Memory;

    use super::*;
    use crate::error::Error;

    fn memory_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap()
    }

    /// A reader whose every read fails with a temporary error.
    struct FailingReader;

    impl oio::Read for FailingReader {
        async fn open(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
            Err(injected_failure("read").set_temporary())
        }

        async fn read(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, mock::Buffer)> {
            Err(injected_failure("read").set_temporary())
        }
    }

    /// A writer that fails to close, so nothing is written.
    struct FailingWriter;

    impl oio::Write for FailingWriter {
        async fn write(&mut self, _buffer: mock::Buffer) -> mock::Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> mock::Result<mock::Metadata> {
            Err(injected_failure("write"))
        }

        async fn abort(&mut self) -> mock::Result<()> {
            Ok(())
        }
    }

    fn injected_failure(operation: &str) -> mock::Error {
        mock::Error::new(
            mock::ErrorKind::Unexpected,
            format!("injected {operation} failure"),
        )
    }

    fn with_failing_reads(store: ObjectStore) -> ObjectStore {
        store.layer(
            MockLayerBuilder::default()
                .reader_factory(Arc::new(|_, _, _| Box::new(FailingReader)))
                .build()
                .unwrap(),
        )
    }

    fn with_failing_writes(store: ObjectStore) -> ObjectStore {
        store.layer(
            MockLayerBuilder::default()
                .writer_factory(Arc::new(|_, _, _| Box::new(FailingWriter)))
                .build()
                .unwrap(),
        )
    }

    fn memory_io() -> ObjectStoreIo {
        ObjectStoreIo::new(memory_store(), "datanodes/1/epochs/2/").unwrap()
    }

    #[test]
    fn test_io_builds_and_parses_deterministic_object_paths() {
        let io = ObjectStoreIo::new(memory_store(), " datanodes/1/epochs/2/ ").unwrap();
        assert_eq!(
            "datanodes/1/epochs/2/objects/00000000000000000000.wal",
            io.object_path(0)
        );
        assert_eq!(
            "datanodes/1/epochs/2/objects/00000000000000000042.wal",
            io.object_path(42)
        );
        assert_eq!(
            "datanodes/1/epochs/2/objects/18446744073709551615.wal",
            io.object_path(u64::MAX)
        );
        assert_eq!(Some(42), io.parse_object_seq(&io.object_path(42)));
        assert_eq!(
            None,
            io.parse_object_seq("datanodes/1/epochs/2/objects/42.wal")
        );
    }

    #[test]
    fn test_io_rejects_invalid_prefixes() {
        for prefix in [
            "",
            " ",
            "/absolute",
            ".",
            "./node",
            "node/./epoch",
            "..",
            "node/../epoch",
            "node//epoch",
            "node//",
            "node///",
        ] {
            match ObjectStoreIo::new(memory_store(), prefix) {
                Err(Error::InvalidWalObjectStore { .. }) => {}
                Err(error) => panic!("unexpected error for prefix {prefix:?}: {error:?}"),
                Ok(_) => panic!("expected prefix {prefix:?} to be rejected"),
            }
        }
    }

    #[tokio::test]
    async fn test_io_puts_gets_and_lists_objects_by_sequence() {
        let io = memory_io();

        for object_seq in [10, 2, 1] {
            assert_eq!(
                PutResult::Created,
                io.put_if_absent(object_seq, Bytes::from(object_seq.to_string()))
                    .await
                    .unwrap()
            );
        }

        assert_eq!(Bytes::from_static(b"2"), io.get(2).await.unwrap());
        let objects = io.list().await.unwrap();
        assert_eq!(vec![1, 2, 10], object_seqs(objects.clone()));
        assert_eq!(io.object_path(1), objects[0].path);
        assert_eq!(
            vec![1, 1, 2],
            objects.iter().map(|object| object.size).collect::<Vec<_>>()
        );

        // A delete of an object that is already gone succeeds.
        io.delete(2).await.unwrap();
        io.delete(2).await.unwrap();
        assert_eq!(vec![1, 10], object_seqs(io.list().await.unwrap()));
    }

    #[tokio::test]
    async fn test_io_get_range_reads_a_slice_inside_the_object() {
        let io = memory_io();
        io.put_if_absent(3, Bytes::from_static(b"0123456789"))
            .await
            .unwrap();

        assert_eq!(
            Bytes::from_static(b"234"),
            io.get_range(3, 2, 3).await.unwrap()
        );
        assert_eq!(
            Bytes::from_static(b"89"),
            io.get_range(3, 8, 2).await.unwrap()
        );
        for (object_seq, offset, len) in [(3, 8, 5), (3, 10, 1), (7, 0, 1)] {
            let error = io.get_range(object_seq, offset, len).await.unwrap_err();
            assert!(
                matches!(error, Error::WalObjectStore { operation: "read", ref path, .. } if path == &io.object_path(object_seq)),
                "unexpected error for range {offset}..{len} of object {object_seq}: {error:?}"
            );
        }
        let error = io.get_range(3, u64::MAX, 1).await.unwrap_err();
        assert!(
            matches!(error, Error::CorruptedWalObject { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_io_same_content_is_an_idempotent_retry() {
        let io = memory_io();
        let content = Bytes::from_static(b"immutable");

        assert_eq!(
            PutResult::Created,
            io.put_if_absent(7, content.clone()).await.unwrap()
        );
        assert_eq!(
            PutResult::AlreadyPresent,
            io.put_if_absent(7, content.clone()).await.unwrap()
        );
        assert_eq!(content, io.get(7).await.unwrap());
    }

    #[tokio::test]
    async fn test_io_rejects_different_content_without_overwriting() {
        let io = memory_io();
        let original = Bytes::from_static(b"original");
        io.put_if_absent(7, original.clone()).await.unwrap();

        let error = io
            .put_if_absent(7, Bytes::from_static(b"replacement"))
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::WalObjectConflict { ref path, .. } if path == &io.object_path(7)),
            "unexpected error: {error:?}"
        );
        assert_eq!(original, io.get(7).await.unwrap());
    }

    #[tokio::test]
    async fn test_io_retry_of_an_existing_object_reports_the_failed_read() {
        let store = memory_store();
        let prefix = "datanodes/1/epochs/2/";
        let content = Bytes::from_static(b"immutable");
        ObjectStoreIo::new(store.clone(), prefix)
            .unwrap()
            .put_if_absent(7, content.clone())
            .await
            .unwrap();

        // The object exists, so the create is refused, and the read that would
        // settle the retry fails: that failure and its retry hint come back.
        let io = ObjectStoreIo::new(with_failing_reads(store.clone()), prefix).unwrap();
        let error = io.put_if_absent(7, content.clone()).await.unwrap_err();
        assert!(
            matches!(error, Error::WalObjectStore { operation: "read", ref path, .. } if path == &io.object_path(7)),
            "unexpected error: {error:?}"
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());

        // A create that fails for another reason is reported as the write
        // failure it is, although the read of the missing object fails too.
        let io = ObjectStoreIo::new(with_failing_writes(store), prefix).unwrap();
        let error = io.put_if_absent(8, content).await.unwrap_err();
        assert!(
            matches!(error, Error::WalObjectStore { operation: "write", ref path, .. } if path == &io.object_path(8)),
            "unexpected error: {error:?}"
        );
        assert_eq!(RetryHint::NonRetryable, error.retry_hint());
    }

    #[tokio::test]
    async fn test_io_create_under_a_file_parent_is_reported_as_the_write_failure() {
        let temp_dir = create_temp_dir("object_store_wal_io_file_parent");
        std::fs::write(temp_dir.path().join("datanodes"), []).unwrap();
        let store = SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();
        let io = ObjectStoreIo::new(store, "datanodes/1/epochs/2/").unwrap();

        // The store reports `AlreadyExists` for the parent, not for the
        // object, so the create failure itself comes back.
        let error = io
            .put_if_absent(7, Bytes::from_static(b"wal"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::WalObjectStore { operation: "write", ref path, .. } if path == &io.object_path(7)),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_io_get_of_a_missing_object_fails() {
        let io = memory_io();

        let error = io.get(7).await.unwrap_err();
        assert!(
            matches!(error, Error::WalObjectStore { operation: "read", ref path, .. } if path == &io.object_path(7)),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_io_list_ahead_yields_the_objects_list_returns() {
        let io = memory_io();
        assert!(
            io.list_ahead()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );

        io.store
            .write(
                "datanodes/1/epochs/2/objects/not-a-sequence.wal",
                "malformed",
            )
            .await
            .unwrap();
        for object_seq in [10, 2, 1] {
            io.put_if_absent(object_seq, Bytes::from(object_seq.to_string()))
                .await
                .unwrap();
        }
        let mut listed = io.list_ahead().try_collect::<Vec<_>>().await.unwrap();
        listed.sort_unstable_by_key(|object| object.object_seq);
        assert_eq!(io.list().await.unwrap(), listed);
    }

    struct CountingLister {
        inner: oio::Lister,
        listed: Arc<AtomicUsize>,
    }

    impl oio::List for CountingLister {
        async fn next(&mut self) -> mock::Result<Option<oio::Entry>> {
            self.listed.fetch_add(1, Ordering::SeqCst);
            self.inner.next().await
        }
    }

    #[tokio::test]
    async fn test_io_listing_waits_while_the_backlog_is_full() {
        let store = memory_store();
        let objects = LISTING_AHEAD_OBJECTS + 100;
        let io = ObjectStoreIo::new(store.clone(), "datanodes/1/epochs/2/").unwrap();
        for object_seq in 0..objects as u64 {
            io.put_if_absent(object_seq, Bytes::from_static(b"wal"))
                .await
                .unwrap();
        }
        let listed = Arc::new(AtomicUsize::new(0));
        let lister_listed = listed.clone();
        let mock = MockLayerBuilder::default()
            .lister_factory(Arc::new(move |_, _, inner| {
                let listed = lister_listed.clone();
                Box::new(CountingLister { inner, listed })
            }))
            .build()
            .unwrap();
        let io = ObjectStoreIo::new(store.layer(mock), "datanodes/1/epochs/2/").unwrap();
        let (listing, mut rx) = io.listing();
        // Out of the cooperative budget, so it waits only on a full channel.
        let mut listing = pin!(tokio::task::unconstrained(listing));

        // Nothing takes the objects, so the listing waits once the backlog is
        // full, with the directory entry and the object waiting to be sent
        // listed on top of it.
        for _ in 0..100 {
            assert!(poll!(listing.as_mut()).is_pending());
        }
        let ahead = listed.load(Ordering::SeqCst);
        assert!(
            (LISTING_AHEAD_OBJECTS + 1..=LISTING_AHEAD_OBJECTS + 2).contains(&ahead),
            "{ahead} entries listed"
        );

        let taken = async {
            let mut taken = 0;
            while rx.recv().await.is_some() {
                taken += 1;
            }
            taken
        };
        let (listed, taken) = futures::join!(listing, taken);
        listed.unwrap();
        assert_eq!(objects, taken);
    }

    #[tokio::test]
    async fn test_io_failed_listing_goes_before_the_objects_it_listed() {
        let io = memory_io();
        let (tx, rx) = mpsc::channel(LISTING_AHEAD_OBJECTS);
        for object_seq in 0..1000 {
            let path = io.object_path(object_seq);
            tx.try_send(ListedObject {
                object_seq,
                path,
                size: 0,
            })
            .unwrap();
        }
        let listing = AbortOnDropHandle::new(tokio::spawn(async move {
            drop(tx);
            Err(injected_failure("list"))
        }));
        while !listing.is_finished() {
            tokio::task::yield_now().await;
        }

        let mut objects = settle_listing(rx, listing, io.object_prefix.clone());
        let error = objects.try_next().await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectStore {
                    operation: "list",
                    ..
                }
            ),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_io_list_ignores_objects_outside_the_wal_layout() {
        let io = memory_io();
        io.store.write("unrelated", "outside").await.unwrap();
        io.store
            .write(
                "datanodes/1/epochs/2/objects/not-a-sequence.wal",
                "malformed",
            )
            .await
            .unwrap();
        io.put_if_absent(3, Bytes::from_static(b"wal"))
            .await
            .unwrap();

        assert_eq!(vec![3], object_seqs(io.list().await.unwrap()));
    }

    #[tokio::test]
    async fn test_io_list_is_isolated_by_prefix() {
        let store = memory_store();
        let first = ObjectStoreIo::new(store.clone(), "datanodes/1/epochs/1").unwrap();
        let second = ObjectStoreIo::new(store, "datanodes/1/epochs/2").unwrap();
        first
            .put_if_absent(1, Bytes::from_static(b"first"))
            .await
            .unwrap();
        second
            .put_if_absent(2, Bytes::from_static(b"second"))
            .await
            .unwrap();

        assert_eq!(vec![1], object_seqs(first.list().await.unwrap()));
        assert_eq!(vec![2], object_seqs(second.list().await.unwrap()));
    }

    #[tokio::test]
    async fn test_io_equal_sequences_in_different_prefixes_do_not_conflict() {
        let store = memory_store();
        let first = ObjectStoreIo::new(store.clone(), "datanodes/1/epochs/1").unwrap();
        let second = ObjectStoreIo::new(store, "datanodes/1/epochs/2").unwrap();

        assert_eq!(
            PutResult::Created,
            first
                .put_if_absent(7, Bytes::from_static(b"first"))
                .await
                .unwrap()
        );
        assert_eq!(
            PutResult::Created,
            second
                .put_if_absent(7, Bytes::from_static(b"different"))
                .await
                .unwrap()
        );
        assert_eq!(Bytes::from_static(b"first"), first.get(7).await.unwrap());
        assert_eq!(
            Bytes::from_static(b"different"),
            second.get(7).await.unwrap()
        );
    }

    fn object_seqs(objects: Vec<ListedObject>) -> Vec<u64> {
        objects
            .into_iter()
            .map(|object| object.object_seq)
            .collect()
    }
}
