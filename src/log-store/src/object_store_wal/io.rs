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

use bytes::Bytes;
use object_store::{ErrorKind, ObjectStore};
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{
    CorruptedWalObjectSnafu, InvalidWalObjectStoreSnafu, Result, WalObjectConflictSnafu,
    WalObjectStoreSnafu,
};

/// Width of the zero-padded object sequence in an object key, wide enough for
/// [`u64::MAX`] so that lexicographic and numeric order agree.
const OBJECT_SEQ_WIDTH: usize = 20;
const OBJECT_SUFFIX: &str = ".wal";

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
pub(crate) struct ObjectStoreIo {
    store: ObjectStore,
    object_prefix: String,
}

impl ObjectStoreIo {
    /// Binds `store` to the objects under `prefix`. The store must support
    /// conditional creates, which is what keeps a retried write from
    /// overwriting a durable object.
    pub(crate) fn new(store: ObjectStore, prefix: impl AsRef<str>) -> Result<Self> {
        let object_prefix = normalize_prefix(prefix.as_ref())?;
        ensure!(
            store.info().capability().write_with_if_not_exists,
            InvalidWalObjectStoreSnafu {
                reason: "object store does not support conditional create",
            }
        );
        Ok(Self {
            store,
            object_prefix,
        })
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
            .into_iter()
            .filter_map(|entry| {
                self.parse_object_seq(entry.path())
                    .map(|object_seq| ListedObject {
                        object_seq,
                        path: entry.path().to_string(),
                        size: entry.metadata().content_length(),
                    })
            })
            .collect::<Vec<_>>();
        objects.sort_unstable_by_key(|object| object.object_seq);
        Ok(objects)
    }

    pub(crate) fn object_path(&self, object_seq: u64) -> String {
        format!(
            "{}{object_seq:0OBJECT_SEQ_WIDTH$}{OBJECT_SUFFIX}",
            self.object_prefix
        )
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
    Ok(format!("{prefix}/objects/"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::ext::{ErrorExt, RetryHint};
    use common_test_util::temp_dir::create_temp_dir;
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
