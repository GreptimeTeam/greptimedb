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

//! Streaming destinations for one schema chunk of independent Parquet streams.

use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore, Writer};
use snafu::{ResultExt, ensure};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::{self, Result};
use crate::packed_snapshot::{ObjectKind, PACK_INDEX_FILE, PackIndex, PackObject, PackTable};

pub const WRITE_BYTES: usize = 8 * 1024 * 1024;

/// One request owns this sink. Finishers retain their encoder admission while
/// waiting on the mutex, bounding both completed buffers and pending appends.
pub type PackedWriterRef = Arc<Mutex<PackedWriter>>;

#[derive(Clone, Copy)]
struct UploadLimits {
    part: usize,
    object: u64,
}

impl UploadLimits {
    fn for_store(store: &ObjectStore) -> Result<Self> {
        let info = store.info();
        let caps = info.capability();
        let part = caps
            .write_multi_max_size
            .unwrap_or(WRITE_BYTES)
            .min(WRITE_BYTES);
        ensure!(
            part > 0 && caps.write_multi_min_size.unwrap_or(0) <= part,
            error::ParquetWriterResourceSnafu {
                reason: "backend cannot accept bounded 8 MiB parts"
            }
        );
        // OpenDAL exposes part size but not part count. These are the limits of
        // its multipart implementations, including GCS's XML multipart writer.
        let parts = match info.scheme() {
            "s3" | "oss" | "gcs" => 10_000,
            "azblob" => 50_000,
            _ => u64::MAX,
        };
        let object = (caps.write_total_max_size.unwrap_or(usize::MAX) as u64)
            .min(parts.saturating_mul(part as u64));
        Ok(Self { part, object })
    }
}

struct Upload {
    writer: Writer,
    store: ObjectStore,
    path: String,
    limits: UploadLimits,
    length: u64,
    conditional: bool,
    close_started: bool,
}

impl Upload {
    async fn open(store: &ObjectStore, path: String, limits: UploadLimits) -> Result<Self> {
        let conditional = store.info().capability().write_with_if_not_exists;
        if !conditional {
            ensure!(
                !store
                    .exists(&path)
                    .await
                    .context(error::ReadObjectSnafu { path: &path })?,
                error::InvalidPackedSnapshotSnafu {
                    reason: format!("output already exists: {path}")
                }
            );
        }
        let writer = store
            .writer_with(&path)
            .chunk(limits.part)
            .concurrent(1)
            .if_not_exists(conditional)
            .await
            .context(error::WriteObjectSnafu { path: &path })?;
        Ok(Self {
            writer,
            store: store.clone(),
            path,
            limits,
            length: 0,
            conditional,
            close_started: false,
        })
    }

    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        ensure!(
            (bytes.len() as u64) <= self.limits.object.saturating_sub(self.length),
            error::ParquetWriterResourceSnafu {
                reason: "packed export object or multipart part-count limit exceeded"
            }
        );
        for offset in (0..bytes.len()).step_by(self.limits.part) {
            self.writer
                .write(bytes.slice(offset..(offset + self.limits.part).min(bytes.len())))
                .await
                .context(error::WriteObjectSnafu { path: &self.path })?;
        }
        self.length += bytes.len() as u64;
        Ok(())
    }

    async fn write_json_array<T: serde::Serialize>(
        &mut self,
        values: &[T],
        token: &CancellationToken,
    ) -> Result<()> {
        self.write(Bytes::from_static(b"[")).await?;
        for (i, value) in values.iter().enumerate() {
            check_cancelled(Some(token))?;
            if i > 0 {
                self.write(Bytes::from_static(b",")).await?;
            }
            let bytes = serde_json::to_vec(value).map_err(|e| {
                error::InvalidPackedSnapshotSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?;
            self.write(bytes.into()).await?;
        }
        self.write(Bytes::from_static(b"]")).await
    }

    async fn close(&mut self) -> Result<()> {
        self.close_started = true;
        self.writer
            .close()
            .await
            .context(error::WriteObjectSnafu { path: &self.path })?;
        Ok(())
    }

    async fn abort(mut self) -> Result<()> {
        let result = self.writer.abort().await;
        if !self.conditional
            && result.as_ref().is_err_and(|e| {
                e.kind() == object_store::ErrorKind::Unsupported
                    && (!self.close_started
                        || object_store::secure_fs::is_unsynced_overwrite_abort(e))
            })
        {
            let store = self.store.clone();
            let path = self.path.clone();
            drop(self);
            store
                .delete(&path)
                .await
                .context(error::WriteObjectSnafu { path })?;
        } else {
            result.context(error::WriteObjectSnafu { path: &self.path })?;
        }
        Ok(())
    }
}

/// Pack/index metadata is accounted separately from retained Arrow payloads.
pub struct PackedWriter {
    store: ObjectStore,
    limits: UploadLimits,
    pack: Option<Upload>,
    index: PackIndex,
    next_pack: usize,
}

impl PackedWriter {
    pub fn new(store: ObjectStore) -> Result<PackedWriterRef> {
        let limits = UploadLimits::for_store(&store)?;
        Ok(Arc::new(Mutex::new(Self {
            store,
            limits,
            pack: None,
            index: PackIndex {
                version: 1,
                objects: vec![],
                tables: vec![],
            },
            next_pack: 0,
        })))
    }

    async fn close_pack(&mut self) -> Result<()> {
        if let Some(pack) = &mut self.pack {
            pack.close().await?;
            self.index.objects.push(PackObject {
                path: pack.path.clone(),
                kind: ObjectKind::Pack,
                length: pack.length,
            });
            self.pack = None;
        }
        Ok(())
    }

    async fn append(
        &mut self,
        name: String,
        bytes: Bytes,
        rows: u64,
        token: Option<&CancellationToken>,
    ) -> Result<()> {
        check_cancelled(token)?;
        if self
            .pack
            .as_ref()
            .is_some_and(|p| bytes.len() as u64 > self.limits.object.saturating_sub(p.length))
        {
            self.close_pack().await?;
        }
        check_cancelled(token)?;
        if self.pack.is_none() {
            let path = format!("pack-{:06}.bin", self.next_pack);
            self.next_pack += 1;
            self.pack = Some(Upload::open(&self.store, path, self.limits).await?);
        }
        let pack = self.pack.as_mut().ok_or_else(|| {
            error::InvalidPackedSnapshotSnafu {
                reason: "missing pack writer",
            }
            .build()
        })?;
        let table = PackTable {
            table_name: name,
            object: pack.path.clone(),
            offset: pack.length,
            length: bytes.len() as u64,
            row_count: rows,
        };
        pack.write(bytes).await?;
        check_cancelled(token)?;
        self.index.tables.push(table);
        Ok(())
    }

    /// Publish the index only after every data object has closed successfully.
    pub async fn finish(&mut self, token: &CancellationToken) -> Result<Vec<String>> {
        check_cancelled(Some(token))?;
        self.close_pack().await?;
        check_cancelled(Some(token))?;
        self.index
            .tables
            .sort_unstable_by(|a, b| a.table_name.cmp(&b.table_name));
        self.index.validate()?;
        let mut upload = Upload::open(&self.store, PACK_INDEX_FILE.into(), self.limits).await?;
        let result = async {
            upload
                .write(Bytes::from_static(b"{\"version\":1,\"objects\":"))
                .await?;
            upload.write_json_array(&self.index.objects, token).await?;
            upload.write(Bytes::from_static(b",\"tables\":")).await?;
            upload.write_json_array(&self.index.tables, token).await?;
            upload.write(Bytes::from_static(b"}")).await?;
            check_cancelled(Some(token))?;
            upload.close().await?;
            check_cancelled(Some(token))
        }
        .await;
        if let Err(error) = result {
            if let Err(secondary) = upload.abort().await {
                common_telemetry::warn!(secondary; "Failed to abort pack index");
            }
            return Err(error);
        }
        Ok(self
            .index
            .objects
            .iter()
            .map(|o| o.path.clone())
            .chain([PACK_INDEX_FILE.into()])
            .collect())
    }

    /// Call only after all admitted encoders have drained their I/O.
    pub async fn abort(&mut self) -> Result<()> {
        if let Some(pack) = self.pack.take() {
            pack.abort().await?;
        }
        Ok(())
    }
}

/// Buffers a small stream, or spills its prefix and continues the same stream.
pub struct PackedTableWriter {
    shared: PackedWriterRef,
    name: String,
    path: String,
    buffer: Vec<u8>,
    ordinary: bool,
    standalone: Option<Upload>,
}

impl PackedTableWriter {
    /// The caller retains encoder admission until finish/abort returns.
    pub fn new(shared: PackedWriterRef, name: String, id: u32, ordinary: bool) -> Self {
        Self {
            shared,
            name,
            path: format!("table-{id}.parquet"),
            buffer: if ordinary {
                Vec::new()
            } else {
                Vec::with_capacity(WRITE_BYTES)
            },
            ordinary,
            standalone: None,
        }
    }

    pub(crate) async fn write(&mut self, bytes: Bytes) -> Result<()> {
        if self.standalone.is_none()
            && (self.ordinary || self.buffer.len().saturating_add(bytes.len()) > WRITE_BYTES)
        {
            let sink = self.shared.lock().await;
            self.standalone =
                Some(Upload::open(&sink.store, self.path.clone(), sink.limits).await?);
        }
        if let Some(upload) = &mut self.standalone {
            if !self.buffer.is_empty() {
                upload
                    .write(std::mem::take(&mut self.buffer).into())
                    .await?;
            }
            upload.write(bytes).await
        } else {
            self.buffer.extend_from_slice(&bytes);
            Ok(())
        }
    }

    pub(crate) async fn finish(
        &mut self,
        rows: u64,
        token: Option<&CancellationToken>,
    ) -> Result<()> {
        check_cancelled(token)?;
        if let Some(upload) = &mut self.standalone {
            upload.close().await?;
            check_cancelled(token)?;
            let mut shared = self.shared.lock().await;
            shared.index.objects.push(PackObject {
                path: self.path.clone(),
                kind: ObjectKind::Parquet,
                length: upload.length,
            });
            shared.index.tables.push(PackTable {
                table_name: self.name.clone(),
                object: self.path.clone(),
                offset: 0,
                length: upload.length,
                row_count: rows,
            });
        } else {
            self.shared
                .lock()
                .await
                .append(
                    self.name.clone(),
                    // OpenDAL may retain each slice until the pack part fills.
                    // Release the encoder's 8 MiB capacity before queuing it.
                    Bytes::from(std::mem::take(&mut self.buffer).into_boxed_slice()),
                    rows,
                    token,
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn abort(&mut self) -> Result<()> {
        if let Some(upload) = self.standalone.take() {
            upload.abort().await?;
        }
        Ok(())
    }
}

fn check_cancelled(token: Option<&CancellationToken>) -> Result<()> {
    ensure!(
        token.is_none_or(|t| !t.is_cancelled()),
        error::ParquetWriteCancelledSnafu
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use object_store::layers::mock::{Metadata, MockLayerBuilder, MockWriterFactory, oio};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::file_format::parquet::packed_reader::{PackReadWindows, PackedParquetReader};
    use crate::parquet_writer::{ParquetFileWriter, ParquetWriterLimits};

    #[tokio::test]
    async fn independent_streams_empty_and_spilled_roundtrip() {
        let directory = common_test_util::temp_dir::create_temp_dir("packed-writer");
        let store = object_store::secure_fs::SecureFsRoot::open(directory.path())
            .unwrap()
            .build_operator();
        let sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let factory: MockWriterFactory = Arc::new({
            let sizes = sizes.clone();
            move |_, _, writer| Box::new(Observe(writer, sizes.clone()))
        });
        let store = store.layer(
            MockLayerBuilder::default()
                .writer_factory(factory)
                .build()
                .unwrap(),
        );
        let shared = PackedWriter::new(store.clone()).unwrap();
        let small = RecordBatch::try_from_iter([(
            "tag",
            Arc::new(StringArray::from(vec![Some(""), None, Some("value")])) as ArrayRef,
        )])
        .unwrap();
        let mut state = 17u64;
        let values = (0..600_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as i64
            })
            .collect::<Vec<_>>();
        let random = Arc::new(Int64Array::from(values)) as ArrayRef;
        let large = RecordBatch::try_from_iter([("a", random.clone()), ("b", random)]).unwrap();
        let batches = [small.clone(), RecordBatch::new_empty(large.schema()), large];
        for (id, batch) in batches.iter().enumerate() {
            let sink =
                PackedTableWriter::new(shared.clone(), format!("table{id}"), id as u32, false);
            let mut writer = ParquetFileWriter::open_packed(
                batch.schema(),
                store.clone(),
                "unused",
                Some(ParquetWriterLimits {
                    row_group_rows: 8192,
                    flush_threshold_bytes: WRITE_BYTES,
                    max_row_groups: 4096,
                }),
                sink,
            )
            .unwrap();
            writer.write(batch.clone(), None).await.unwrap();
            writer.finish(None).await.unwrap();
        }
        let inventory = shared
            .lock()
            .await
            .finish(&CancellationToken::new())
            .await
            .unwrap();
        let index: PackIndex =
            serde_json::from_slice(&store.read(PACK_INDEX_FILE).await.unwrap().to_bytes()).unwrap();
        index
            .validate_membership(["table0", "table1", "table2"])
            .unwrap();
        assert_eq!(inventory.len(), 3);
        assert!(sizes.lock().unwrap().iter().all(|n| *n <= WRITE_BYTES));
        assert_eq!(
            index
                .objects
                .iter()
                .filter(|o| o.kind == ObjectKind::Pack)
                .count(),
            1
        );
        assert!(
            index
                .objects
                .iter()
                .any(|o| o.kind == ObjectKind::Parquet && o.length > WRITE_BYTES as u64)
        );
        let windows = PackReadWindows::new(store.clone());
        for (entry, expected) in index.tables.iter().zip(batches) {
            let object = index
                .objects
                .iter()
                .find(|o| o.path == entry.object)
                .unwrap();
            let actual = if object.kind == ObjectKind::Pack {
                let reader = PackedParquetReader::new(
                    windows.clone(),
                    object.path.clone(),
                    object.length,
                    entry.offset,
                    entry.length,
                )
                .unwrap();
                ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .unwrap()
                    .build()
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
            } else {
                ParquetRecordBatchReaderBuilder::try_new(
                    store.read(&object.path).await.unwrap().to_bytes(),
                )
                .unwrap()
                .build()
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
            };
            assert_eq!(entry.row_count, expected.num_rows() as u64);
            assert_eq!(
                arrow::compute::concat_batches(&expected.schema(), &actual).unwrap(),
                expected
            );
        }
    }

    struct Observe(oio::Writer, Arc<std::sync::Mutex<Vec<usize>>>);
    impl oio::Write for Observe {
        async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
            self.1.lock().unwrap().push(bytes.len());
            self.0.write(bytes).await
        }
        async fn close(&mut self) -> object_store::Result<Metadata> {
            self.0.close().await
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            self.0.abort().await
        }
    }

    #[tokio::test]
    async fn rolls_at_table_boundary_and_aborts_standalone_at_part_limit() {
        let directory = common_test_util::temp_dir::create_temp_dir("packed-limit");
        let sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let factory: MockWriterFactory = Arc::new({
            let sizes = sizes.clone();
            move |_, _, writer| Box::new(Observe(writer, sizes.clone()))
        });
        let store = object_store::secure_fs::SecureFsRoot::open(directory.path())
            .unwrap()
            .build_operator()
            .layer(
                MockLayerBuilder::default()
                    .writer_factory(factory)
                    .build()
                    .unwrap(),
            );
        let shared = PackedWriter::new(store.clone()).unwrap();
        // Two data parts per object; no oversized fixture is needed.
        shared.lock().await.limits = UploadLimits {
            part: 512,
            object: 1024,
        };
        for id in 0..3 {
            let mut table = PackedTableWriter::new(shared.clone(), format!("t{id}"), id, false);
            table.write(vec![id as u8; 600].into()).await.unwrap();
            table.finish(0, None).await.unwrap();
        }
        let mut table = PackedTableWriter::new(shared.clone(), "large".into(), 4, true);
        table.write(vec![0; 1024].into()).await.unwrap();
        assert!(matches!(
            table.write(Bytes::from_static(b"x")).await,
            Err(error::Error::ParquetWriterResource { .. })
        ));
        table.abort().await.unwrap();
        assert!(!store.exists("table-4.parquet").await.unwrap());
        shared
            .lock()
            .await
            .finish(&CancellationToken::new())
            .await
            .unwrap();
        let index: PackIndex =
            serde_json::from_slice(&store.read(PACK_INDEX_FILE).await.unwrap().to_bytes()).unwrap();
        assert_eq!(index.objects.len(), 3);
        assert!(
            index
                .tables
                .iter()
                .all(|t| t.offset == 0 && t.length == 600)
        );
        assert!(sizes.lock().unwrap().iter().all(|n| *n <= 512));
    }

    struct FailClose(oio::Writer);
    impl oio::Write for FailClose {
        async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
            self.0.write(bytes).await
        }
        async fn close(&mut self) -> object_store::Result<Metadata> {
            Err(object_store::Error::new(
                object_store::ErrorKind::Unexpected,
                "injected close failure",
            ))
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            self.0.abort().await
        }
    }

    #[tokio::test]
    async fn close_failure_cancellation_and_collision_never_publish_index() {
        for failure in ["pack-000000.bin", PACK_INDEX_FILE, "cancel", "collision"] {
            let directory = common_test_util::temp_dir::create_temp_dir("packed-failure");
            let store = object_store::secure_fs::SecureFsRoot::open(directory.path())
                .unwrap()
                .build_operator();
            if failure == "collision" {
                store.write("pack-000000.bin", "winner").await.unwrap();
            }
            let factory: MockWriterFactory = Arc::new(move |path, _, writer| {
                if path.trim_start_matches('/') == failure {
                    Box::new(FailClose(writer))
                } else {
                    writer
                }
            });
            let store = store.layer(
                MockLayerBuilder::default()
                    .writer_factory(factory)
                    .build()
                    .unwrap(),
            );
            let shared = PackedWriter::new(store.clone()).unwrap();
            let mut table = PackedTableWriter::new(shared.clone(), "t".into(), 0, false);
            table.write(vec![0; 12].into()).await.unwrap();
            table.finish(0, None).await.unwrap();
            let token = CancellationToken::new();
            if failure == "cancel" {
                token.cancel();
            }
            assert!(shared.lock().await.finish(&token).await.is_err());
            let _ = shared.lock().await.abort().await;
            assert!(!store.exists(PACK_INDEX_FILE).await.unwrap());
            if failure == "collision" {
                assert_eq!(
                    store.read("pack-000000.bin").await.unwrap().to_bytes(),
                    Bytes::from_static(b"winner")
                );
            }
        }
    }
}
