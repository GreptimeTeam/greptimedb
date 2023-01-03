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

use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_stream::stream;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Bytes, BytesMut};
use common_error::ext::BoxedError;
use common_telemetry::logging::{error, info};
use common_telemetry::{debug, trace};
use futures::Stream;
use futures_util::StreamExt;
use snafu::ResultExt;
use store_api::logstore::entry::{Encode, Entry, Id, Offset};
use store_api::logstore::entry_stream::EntryStream;
use store_api::logstore::namespace::Namespace;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender as MpscSender};
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time;

use crate::error::Error::Eof;
use crate::error::{
    AppendSnafu, Error, InternalSnafu, IoSnafu, OpenLogSnafu, Result, WaitWriteSnafu, WriteSnafu,
};
use crate::fs::chunk::{Chunk, ChunkList};
use crate::fs::config::LogConfig;
use crate::fs::crc::CRC_ALGO;
use crate::fs::entry::{EntryImpl, StreamImpl};
use crate::fs::file_name::FileName;
use crate::fs::namespace::LocalNamespace;
use crate::fs::AppendResponseImpl;

pub const CHUNK_SIZE: usize = 4096;
const LOG_WRITER_BATCH_SIZE: usize = 16;

/// Wraps File operation to get rid of `&mut self` requirements
struct FileWriter {
    inner: Arc<File>,
    path: String,
}

impl FileWriter {
    pub fn new(file: Arc<File>, path: String) -> Self {
        Self { inner: file, path }
    }

    pub async fn write(&self, data: Bytes, offset: u64) -> Result<()> {
        let file = self.inner.clone();
        let handle = common_runtime::spawn_blocking_write(move || {
            crate::fs::io::pwrite_all(&file, &data, offset)
        });
        handle.await.context(WriteSnafu)?
    }

    /// Writes a batch of `AppendRequest` to file.
    pub async fn write_batch(self: &Arc<Self>, batch: &Vec<AppendRequest>) -> Result<usize> {
        let mut futures = Vec::with_capacity(batch.len());

        let mut max_offset = 0;
        for req in batch {
            let offset = req.offset;
            let end = req.data.len() + offset;
            max_offset = max_offset.max(end);
            let future = self.write(req.data.clone(), offset as u64);
            futures.push(future);
        }
        debug!(
            "Write batch, size: {}, max offset: {}",
            batch.len(),
            max_offset
        );
        futures::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .map(|_| max_offset)
    }

    pub async fn flush(&self) -> Result<()> {
        let file = self.inner.clone();
        common_runtime::spawn_blocking_write(move || file.sync_all().context(IoSnafu))
            .await
            .context(WaitWriteSnafu)?
    }

    pub async fn destroy(&self) -> Result<()> {
        tokio::fs::remove_file(&self.path).await.context(IoSnafu)?;
        Ok(())
    }
}

pub type LogFileRef = Arc<LogFile>;

pub struct LogFile {
    // name of log file
    name: FileName,
    // file writer
    writer: Arc<FileWriter>,
    // append request channel
    pending_request_tx: Option<MpscSender<AppendRequest>>,
    // flush task notifier
    notify: Arc<Notify>,
    // flush task join handle
    join_handle: Mutex<Option<JoinHandle<Result<()>>>>,
    // internal state(offset, id counter...)
    state: Arc<State>,
    // the start entry id of current log file
    start_entry_id: u64,
    // max file size of current log file
    max_file_size: usize,
    // buffer size for append request channel. read from config on start.
    append_buffer_size: usize,
}

impl Drop for LogFile {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::Relaxed);
        info!("Dropping log file {}", self.name);
    }
}

impl LogFile {
    /// Opens a file in path with given log config.
    pub async fn open(path: impl Into<String>, config: &LogConfig) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path.clone())
            .context(OpenLogSnafu { file_name: &path })?;

        let file_name = FileName::try_from(path.as_str())?;
        let start_entry_id = file_name.entry_id();

        let mut log = Self {
            name: file_name,
            writer: Arc::new(FileWriter::new(Arc::new(file), path.clone())),
            start_entry_id,
            pending_request_tx: None,
            notify: Arc::new(Notify::new()),
            max_file_size: config.max_log_file_size,
            join_handle: Mutex::new(None),
            state: Arc::new(State::default()),
            append_buffer_size: config.append_buffer_size,
        };

        let metadata = log.writer.inner.metadata().context(IoSnafu)?;
        let expect_length = metadata.len() as usize;
        log.state
            .write_offset
            .store(expect_length, Ordering::Relaxed);
        log.state
            .flush_offset
            .store(expect_length, Ordering::Relaxed);

        let replay_start_time = time::Instant::now();
        let (actual_offset, next_entry_id) = log.replay().await?;

        info!(
            "Log file {} replay finished, last offset: {}, file start entry id: {}, elapsed time: {}ms",
            path, actual_offset, start_entry_id,
            time::Instant::now().duration_since(replay_start_time).as_millis()
        );

        log.state
            .write_offset
            .store(actual_offset, Ordering::Relaxed);
        log.state
            .flush_offset
            .store(actual_offset, Ordering::Relaxed);
        log.state
            .last_entry_id
            .store(next_entry_id, Ordering::Relaxed);
        Ok(log)
    }

    /// Returns the persisted size of current log file.
    #[inline]
    pub fn persisted_size(&self) -> usize {
        self.state.flush_offset()
    }

    /// Starts log file and it's internal components(including flush task, etc.).
    pub async fn start(&mut self) -> Result<()> {
        let notify = self.notify.clone();
        let writer = self.writer.clone();
        let state = self.state.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(self.append_buffer_size);

        let handle = tokio::spawn(async move {
            while !state.is_stopped() {
                let batch = Self::recv_batch(&mut rx, &state, &notify, true).await;
                debug!("Receive write request, size: {}", batch.len());
                if !batch.is_empty() {
                    Self::handle_batch(batch, &state, &writer).await;
                }
            }

            // log file stopped
            let batch = Self::recv_batch(&mut rx, &state, &notify, false).await;
            if !batch.is_empty() {
                Self::handle_batch(batch, &state, &writer).await;
            }
            info!("Writer task finished");
            Ok(())
        });

        self.pending_request_tx = Some(tx);
        *self.join_handle.lock().unwrap() = Some(handle);
        info!("Flush task started: {}", self.name);
        Ok(())
    }

    /// Stops log file.
    /// # Panics
    /// Panics when a log file is stopped while not being started ever.
    pub async fn stop(&self) -> Result<()> {
        self.state.stopped.store(true, Ordering::Release);
        let join_handle = self
            .join_handle
            .lock()
            .unwrap()
            .take()
            .expect("Join handle should present");
        self.notify.notify_waiters();
        let res = join_handle.await.unwrap();
        info!("LogFile task finished: {:?}", res);
        res
    }

    pub async fn destroy(&self) -> Result<()> {
        self.writer.destroy().await?;
        Ok(())
    }

    async fn handle_batch(
        mut batch: Vec<AppendRequest>,
        state: &Arc<State>,
        writer: &Arc<FileWriter>,
    ) {
        // preserve previous write offset
        let prev_write_offset = state.write_offset();

        let mut last_id = 0;
        for mut req in &mut batch {
            req.offset = state
                .write_offset
                .fetch_add(req.data.len(), Ordering::AcqRel);
            last_id = req.id;
            debug!("Entry id: {}, offset: {}", req.id, req.offset,);
        }

        match writer.write_batch(&batch).await {
            Ok(max_offset) => match writer.flush().await {
                Ok(_) => {
                    let prev_ofs = state.flush_offset.swap(max_offset, Ordering::Acquire);
                    let prev_id = state.last_entry_id.swap(last_id, Ordering::Acquire);
                    debug!(
                        "Flush offset: {} -> {}, max offset in batch: {}, entry id: {}->{}",
                        prev_ofs,
                        state.flush_offset.load(Ordering::Acquire),
                        max_offset,
                        prev_id,
                        state.last_entry_id.load(Ordering::Acquire),
                    );
                    batch.into_iter().for_each(AppendRequest::complete);
                }
                Err(e) => {
                    error!(e; "Failed to flush log file");
                    batch.into_iter().for_each(|r| r.fail());
                    state
                        .write_offset
                        .store(prev_write_offset, Ordering::Release);
                }
            },
            Err(e) => {
                error!(e; "Failed to write append requests");
                batch.into_iter().for_each(|r| r.fail());
                state
                    .write_offset
                    .store(prev_write_offset, Ordering::Release);
            }
        }
    }

    async fn recv_batch(
        rx: &mut Receiver<AppendRequest>,
        state: &Arc<State>,
        notify: &Arc<Notify>,
        wait_on_empty: bool,
    ) -> Vec<AppendRequest> {
        let mut batch: Vec<AppendRequest> = Vec::with_capacity(LOG_WRITER_BATCH_SIZE);
        for _ in 0..LOG_WRITER_BATCH_SIZE {
            match rx.try_recv() {
                Ok(req) => {
                    batch.push(req);
                }
                Err(e) => match e {
                    TryRecvError::Empty => {
                        if batch.is_empty() && wait_on_empty {
                            notify.notified().await;
                            if state.is_stopped() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    TryRecvError::Disconnected => {
                        error!("Channel unexpectedly disconnected!");
                        break;
                    }
                },
            }
        }
        batch
    }

    #[inline]
    pub fn start_entry_id(&self) -> Id {
        self.start_entry_id
    }

    /// Replays current file til last entry read
    pub async fn replay(&mut self) -> Result<(usize, Id)> {
        let log_name = self.name.to_string();
        let previous_offset = self.state.flush_offset();
        let ns = LocalNamespace::default();
        let mut stream = self.create_stream(
            // TODO(hl): LocalNamespace should be filled
            &ns, 0,
        );

        let mut last_offset = 0usize;
        let mut last_entry_id: Option<Id> = None;
        while let Some(res) = stream.next().await {
            match res {
                Ok(entries) => {
                    for e in entries {
                        last_offset += e.len();
                        last_entry_id = Some(e.id());
                    }
                }
                Err(e) => {
                    error!(e; "Error while replay log {}", log_name);
                    break;
                }
            }
        }
        info!(
            "Replay log {} finished, offset: {} -> {}, last entry id: {:?}",
            log_name, previous_offset, last_offset, last_entry_id
        );
        Ok((last_offset, last_entry_id.unwrap_or(self.start_entry_id)))
    }

    /// Creates a reader stream that asynchronously generates entries start from given entry id.
    /// ### Notice
    /// If the entry with start entry id is not present, the first generated entry will start with
    /// the first entry with an id greater than `start_entry_id`.
    pub fn create_stream(
        &self,
        _ns: &impl Namespace,
        start_entry_id: u64,
    ) -> impl EntryStream<Entry = EntryImpl, Error = Error> + '_ {
        let length = self.state.flush_offset.load(Ordering::Relaxed);

        let mut chunk_stream = file_chunk_stream(self.writer.inner.clone(), 0, length, 0);
        let entry_stream = stream!({
            let mut chunks = ChunkList::new();
            while let Some(chunk) = chunk_stream.next().await {
                let chunk = chunk.unwrap();
                chunks.push(chunk);
                let mut batch = vec![];
                loop {
                    match EntryImpl::decode(&mut chunks) {
                        Ok(e) => {
                            if e.id() >= start_entry_id {
                                batch.push(e);
                            }
                        }
                        Err(Error::DecodeAgain { .. }) => {
                            // no more data for decoding
                            break;
                        }
                        Err(e) => {
                            yield Err(e);
                            break;
                        }
                    }
                }
                trace!("Yield batch size: {}", batch.len());
                yield Ok(batch);
            }
        });

        StreamImpl {
            inner: Box::pin(entry_stream),
            start_entry_id,
        }
    }

    /// Appends an entry to `LogFile` and return a `Result` containing the id of entry appended.
    pub async fn append<T: Entry>(&self, e: &mut T) -> Result<AppendResponseImpl>
    where
        T: Encode<Error = Error>,
    {
        if self.state.is_stopped() {
            return Err(Error::Eof);
        }
        let entry_id = e.id();
        let mut serialized = BytesMut::with_capacity(e.encoded_size());
        e.encode_to(&mut serialized)
            .map_err(BoxedError::new)
            .context(AppendSnafu)?;
        let size = serialized.len();

        if size + self.state.write_offset() > self.max_file_size {
            return Err(Error::Eof);
        }

        // rewrite encoded data
        LittleEndian::write_u64(&mut serialized[0..8], entry_id);
        let checksum = CRC_ALGO.checksum(&serialized[0..size - 4]);
        LittleEndian::write_u32(&mut serialized[size - 4..], checksum);

        let (tx, rx) = oneshot::channel();
        self.pending_request_tx
            .as_ref()
            .expect("Call start before write to LogFile!")
            .send(AppendRequest {
                data: serialized.freeze(),
                tx,
                offset: 0,
                id: entry_id,
            })
            .await
            .map_err(|_| {
                InternalSnafu {
                    msg: "Send append request",
                }
                .build()
            })?;

        self.notify.notify_one(); // notify write thread.

        rx.await
            .expect("Sender dropped while waiting for append result")
            .map_err(|_| {
                InternalSnafu {
                    msg: "Failed to write request".to_string(),
                }
                .build()
            })
    }

    #[inline]
    pub fn try_seal(&self) -> bool {
        self.state
            .sealed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    #[inline]
    pub fn is_seal(&self) -> bool {
        self.state.sealed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.state.stopped.load(Ordering::Acquire)
    }

    #[inline]
    pub fn unseal(&self) {
        self.state.sealed.store(false, Ordering::Release);
    }

    #[inline]
    pub fn file_name(&self) -> String {
        self.name.to_string()
    }

    #[inline]
    pub fn last_entry_id(&self) -> Id {
        self.state.last_entry_id.load(Ordering::Acquire)
    }
}

impl Debug for LogFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFile")
            .field("name", &self.name)
            .field("start_entry_id", &self.start_entry_id)
            .field("max_file_size", &self.max_file_size)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct AppendRequest {
    tx: OneshotSender<std::result::Result<AppendResponseImpl, ()>>,
    offset: Offset,
    id: Id,
    data: Bytes,
}

impl AppendRequest {
    #[inline]
    pub fn complete(self) {
        let _ = self.tx.send(Ok(AppendResponseImpl {
            offset: self.offset,
            entry_id: self.id,
        }));
    }

    #[inline]
    pub fn fail(self) {
        let _ = self.tx.send(Err(()));
    }
}

#[derive(Default, Debug)]
struct State {
    write_offset: AtomicUsize,
    flush_offset: AtomicUsize,
    last_entry_id: AtomicU64,
    sealed: AtomicBool,
    stopped: AtomicBool,
}

impl State {
    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    #[inline]
    pub fn write_offset(&self) -> usize {
        self.write_offset.load(Ordering::Acquire)
    }

    #[inline]
    pub fn flush_offset(&self) -> usize {
        self.flush_offset.load(Ordering::Acquire)
    }
}

type SendableChunkStream = Pin<Box<dyn Stream<Item = Result<Chunk>> + Send>>;

/// Creates a stream of chunks of data from file. If `buffer_size` is not 0, the returned stream
/// will have a bounded buffer and a background thread will do prefetching. When consumer cannot
/// catch up with spawned prefetch loop, the prefetch thread will be blocked and wait until buffer
/// has enough capacity.
///
/// If the `buffer_size` is 0, there will not be a prefetching thread. File chunks will not be read
/// until stream consumer asks for next chunk.
fn file_chunk_stream(
    file: Arc<File>,
    mut offset: usize,
    file_size: usize,
    buffer_size: usize,
) -> SendableChunkStream {
    if buffer_size == 0 {
        return file_chunk_stream_sync(file, offset, file_size);
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(buffer_size);
    common_runtime::spawn_blocking_read(move || loop {
        if offset >= file_size {
            return;
        }
        match read_at(&file, offset, file_size) {
            Ok(data) => {
                let data_len = data.len();
                if tx.blocking_send(Ok(data)).is_err() {
                    break;
                }
                offset += data_len;
                continue;
            }
            Err(e) => {
                error!(e; "Failed to read file chunk");
                // we're going to break any way so just forget the join result.
                let _ = tx.blocking_send(Err(e));
                break;
            }
        }
    });
    Box::pin(stream!({
        while let Some(v) = rx.recv().await {
            yield v;
        }
    }))
}

fn file_chunk_stream_sync(
    file: Arc<File>,
    mut offset: usize,
    file_size: usize,
) -> SendableChunkStream {
    let s = stream!({
        loop {
            if offset >= file_size {
                return;
            }
            match read_at(&file, offset, file_size) {
                Ok(data) => {
                    let data_len = data.len();
                    yield Ok(data);
                    offset += data_len;
                    continue;
                }
                Err(e) => {
                    error!(e; "Failed to read file chunk");
                    yield Err(e);
                    break;
                }
            }
        }
    });

    Box::pin(s)
}

/// Reads a chunk of data from file in a blocking manner.
/// The file may not contain enough data to fulfill the whole chunk so only data available
/// is read into chunk. The `write` field of `Chunk` indicates the end of valid data.  
fn read_at(file: &Arc<File>, offset: usize, file_length: usize) -> Result<Chunk> {
    if offset > file_length {
        return Err(Eof);
    }
    let size = CHUNK_SIZE.min(file_length - offset);
    let mut data = Box::new([0u8; CHUNK_SIZE]);
    crate::fs::io::pread_exact(file.as_ref(), &mut data[0..size], offset as u64)?;
    Ok(Chunk::new(data, size))
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use common_telemetry::logging;
    use futures::pin_mut;
    use futures_util::StreamExt;
    use tempdir::TempDir;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::fs::namespace::LocalNamespace;

    #[tokio::test]
    pub async fn test_create_entry_stream() {
        logging::init_default_ut_logging();
        let config = LogConfig::default();

        let dir = TempDir::new("greptimedb-store-test").unwrap();
        let path_buf = dir.path().join("0010.log");
        let path = path_buf.to_str().unwrap().to_string();
        File::create(path.as_str()).unwrap();

        let mut file = LogFile::open(path.clone(), &config)
            .await
            .unwrap_or_else(|_| panic!("Failed to open file: {path}"));
        file.start().await.expect("Failed to start log file");

        assert_eq!(
            10,
            file.append(&mut EntryImpl::new(
                "test1".as_bytes(),
                10,
                LocalNamespace::new(42)
            ))
            .await
            .expect("Failed to append entry 1")
            .entry_id
        );

        assert_eq!(
            11,
            file.append(&mut EntryImpl::new(
                "test-2".as_bytes(),
                11,
                LocalNamespace::new(42)
            ))
            .await
            .expect("Failed to append entry 2")
            .entry_id
        );

        let mut log_file = std::fs::File::open(path.clone()).expect("Test log file does not exist");
        let metadata = log_file.metadata().expect("Failed to read file metadata");
        info!("Log file metadata: {:?}", metadata);

        assert_eq!(75, metadata.len()); // 32+5+32+6
        let mut content = vec![0; metadata.len() as usize];
        log_file
            .read_exact(&mut content)
            .expect("Read log file failed");

        info!(
            "Log file {:?} content: {}, size:{}",
            dir,
            hex::encode(content),
            metadata.len()
        );

        let ns = LocalNamespace::new(42);
        let mut stream = file.create_stream(&ns, 0);
        let mut data = vec![];

        while let Some(v) = stream.next().await {
            let entries = v.unwrap();
            for e in entries {
                let vec = e.data().to_vec();
                info!("Read entry: {}", String::from_utf8_lossy(&vec));
                data.push(String::from_utf8(vec).unwrap());
            }
        }

        assert_eq!(vec!["test1".to_string(), "test-2".to_string()], data);
        drop(stream);

        let result = file.stop().await;
        info!("Stop file res: {:?}", result);
    }

    #[tokio::test]
    pub async fn test_read_at() {
        let dir = tempdir::TempDir::new("greptimedb-store-test").unwrap();
        let file_path = dir.path().join("chunk-stream-file-test");
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&file_path)
            .await
            .unwrap();
        file.write_all("1234567890ab".as_bytes()).await.unwrap();
        file.flush().await.unwrap();

        let file = Arc::new(file.into_std().await);
        let result = read_at(&file, 0, 12).unwrap();

        assert_eq!(12, result.len());
        assert_eq!("1234567890ab".as_bytes(), &result.data[0..result.len()]);
    }

    #[tokio::test]
    pub async fn test_read_at_center() {
        let dir = tempdir::TempDir::new("greptimedb-store-test").unwrap();
        let file_path = dir.path().join("chunk-stream-file-test-center");
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&file_path)
            .await
            .unwrap();
        file.write_all("1234567890ab".as_bytes()).await.unwrap();
        file.flush().await.unwrap();

        let file_len = file.metadata().await.unwrap().len();
        let file = Arc::new(file.into_std().await);
        let result = read_at(&file, 8, file_len as usize).unwrap();
        assert_eq!(4, result.len());
        assert_eq!("90ab".as_bytes(), &result.data[0..result.len()]);
    }

    #[tokio::test]
    pub async fn test_file_chunk_stream() {
        let dir = tempdir::TempDir::new("greptimedb-store-test").unwrap();
        let file_path = dir.path().join("chunk-stream-file-test");
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&file_path)
            .await
            .unwrap();
        file.write_all(&vec![42].repeat(4096 + 1024)).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.metadata().await.unwrap().len();
        let file = Arc::new(file.into_std().await);
        let stream = file_chunk_stream(file, 0, file_size as usize, 1024);
        pin_mut!(stream);

        let mut chunks = vec![];
        while let Some(r) = stream.next().await {
            chunks.push(r.unwrap());
        }
        assert_eq!(
            vec![4096, 1024],
            chunks.iter().map(|c| c.write_offset).collect::<Vec<_>>()
        );
        assert_eq!(
            vec![vec![42].repeat(4096), vec![42].repeat(1024)],
            chunks
                .iter()
                .map(|c| &c.data[0..c.write_offset])
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    pub async fn test_sync_chunk_stream() {
        let dir = tempdir::TempDir::new("greptimedb-store-test").unwrap();
        let file_path = dir.path().join("chunk-stream-file-test");
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&file_path)
            .await
            .unwrap();
        file.write_all(&vec![42].repeat(4096 + 1024)).await.unwrap();
        file.flush().await.unwrap();

        let file_size = file.metadata().await.unwrap().len();
        let file = Arc::new(file.into_std().await);
        let stream = file_chunk_stream_sync(file, 0, file_size as usize);
        pin_mut!(stream);

        let mut chunks = vec![];
        while let Some(r) = stream.next().await {
            chunks.push(r.unwrap());
        }
        assert_eq!(
            vec![4096, 1024],
            chunks.iter().map(|c| c.write_offset).collect::<Vec<_>>()
        );
        assert_eq!(
            vec![vec![42].repeat(4096), vec![42].repeat(1024)],
            chunks
                .iter()
                .map(|c| &c.data[0..c.write_offset])
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_shutdown() {
        logging::init_default_ut_logging();
        let config = LogConfig::default();
        let dir = TempDir::new("greptimedb-store-test").unwrap();
        let path_buf = dir.path().join("0010.log");
        let path = path_buf.to_str().unwrap().to_string();
        File::create(path.as_str()).unwrap();

        let mut file = LogFile::open(path.clone(), &config)
            .await
            .unwrap_or_else(|_| panic!("Failed to open file: {path}"));

        let state = file.state.clone();
        file.start().await.unwrap();
        drop(file);

        assert!(state.stopped.load(Ordering::Relaxed));
    }
}
