use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_stream::stream;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use bytes::{Bytes, BytesMut};
use common_error::ext::BoxedError;
use common_telemetry::debug;
use common_telemetry::logging::{error, info};
use futures::Stream;
use futures_util::StreamExt;
use memmap2::{Mmap, MmapOptions};
use snafu::ResultExt;
use store_api::logstore::entry::{Encode, Entry, Id, Offset};
use store_api::logstore::entry_stream::EntryStream;
use store_api::logstore::namespace::Namespace;
use tokio::sync::mpsc::error::{SendError, TryRecvError};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time;

use crate::error::Error::Eof;
use crate::error::{AppendSnafu, Error, InternalSnafu, IoSnafu, OpenLogSnafu, Result};
use crate::fs::chunk::{Chunk, CompositeChunk};
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
}

impl FileWriter {
    pub fn new(file: Arc<File>) -> Self {
        Self { inner: file }
    }

    pub async fn write(&self, data: Arc<Bytes>, offset: u64) -> Result<()> {
        let file = self.inner.clone();
        let handle = common_runtime::spawn_blocking_write(move || {
            crate::fs::io::pwrite_all(&file, &data, offset)
        });
        handle
            .await
            .map_err(|e| {
                InternalSnafu {
                    msg: format!("Error while waiting for write finish, error:{}", e),
                }
                .build()
            })
            .and_then(|e| e) // Result::flatten is unstable now
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

        futures::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .map(|_| max_offset)
    }

    pub async fn flush(&self) -> Result<()> {
        let file = self.inner.clone();
        let handle =
            common_runtime::spawn_blocking_write(move || file.sync_data().context(IoSnafu));
        handle
            .await
            .map_err(|e| {
                InternalSnafu {
                    msg: format!("Error while waiting for flush finish, error: {}", e),
                }
                .build()
            })
            .and_then(|e| e)
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

        let file_name: FileName = path.as_str().try_into()?;
        let start_entry_id = file_name.entry_id();

        let mut log = Self {
            name: file_name,
            writer: Arc::new(FileWriter::new(Arc::new(file))),
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
            "Log file {} replay finished, last offset: {}, file start entry id: {}, next entry id: {}, elapsed time: {}ms",
            path, actual_offset, start_entry_id, next_entry_id,
            time::Instant::now().duration_since(replay_start_time).as_millis()
        );

        log.state
            .write_offset
            .store(actual_offset, Ordering::Relaxed);
        log.state
            .flush_offset
            .store(actual_offset, Ordering::Relaxed);
        log.state
            .next_entry_id
            .store(next_entry_id, Ordering::Relaxed);
        Ok(log)
    }

    /// Creates a file mmap region.
    async fn map(&self, start: u64, length: usize) -> Result<Mmap> {
        unsafe {
            let file = self.writer.inner.clone();
            MmapOptions::new()
                .offset(start)
                .len(length)
                .map(&*file)
                .context(IoSnafu)
        }
    }

    /// Returns the persisted size of current log file.
    #[allow(unused)]
    #[inline]
    pub fn persisted_size(&self) -> usize {
        self.state.flush_offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn next_entry_id(&self) -> Id {
        self.state.next_entry_id.load(Ordering::Relaxed)
    }

    /// Increases next entry field by `delta` and return the previous value.
    fn inc_entry_id(&self) -> u64 {
        // Relaxed order is enough since no sync-with relationship
        // between `offset` and any other field.
        self.state.next_entry_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Starts log file and it's internal components(including flush task, etc.).
    pub async fn start(&mut self) -> Result<()> {
        let notify = self.notify.clone();
        let writer = self.writer.clone();
        let state = self.state.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(self.append_buffer_size);

        let handle = tokio::spawn(async move {
            while !state.stopped.load(Ordering::Acquire) {
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

    async fn handle_batch(
        mut batch: Vec<AppendRequest>,
        state: &Arc<State>,
        writer: &Arc<FileWriter>,
    ) {
        // preserve previous write offset
        let prev_write_offset = state.write_offset.load(Ordering::Acquire);

        for mut req in &mut batch {
            req.offset = state
                .write_offset
                .fetch_add(req.data.len(), Ordering::AcqRel);
            info!(
                "Write offset, {}->{}",
                req.offset,
                state.write_offset.load(Ordering::Acquire)
            );
        }

        match writer.write_batch(&batch).await {
            Ok(max_offset) => match writer.flush().await {
                Ok(_) => {
                    let prev = state.flush_offset.swap(max_offset, Ordering::Acquire);
                    debug!(
                        "Flush offset: {} -> {}, max offset in batch: {}",
                        prev,
                        state.flush_offset.load(Ordering::Acquire),
                        max_offset
                    );
                    batch.into_iter().for_each(AppendRequest::complete);
                }
                Err(e) => {
                    error!("Failed to flush log file: {}", e);
                    batch.into_iter().for_each(|r| r.fail());
                    state
                        .write_offset
                        .store(prev_write_offset, Ordering::Release);
                }
            },
            Err(e) => {
                error!("Failed to write append requests, error: {}", e);
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
                            if state.stopped.load(Ordering::Acquire) {
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
        let previous_offset = self.state.flush_offset.load(Ordering::Relaxed);
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
                    error!("Error while replay log {} {:?}", log_name, e);
                    break;
                }
            }
        }
        info!(
            "Replay log {} finished, offset: {} -> {}",
            log_name, previous_offset, last_offset
        );
        Ok((
            last_offset,
            match last_entry_id {
                None => self.start_entry_id,
                Some(v) => v + 1,
            },
        ))
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

        let s = stream!({
            let mmap = self.map(0, length).await?;
            let mut buf = &mmap[..];
            if buf.is_empty() {
                info!("File is just created!");
                // file is newly created
                return;
            }

            while !buf.is_empty() {
                let entry = EntryImpl::decode(&mut buf)?;
                if entry.id() >= start_entry_id {
                    yield Ok(vec![entry]);
                }
            }
        });

        StreamImpl {
            inner: Box::pin(s),
            start_entry_id,
        }
    }

    /// Appends an entry to `LogFile` and return a `Result` containing the id of entry appended.
    pub async fn append<T: Entry>(&self, e: &mut T) -> Result<AppendResponseImpl>
    where
        T: Encode<Error = Error>,
    {
        if self.state.stopped.load(Ordering::Acquire) {
            return Err(Error::Eof);
        }
        e.set_id(0);
        let mut serialized = BytesMut::with_capacity(e.encoded_size());
        e.encode_to(&mut serialized)
            .map_err(BoxedError::new)
            .context(AppendSnafu)?;
        let size = serialized.len();

        if size + self.state.write_offset.load(Ordering::Relaxed) > self.max_file_size {
            return Err(Error::Eof);
        }

        let entry_id = self.inc_entry_id();
        // rewrite encoded data
        LittleEndian::write_u64(&mut serialized[0..8], entry_id);
        let checksum = CRC_ALGO.checksum(&serialized[0..size - 4]);
        LittleEndian::write_u32(&mut serialized[size - 4..], checksum);

        let (tx, rx) = oneshot::channel();
        self.pending_request_tx
            .as_ref()
            .expect("Call start before write to LogFile!")
            .send(AppendRequest {
                data: Arc::new(serialized.freeze()),
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
    pub fn unseal(&self) {
        self.state.sealed.store(false, Ordering::Release);
    }

    #[inline]
    pub fn file_name(&self) -> String {
        self.name.to_string()
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
    data: Arc<Bytes>,
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
    next_entry_id: AtomicU64,
    sealed: AtomicBool,
    stopped: AtomicBool,
}

fn file_chunk_stream(
    file: Arc<File>,
    mut offset: usize,
    file_size: usize,
) -> tokio::sync::mpsc::Receiver<Result<Chunk<CHUNK_SIZE>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    common_runtime::spawn_blocking_read(move || loop {
        if offset >= file_size {
            return;
        }
        let file = file.clone();
        match read_at(file, offset, file_size) {
            Ok(data) => {
                let data_len = data.len();
                if let Err(e) = common_runtime::block_on_read(async { tx.send(Ok(data)).await }) {
                    // Rx dropped
                    break;
                }

                offset += data_len;
                continue;
            }
            Err(e) => {
                error!("Failed to read file chunk, error: {}", &e);
                if let Err(e) = common_runtime::block_on_read(async { tx.send(Err(e)).await }) {
                    // Rx dropped
                    break;
                }
                break;
            }
        }
    });
    rx
}

fn read_at(file: Arc<File>, offset: usize, file_length: usize) -> Result<Chunk<CHUNK_SIZE>> {
    if offset > file_length {
        return Err(Eof);
    }
    let size = CHUNK_SIZE.min((file_length - offset) as usize);
    let mut data = [0u8; CHUNK_SIZE];
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
            .unwrap_or_else(|_| panic!("Failed to open file: {}", path));
        file.start().await.expect("Failed to start log file");

        assert_eq!(
            10,
            file.append(&mut EntryImpl::new("test1".as_bytes()))
                .await
                .expect("Failed to append entry 1")
                .entry_id
        );

        assert_eq!(
            11,
            file.append(&mut EntryImpl::new("test-2".as_bytes()))
                .await
                .expect("Failed to append entry 2")
                .entry_id
        );

        let mut log_file = std::fs::File::open(path.clone()).expect("Test log file does not exist");
        let metadata = log_file.metadata().expect("Failed to read file metadata");
        info!("Log file metadata: {:?}", metadata);

        assert_eq!(59, metadata.len()); // 24+5+24+6
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

        let mut stream = file.create_stream(LocalNamespace::default(), 0);

        let mut data = vec![];

        while let Some(v) = stream.next().await {
            let entries = v.unwrap();
            let content = entries[0].data();
            let vec = content.to_vec();
            info!("Read entry: {}", String::from_utf8_lossy(&vec));
            data.push(String::from_utf8(vec).unwrap());
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
        let result = read_at(file, 0, 12).unwrap();

        assert_eq!(12, result.len());
        assert_eq!("1234567890ab".as_bytes(), &result.data[0..result.len()]);
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
        let stream = file_chunk_stream(file, 0, file_size as usize);
        pin_mut!(stream);

        let mut chunks = vec![];
        while let Some(r) = stream.recv().await {
            chunks.push(r.unwrap());
        }
        assert_eq!(
            vec![4096, 1024],
            chunks.iter().map(|c| c.write).collect::<Vec<_>>()
        );
        assert_eq!(
            vec![vec![42].repeat(4096), vec![42].repeat(1024)],
            chunks
                .iter()
                .map(|c| &c.data[0..c.write])
                .collect::<Vec<_>>()
        );
    }
}
