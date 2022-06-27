use std::fmt::{Debug, Formatter};
use std::io::SeekFrom;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_stream::stream;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use bytes::BytesMut;
use common_base::buffer::Buffer;
use common_error::ext::BoxedError;
use common_telemetry::logging::{error, info};
use common_telemetry::warn;
use futures_util::StreamExt;
use memmap2::{Mmap, MmapOptions};
use mmap::MmappedBuffer;
use snafu::{Backtrace, GenerateImplicitData, ResultExt};
use store_api::logstore::entry::{Encode, Entry, Id, Offset};
use store_api::logstore::entry_stream::EntryStream;
use store_api::logstore::namespace::Namespace;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::{oneshot, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time;

use crate::error::{EncodeSnafu, Error, IoSnafu, OpenLogSnafu, Result};
use crate::fs::config::LogConfig;
use crate::fs::crc::CRC_ALGO;
use crate::fs::entry::{EntryImpl, StreamImpl};
use crate::fs::file_name::FileName;
use crate::fs::namespace::LocalNamespace;
use crate::fs::{mmap, AppendResponseImpl};

const LOG_WRITER_BATCH_SIZE: usize = 16;

// TODO(hl): use pwrite polyfill in different platforms, avoid write syscall in each append request.
pub struct LogFile {
    name: FileName,
    file: Arc<RwLock<File>>,
    path: String,
    write_offset: Arc<AtomicUsize>,
    flush_offset: Arc<AtomicUsize>,
    next_entry_id: Arc<AtomicU64>,
    start_entry_id: u64,
    pending_request_rx: Option<MpscReceiver<AppendRequest>>,
    pending_request_tx: MpscSender<AppendRequest>,
    notify: Arc<Notify>,
    max_file_size: usize,
    join_handle: Mutex<Option<JoinHandle<Result<()>>>>,
    sealed: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

impl Debug for LogFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFile")
            .field("name", &self.name)
            .field("start_entry_id", &self.start_entry_id)
            .field("write_offset", &self.write_offset.load(Ordering::Relaxed))
            .field("flush_offset", &self.flush_offset.load(Ordering::Relaxed))
            .field("next_entry_id", &self.next_entry_id.load(Ordering::Relaxed))
            .field("max_file_size", &self.max_file_size)
            .field("sealed", &self.sealed.load(Ordering::Relaxed))
            .finish()
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
            .await
            .context(OpenLogSnafu { file_name: &path })?;

        let file_name: FileName = path.as_str().try_into()?;
        let start_entry_id = file_name.entry_id();
        let (tx, rx) = tokio::sync::mpsc::channel(config.append_buffer_size);

        let mut log = Self {
            name: file_name,
            file: Arc::new(RwLock::new(file)),
            path: path.to_string(),
            write_offset: Arc::new(AtomicUsize::new(0)),
            flush_offset: Arc::new(AtomicUsize::new(0)),
            next_entry_id: Arc::new(AtomicU64::new(start_entry_id)),
            start_entry_id,
            pending_request_tx: tx,
            pending_request_rx: Some(rx),
            notify: Arc::new(Notify::new()),
            max_file_size: config.max_log_file_size,
            join_handle: Mutex::new(None),
            sealed: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
        };

        let metadata = log.file.read().await.metadata().await.context(IoSnafu)?;
        let expect_length = metadata.len() as usize;
        log.write_offset.store(expect_length, Ordering::Relaxed);
        log.flush_offset.store(expect_length, Ordering::Relaxed);

        let replay_start_time = time::Instant::now();
        let (actual_offset, next_entry_id) = log.replay().await?;

        info!(
            "Log file {} replay finished, last offset: {}, file start entry id: {}, next entry id: {}, elapsed time: {}ms",
            path, actual_offset, start_entry_id, next_entry_id,
            time::Instant::now().duration_since(replay_start_time).as_millis()
        );

        log.write_offset.store(actual_offset, Ordering::Relaxed);
        log.flush_offset.store(actual_offset, Ordering::Relaxed);
        log.next_entry_id.store(next_entry_id, Ordering::Relaxed);
        log.seek(actual_offset).await?;
        Ok(log)
    }

    /// Advances file cursor to given offset.
    async fn seek(&mut self, offset: usize) -> Result<u64> {
        self.file
            .write()
            .await
            .seek(SeekFrom::Start(offset as u64))
            .await
            .context(IoSnafu)
    }

    /// Creates a file mmap region.
    async fn map(&self, start: u64, length: usize) -> Result<Mmap> {
        unsafe {
            let file = self.file.read().await.try_clone().await.context(IoSnafu)?;
            MmapOptions::new()
                .offset(start)
                .len(length)
                .map(&file)
                .context(IoSnafu)
        }
    }

    /// Returns the persisted size of current log file.
    #[allow(unused)]
    #[inline]
    pub fn persisted_size(&self) -> usize {
        self.flush_offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn next_entry_id(&self) -> Id {
        self.next_entry_id.load(Ordering::Relaxed)
    }

    /// Increases write offset field by `delta` and return the previous value.
    #[inline]
    fn inc_offset(&self, delta: usize) -> usize {
        // Relaxed order is enough since no sync-with relationship
        // between `offset` and any other field.
        self.write_offset.fetch_add(delta, Ordering::Relaxed)
    }

    /// Increases next entry field by `delta` and return the previous value.
    fn inc_entry_id(&self) -> u64 {
        // Relaxed order is enough since no sync-with relationship
        // between `offset` and any other field.
        self.next_entry_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Starts log file and it's internal components(including flush task, etc.).
    pub async fn start(&mut self) -> Result<()> {
        let notify = self.notify.clone();
        let file = self.file.write().await.try_clone().await.context(IoSnafu)?;

        let write_offset = self.write_offset.clone();
        let flush_offset = self.flush_offset.clone();

        let stopped = self.stopped.clone();

        if let Some(mut rx) = self.pending_request_rx.take() {
            let handle = tokio::spawn(async move {
                let mut batch: Vec<AppendRequest> = Vec::with_capacity(LOG_WRITER_BATCH_SIZE);

                let mut error_occurred = false;
                while !stopped.load(Ordering::Acquire) {
                    for _ in 0..LOG_WRITER_BATCH_SIZE {
                        match rx.try_recv() {
                            Ok(req) => {
                                batch.push(req);
                            }
                            Err(e) => match e {
                                TryRecvError::Empty => {
                                    if batch.is_empty() {
                                        notify.notified().await;
                                        if stopped.load(Ordering::Acquire) {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                                TryRecvError::Disconnected => {
                                    info!("Channel disconnected...");
                                    error_occurred = true;
                                    break;
                                }
                            },
                        }
                    }

                    // flush all pending data to disk
                    let write_offset_read = write_offset.load(Ordering::Relaxed);
                    // TODO(hl): add flush metrics
                    if let Err(flush_err) = file.sync_all().await {
                        error!("Failed to flush log file: {}", flush_err);
                        error_occurred = true;
                    }
                    if error_occurred {
                        info!("Flush task stop");
                        break;
                    }
                    let prev = flush_offset.load(Ordering::Acquire);
                    flush_offset.store(write_offset_read, Ordering::Relaxed);
                    info!("Flush offset: {} -> {}", prev, write_offset_read);
                    while let Some(req) = batch.pop() {
                        req.complete();
                    }
                }

                // drain all pending request on stopping.
                let write_offset_read = write_offset.load(Ordering::Relaxed);
                if file.sync_all().await.is_ok() {
                    flush_offset.store(write_offset_read, Ordering::Release);
                    while let Ok(req) = rx.try_recv() {
                        req.complete()
                    }
                }
                Ok(())
            });

            *self.join_handle.lock().unwrap() = Some(handle);
            info!("Flush task started: {}", self.name);
        }
        Ok(())
    }

    /// Stops log file.
    /// # Panics
    /// Panics when a log file is stopped while not being started ever.
    pub async fn stop(&self) -> Result<()> {
        self.stopped.store(true, Ordering::Release);
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

    #[inline]
    pub fn start_entry_id(&self) -> Id {
        self.start_entry_id
    }

    /// Replays current file til last entry read
    pub async fn replay(&mut self) -> Result<(usize, Id)> {
        let log_name = self.name.to_string();
        let previous_offset = self.flush_offset.load(Ordering::Relaxed);
        let mut stream = self.create_stream(
            // TODO(hl): LocalNamespace should be filled
            LocalNamespace::default(),
            0,
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
    pub fn create_stream(&self, _ns: impl Namespace, start_entry_id: u64) -> impl EntryStream + '_ {
        let length = self.flush_offset.load(Ordering::Relaxed);

        let s = stream!({
            let mmap = self.map(0, length).await?;
            let mut buf = MmappedBuffer::new(mmap);
            if buf.remaining_size() == 0 {
                info!("File is just created!");
                // file is newly created
                return;
            }

            while buf.remaining_size() > 0 {
                let entry: EntryImpl = EntryImpl::decode(&mut buf)?;
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
    pub async fn append<T: Entry>(&self, e: &mut T) -> Result<AppendResponseImpl> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::Eof);
        }
        e.set_id(0);
        let mut serialized = BytesMut::with_capacity(e.encoded_size());
        e.encode_to(&mut serialized)
            .map_err(BoxedError::new)
            .context(EncodeSnafu)?;
        let size = serialized.len();

        if size + self.write_offset.load(Ordering::Relaxed) > self.max_file_size {
            return Err(Error::Eof);
        }

        let entry_offset;
        let entry_id;

        {
            let mut file = self.file.write().await;
            // generate entry id
            entry_id = self.inc_entry_id();
            // rewrite encoded data
            LittleEndian::write_u64(&mut serialized[0..8], entry_id);
            // TODO(hl): CRC was calculated twice
            let checksum = CRC_ALGO.checksum(&serialized[0..size - 4]);
            LittleEndian::write_u32(&mut serialized[size - 4..], checksum);

            // write to file
            // TODO(hl): use io buffer and pwrite to reduce syscalls.
            file.write(&serialized.freeze()).await.context(IoSnafu)?;
            // generate in-file offset
            entry_offset = self.inc_offset(size);
        }

        let (tx, rx) = oneshot::channel();

        if self
            .pending_request_tx
            .send(AppendRequest {
                tx,
                offset: entry_offset,
                id: entry_id,
            })
            .await
            .is_err()
        {
            self.file.write().await.sync_all().await.context(IoSnafu)?;
            Ok(AppendResponseImpl {
                offset: entry_offset,
                entry_id,
            })
        } else {
            self.notify.notify_one(); // notify flush thread.
            rx.await.map_err(|e| {
                warn!(
                    "Error while waiting for append result:{}, file {}",
                    e,
                    self.name.to_string()
                );
                Error::Internal {
                    msg: "Sender already dropped".to_string(),
                    backtrace: Backtrace::generate(),
                }
            })
        }
    }

    #[inline]
    pub fn try_seal(&self) -> bool {
        self.sealed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    #[inline]
    pub fn is_seal(&self) -> bool {
        self.sealed.load(Acquire)
    }

    #[inline]
    pub fn unseal(&self) {
        self.sealed.store(false, Ordering::Release);
    }

    #[inline]
    pub fn file_name(&self) -> String {
        self.name.to_string()
    }
}

impl ToString for LogFile {
    fn to_string(&self) -> String {
        format!("LogFile{{ name: {}, path: {}, write_offset: {}, flush_offset: {}, start_entry_id: {}, entry_id_counter: {} }}",
                self.name,
                self.path,
                self.write_offset.load(Ordering::Relaxed), self.flush_offset.load(Ordering::Relaxed), self.start_entry_id, self.next_entry_id.load(Ordering::Relaxed))
    }
}

pub type LogFileRef = Arc<LogFile>;

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct AppendRequest {
    tx: OneshotSender<AppendResponseImpl>,
    offset: Offset,
    id: Id,
}

impl AppendRequest {
    pub fn complete(self) {
        // TODO(hl): use this result.
        let _ = self.tx.send(AppendResponseImpl {
            offset: self.offset,
            entry_id: self.id,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use common_telemetry::logging;
    use futures_util::StreamExt;
    use tempdir::TempDir;

    use super::*;
    use crate::fs::namespace::LocalNamespace;

    #[tokio::test]
    pub async fn test_create_entry_stream() {
        logging::init_default_ut_logging();
        let config = LogConfig::default();

        let dir = TempDir::new("greptimedb-store-test").unwrap();
        let path_buf = dir.path().join("0010.log");
        let path = path_buf.to_str().unwrap().to_string();
        File::create(path.as_str()).await.unwrap();

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
}
