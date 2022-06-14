use std::fmt::{Debug, Formatter};
use std::io::SeekFrom;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_stream::stream;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use common_telemetry::logging::{error, info};
use common_telemetry::warn;
use futures_util::task::noop_waker;
use futures_util::StreamExt;
use memmap2::{Mmap, MmapOptions};
use snafu::{Backtrace, GenerateImplicitData, ResultExt};
use store_api::logstore::entry::{Entry, Id, Offset};
use store_api::logstore::entry_stream::EntryStream;
use store_api::logstore::namespace::Namespace;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::{oneshot, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time;

use crate::error::{Error, IoSnafu, OpenLogSnafu, Result};
use crate::fs::config::LogConfig;
use crate::fs::crc::CRC_ALGO;
use crate::fs::entry::{EntryImpl, StreamImpl};
use crate::fs::file_name::FileName;
use crate::fs::namespace::LocalNamespace;
use crate::fs::AppendResultImpl;

const LOG_WRITER_BATCH_SIZE: usize = 16;

// TODO(hl): use pwrite polyfill in different platforms, avoid write syscall in each append request.
pub struct LogFile {
    name: FileName,
    file: Arc<RwLock<File>>,
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
            .context(OpenLogSnafu {
                file_name: path.clone(),
            })?;

        let file_name: FileName = path.as_str().try_into()?;
        let start_entry_id = file_name.entry_id();
        let (tx, rx) = tokio::sync::mpsc::channel(config.append_buffer_size);

        let mut log = Self {
            name: file_name,
            file: Arc::new(RwLock::new(file)),
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
            let file = self.file.read().await.try_clone().await.unwrap();
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
    pub fn size(&self) -> usize {
        self.flush_offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn next_entry_id(&self) -> Id {
        self.next_entry_id.load(Ordering::Relaxed)
    }

    /// Increases offset field by `delta` and return the previous value.
    #[inline]
    fn inc_offset(&self, delta: usize) -> usize {
        // Relaxed order is enough since no sync-with relationship
        // between `offset` and any other field.
        self.write_offset.fetch_add(delta, Ordering::Relaxed)
    }

    /// Increases offset field by `delta` and return the previous value.
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

        if let Some(mut rx) = self.pending_request_rx.take() {
            let handle = tokio::spawn(async move {
                let waker = noop_waker();
                let mut ctx = Context::from_waker(&waker);
                let mut batch: Vec<AppendRequest> = Vec::with_capacity(LOG_WRITER_BATCH_SIZE);

                let mut error_occurred = false;
                loop {
                    match rx.poll_recv(&mut ctx) {
                        Poll::Ready(Some(req)) => {
                            batch.push(req);
                        }
                        Poll::Ready(None) => {
                            error_occurred = true;
                        }
                        Poll::Pending => {
                            if batch.is_empty() {
                                notify.notified().await;
                            } else {
                                break;
                            }
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
                    flush_offset.store(write_offset_read, Ordering::Relaxed);
                    while let Some(req) = batch.pop() {
                        req.complete();
                    }
                }
                Ok(())
            });

            *self.join_handle.lock().unwrap() = Some(handle);
            info!("Flush task started...");
        }
        Ok(())
    }

    /// Stops log file.
    /// # Panics
    /// Panics when a log file is stopped while not being started ever.
    pub async fn stop(&self) -> Result<()> {
        self.notify.notify_one();
        let join_handle = self
            .join_handle
            .lock()
            .unwrap()
            .take()
            .expect("Join handle should present");

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
            LocalNamespace {
                name: "todo".to_string(),
                id: 1,
            },
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
        let s = stream!({
            let length = self.flush_offset.load(Ordering::Relaxed);
            let mmap = self.map(0, length).await?;

            let mut buf: &[u8] = mmap.as_ref();
            if buf.len() == 0 {
                info!("File is just created!");
                // file is newly created
                return;
            }

            loop {
                let entry = EntryImpl::try_from(buf)?;
                let entry_length = entry.len();
                if entry.id() >= start_entry_id {
                    yield Ok(vec![entry]);
                }
                if buf.len() > entry_length {
                    buf = &buf[entry_length..];
                } else {
                    break;
                }
            }
        });

        StreamImpl {
            inner: Box::pin(s),
            start_entry_id,
        }
    }

    /// Appends an entry to `LogFile` and return a `Result` containing the id of entry appended.
    pub async fn append<T: Entry>(&self, e: &mut T) -> Result<AppendResultImpl> {
        e.set_id(0);
        let mut serialized = e.serialize();
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
            // generate in-file offset
            entry_offset = self.inc_offset(size);
            // rewrite encoded data
            LittleEndian::write_u64(&mut serialized[0..8], entry_id);
            // TODO(hl): CRC was calculated twice
            let checksum = CRC_ALGO.checksum(&serialized[0..size - 4]);
            LittleEndian::write_u32(&mut serialized[size - 4..], checksum);

            // write to file
            // TODO(hl): use io buffer and pwrite to reduce syscalls.
            file.write(serialized.as_slice()).await.context(IoSnafu)?;
        }

        let (tx, rx) = oneshot::channel();

        self.pending_request_tx
            .send(AppendRequest {
                tx,
                offset: entry_offset,
                id: entry_id,
            })
            .await
            .expect("Send failed");

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

    #[inline]
    pub fn try_seal(&self) -> bool {
        self.sealed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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
        format!("LogFile{{ name: {}, write_offset: {}, flush_offset: {}, start_entry_id: {}, entry_id_counter: {} }}",
                self.name.to_string(), self.write_offset.load(Ordering::Relaxed), self.flush_offset.load(Ordering::Relaxed), self.start_entry_id, self.next_entry_id.load(Ordering::Relaxed))
    }
}

pub type LogFileRef = Arc<LogFile>;

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct AppendRequest {
    tx: OneshotSender<AppendResultImpl>,
    offset: Offset,
    id: Id,
}

impl AppendRequest {
    pub fn complete(self) {
        // TODO(hl): use this result.
        let _ = self.tx.send(AppendResultImpl {
            offset: self.offset,
            entry_id: self.id,
        });
    }
}

#[cfg(test)]
mod tests {
    use common_telemetry::logging;
    use futures_util::StreamExt;
    use tempdir::TempDir;

    use super::*;
    use crate::fs::namespace::LocalNamespace;

    pub async fn create_temp_dir(file_name: impl AsRef<str>) -> (String, TempDir) {
        let dir = TempDir::new("greptimedb-store-test").unwrap();
        let path_buf = dir.path().join(file_name.as_ref());
        let path_str = path_buf.to_str().unwrap().to_string();
        File::create(path_str.as_str()).await.unwrap();
        (path_str, dir)
    }

    #[tokio::test]
    pub async fn test_create_entry_stream() {
        logging::init_default_ut_logging();
        let config = LogConfig::default();
        let (path, _dir) = create_temp_dir("0010.log").await;
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

        let mut stream = file.create_stream(
            LocalNamespace {
                name: "test".to_string(),
                id: 1,
            },
            0,
        );

        let mut data = vec![];

        while let Some(v) = stream.next().await {
            let entries = v.unwrap();
            let content = entries[0].data();
            let vec = content.to_vec();
            data.push(String::from_utf8(vec).unwrap());
        }

        assert_eq!(vec!["test1".to_string(), "test-2".to_string()], data);
        drop(stream);

        let result = file.stop().await;
        info!("Stop file res: {:?}", result);
    }
}
