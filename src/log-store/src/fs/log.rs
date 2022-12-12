// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_stream::stream;
use common_telemetry::{error, info, warn};
use futures::{pin_mut, StreamExt};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::{Encode, Entry, Id};
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::{Id as NamespaceId, Namespace};
use store_api::logstore::LogStore;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{
    CreateDirSnafu, DuplicateFileSnafu, Error, FileNameIllegalSnafu, InternalSnafu,
    InvalidStateSnafu, IoSnafu, ReadPathSnafu, Result, WaitGcTaskStopSnafu,
};
use crate::fs::config::LogConfig;
use crate::fs::entry::EntryImpl;
use crate::fs::file::{LogFile, LogFileRef};
use crate::fs::file_name::FileName;
use crate::fs::namespace::LocalNamespace;
use crate::fs::AppendResponseImpl;

type FileMap = BTreeMap<u64, LogFileRef>;

#[derive(Debug)]
pub struct LocalFileLogStore {
    files: Arc<RwLock<FileMap>>,
    active: ArcSwap<LogFile>,
    config: LogConfig,
    stable_ids: Arc<RwLock<HashMap<LocalNamespace, u64>>>,
    cancel_token: Mutex<Option<CancellationToken>>,
    gc_task_handle: Mutex<Option<JoinHandle<()>>>,
}

impl LocalFileLogStore {
    /// Opens a directory as log store directory, initialize directory if it is empty.
    pub async fn open(config: &LogConfig) -> Result<Self> {
        // Create the log directory if missing.
        tokio::fs::create_dir_all(&config.log_file_dir)
            .await
            .context(CreateDirSnafu {
                path: &config.log_file_dir,
            })?;

        let mut files = Self::load_dir(&config.log_file_dir, config).await?;

        if files.is_empty() {
            Self::init_on_empty(&mut files, config).await?;
            info!("Initialized log store directory: {}", config.log_file_dir)
        }

        let id = *files.keys().max().context(InternalSnafu {
            msg: format!(
                "log store directory is empty after initialization: {}",
                config.log_file_dir
            ),
        })?;

        info!(
            "Successfully loaded log store directory, files: {:?}",
            files
        );

        let active_file = files
            .get_mut(&id)
            .expect("Not expected to fail when initing log store");

        active_file.unseal();
        let active_file_name = active_file.file_name();
        info!("Log store active log file: {}", active_file_name);

        // Start active log file
        Arc::get_mut(active_file)
            .with_context(|| InternalSnafu {
                msg: format!(
                    "Concurrent modification on log store {} start is not allowed",
                    active_file_name
                ),
            })?
            .start()
            .await?;
        info!(
            "Successfully started current active file: {}",
            active_file_name
        );

        let active_file_cloned = active_file.clone();
        Ok(Self {
            files: Arc::new(RwLock::new(files)),
            active: ArcSwap::new(active_file_cloned),
            config: config.clone(),
            stable_ids: Arc::new(Default::default()),
            cancel_token: Mutex::new(None),
            gc_task_handle: Mutex::new(None),
        })
    }

    pub async fn init_on_empty(files: &mut FileMap, config: &LogConfig) -> Result<()> {
        let path = Path::new(&config.log_file_dir).join(FileName::log(0).to_string());
        let file_path = path.to_str().context(FileNameIllegalSnafu {
            file_name: config.log_file_dir.clone(),
        })?;
        let file = LogFile::open(file_path, config).await?;
        files.insert(0, Arc::new(file));
        Ok(())
    }

    pub async fn load_dir(path: impl AsRef<str>, config: &LogConfig) -> Result<FileMap> {
        let mut map = FileMap::new();
        let mut dir = tokio::fs::read_dir(Path::new(path.as_ref()))
            .await
            .context(ReadPathSnafu {
                path: path.as_ref(),
            })?;

        while let Some(f) = dir.next_entry().await.context(IoSnafu)? {
            let path_buf = f.path();
            let path = path_buf.to_str().context(FileNameIllegalSnafu {
                file_name: path.as_ref().to_string(),
            })?;
            let file_name = FileName::try_from(path)?;
            let start_id = file_name.entry_id();
            let file = LogFile::open(path, config).await?;
            info!("Load log store file {}: {:?}", start_id, file);
            if map.contains_key(&start_id) {
                error!("Log file with start entry id: {} already exists", start_id);
                return DuplicateFileSnafu {
                    msg: format!("File with start id: {} duplicates on start", start_id),
                }
                .fail();
            }
            file.try_seal();
            map.insert(start_id, Arc::new(file));
        }
        Ok(map)
    }

    /// Mark current active file as closed and create a new log file for writing.
    async fn roll_next(&self, active: LogFileRef) -> Result<()> {
        // acquires lock
        let mut files = self.files.write().await;

        // if active is already sealed, then just return.
        if active.is_seal() {
            return Ok(());
        }

        // create and start a new log file
        let next_entry_id = active.last_entry_id() + 1;
        let path_buf =
            Path::new(&self.config.log_file_dir).join(FileName::log(next_entry_id).to_string());
        let path = path_buf.to_str().context(FileNameIllegalSnafu {
            file_name: self.config.log_file_dir.clone(),
        })?;

        let mut new_file = LogFile::open(path, &self.config).await?;
        new_file.start().await?;

        let new_file = Arc::new(new_file);
        files.insert(new_file.start_entry_id(), new_file.clone());

        self.active.swap(new_file);
        active.try_seal();
        tokio::spawn(async move {
            active.stop().await.unwrap();
            info!("Sealed log file {} stopped.", active.file_name());
        });
        Ok(()) // release lock
    }

    pub fn active_file(&self) -> Arc<LogFile> {
        self.active.load().clone()
    }
}

async fn gc(
    files: Arc<RwLock<FileMap>>,
    stables_ids: Arc<RwLock<HashMap<LocalNamespace, u64>>>,
) -> Result<()> {
    if let Some(lowest) = find_lowest_id(stables_ids).await {
        gc_inner(files, lowest).await
    } else {
        Ok(())
    }
}

async fn find_lowest_id(stable_ids: Arc<RwLock<HashMap<LocalNamespace, u64>>>) -> Option<u64> {
    let mut lowest_stable = None;
    {
        let stable_ids = stable_ids.read().await;
        for (ns, id) in stable_ids.iter() {
            if *id <= *lowest_stable.get_or_insert(*id) {
                lowest_stable = Some(*id);
                info!("Current lowest stable id: {}, namespace: {:?}", *id, ns);
            }
        }
    }
    lowest_stable
}

async fn gc_inner(files: Arc<RwLock<FileMap>>, stable_id: u64) -> Result<()> {
    let mut files = files.write().await;
    let files_to_delete = find_files_to_delete(&files, stable_id);
    info!(
        "Compacting log file up to entry id: {}, files to delete: {:?}",
        stable_id, files_to_delete
    );
    for entry_id in files_to_delete {
        if let Some(f) = files.remove(&entry_id) {
            if !f.is_stopped() {
                f.stop().await?;
            }
            f.destroy().await?;
            info!("Destroyed log file: {}", f.file_name());
        }
    }
    Ok(())
}

fn find_files_to_delete<T>(offset_map: &BTreeMap<u64, T>, entry_id: u64) -> Vec<u64> {
    let mut res = vec![];
    for (cur, next) in offset_map.keys().zip(offset_map.keys().skip(1)) {
        if *cur < entry_id && *next <= entry_id {
            res.push(*cur);
        }
    }
    res
}

#[async_trait::async_trait]
impl LogStore for LocalFileLogStore {
    type Error = Error;
    type Namespace = LocalNamespace;
    type Entry = EntryImpl;
    type AppendResponse = AppendResponseImpl;

    async fn start(&self) -> Result<()> {
        let files = self.files.clone();
        let stable_ids = self.stable_ids.clone();
        let interval = self.config.gc_interval;
        let token = tokio_util::sync::CancellationToken::new();
        let child = token.child_token();

        let handle = common_runtime::spawn_bg(async move {
            loop {
                if let Err(e) = gc(files.clone(), stable_ids.clone()).await {
                    error!(e; "Failed to gc log store");
                }

                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = child.cancelled() => {
                        info!("LogStore gc task has been cancelled");
                        return;
                    }
                }
            }
        });

        *self.gc_task_handle.lock().await = Some(handle);
        *self.cancel_token.lock().await = Some(token);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let handle = self
            .gc_task_handle
            .lock()
            .await
            .take()
            .context(InvalidStateSnafu {
                msg: "Logstore gc task not spawned",
            })?;
        let token = self
            .cancel_token
            .lock()
            .await
            .take()
            .context(InvalidStateSnafu {
                msg: "Logstore gc task not spawned",
            })?;
        token.cancel();
        Ok(handle.await.context(WaitGcTaskStopSnafu)?)
    }

    async fn append(&self, mut entry: Self::Entry) -> Result<Self::AppendResponse> {
        // TODO(hl): configurable retry times
        for _ in 0..3 {
            let current_active_file = self.active_file();
            match current_active_file.append(&mut entry).await {
                Ok(r) => return Ok(r),
                Err(e) => match e {
                    Error::Eof => {
                        self.roll_next(current_active_file.clone()).await?;
                        info!(
                            "Rolled to next file, retry append, entry size: {}",
                            entry.encoded_size()
                        );
                        continue;
                    }
                    Error::Internal { .. } => {
                        warn!("File closed, try new file");
                        continue;
                    }
                    _ => {
                        error!(e; "Failed to roll to next log file");
                        return Err(e);
                    }
                },
            }
        }

        return InternalSnafu {
            msg: "Failed to append entry with max retry time exceeds",
        }
        .fail();
    }

    async fn append_batch(&self, _ns: &Self::Namespace, _e: Vec<Self::Entry>) -> Result<Id> {
        todo!()
    }

    async fn read(
        &self,
        ns: &Self::Namespace,
        id: Id,
    ) -> Result<SendableEntryStream<'_, Self::Entry, Self::Error>> {
        let files = self.files.read().await;
        let ns = ns.clone();

        let s = stream!({
            for (start_id, file) in files.iter() {
                // TODO(hl): Use index to lookup file
                if *start_id <= id {
                    let s = file.create_stream(&ns, id);
                    pin_mut!(s);
                    while let Some(entries) = s.next().await {
                        match entries {
                            Ok(entries) => {
                                yield Ok(entries
                                    .into_iter()
                                    .filter(|e| e.namespace().id() == ns.id())
                                    .collect::<Vec<_>>())
                            }
                            Err(e) => yield Err(e),
                        }
                    }
                }
            }
        });

        Ok(Box::pin(s))
    }

    async fn create_namespace(&mut self, _ns: &Self::Namespace) -> Result<()> {
        todo!()
    }

    async fn delete_namespace(&mut self, _ns: &Self::Namespace) -> Result<()> {
        todo!()
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        todo!()
    }

    fn entry<D: AsRef<[u8]>>(&self, data: D, id: Id, namespace: Self::Namespace) -> Self::Entry {
        EntryImpl::new(data, id, namespace)
    }

    fn namespace(&self, id: NamespaceId) -> Self::Namespace {
        LocalNamespace::new(id)
    }

    async fn mark_stable(
        &self,
        namespace: Self::Namespace,
        id: Id,
    ) -> std::result::Result<(), Self::Error> {
        info!("Mark namespace stable entry id, {:?}:{}", namespace, id);
        let mut map = self.stable_ids.write().await;
        let prev = map.insert(namespace, id);
        info!("Prev: {:?}", prev);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use futures_util::StreamExt;
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use store_api::logstore::entry::Entry;
    use tempdir::TempDir;

    use super::*;

    #[tokio::test]
    pub async fn test_roll_file() {
        common_telemetry::logging::init_default_ut_logging();
        let dir = TempDir::new("greptimedb1").unwrap();
        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 128,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let logstore = LocalFileLogStore::open(&config).await.unwrap();
        assert_eq!(
            0,
            logstore
                .append(EntryImpl::new(
                    generate_data(96),
                    0,
                    LocalNamespace::new(42)
                ),)
                .await
                .unwrap()
                .entry_id
        );

        assert_eq!(
            1,
            logstore
                .append(EntryImpl::new(
                    generate_data(96),
                    1,
                    LocalNamespace::new(42)
                ))
                .await
                .unwrap()
                .entry_id
        );
    }

    fn generate_data(size: usize) -> Vec<u8> {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .map(char::from)
            .collect();
        s.into_bytes()
    }

    #[tokio::test]
    pub async fn test_write_and_read_data() {
        common_telemetry::logging::init_default_ut_logging();
        let dir = TempDir::new("greptimedb2").unwrap();

        let dir_str = dir.path().to_string_lossy().to_string();
        info!("dir: {}", dir_str);

        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 128,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let logstore = LocalFileLogStore::open(&config).await.unwrap();
        let ns = LocalNamespace::new(42);
        let id = logstore
            .append(EntryImpl::new(
                generate_data(96),
                0,
                LocalNamespace::new(42),
            ))
            .await
            .unwrap()
            .entry_id;
        assert_eq!(0, id);

        let stream = logstore.read(&ns, 0).await.unwrap();
        tokio::pin!(stream);

        let entries = stream.next().await.unwrap().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id(), 0);
        assert_eq!(42, entries[0].namespace_id);
    }

    #[tokio::test]
    pub async fn test_namespace() {
        common_telemetry::logging::init_default_ut_logging();
        let dir = TempDir::new("greptimedb").unwrap();
        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 1024 * 1024,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let logstore = LocalFileLogStore::open(&config).await.unwrap();
        assert_eq!(
            0,
            logstore
                .append(EntryImpl::new(
                    generate_data(96),
                    0,
                    LocalNamespace::new(42),
                ))
                .await
                .unwrap()
                .entry_id
        );

        assert_eq!(
            1,
            logstore
                .append(EntryImpl::new(
                    generate_data(96),
                    1,
                    LocalNamespace::new(43),
                ))
                .await
                .unwrap()
                .entry_id
        );

        let stream = logstore.read(&LocalNamespace::new(42), 0).await.unwrap();
        tokio::pin!(stream);

        let entries = stream.next().await.unwrap().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id(), 0);
        assert_eq!(42, entries[0].namespace_id);

        let stream = logstore.read(&LocalNamespace::new(43), 0).await.unwrap();
        tokio::pin!(stream);

        let entries = stream.next().await.unwrap().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id(), 1);
        assert_eq!(43, entries[0].namespace_id);
    }

    #[test]
    fn test_find_files_to_delete() {
        let file_map = vec![(1u64, ()), (11u64, ()), (21u64, ()), (31u64, ())]
            .into_iter()
            .collect::<BTreeMap<u64, ()>>();

        assert!(find_files_to_delete(&file_map, 0).is_empty());
        assert!(find_files_to_delete(&file_map, 1).is_empty());
        assert!(find_files_to_delete(&file_map, 2).is_empty());
        assert!(find_files_to_delete(&file_map, 10).is_empty());

        assert_eq!(vec![1], find_files_to_delete(&file_map, 11));
        assert_eq!(vec![1], find_files_to_delete(&file_map, 20));
        assert_eq!(vec![1, 11], find_files_to_delete(&file_map, 21));

        assert_eq!(vec![1, 11, 21], find_files_to_delete(&file_map, 31));
        assert_eq!(vec![1, 11, 21], find_files_to_delete(&file_map, 100));
    }

    #[tokio::test]
    async fn test_find_lowest_id() {
        common_telemetry::logging::init_default_ut_logging();
        let dir = TempDir::new("greptimedb-log-compact").unwrap();
        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 4096,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let logstore = LocalFileLogStore::open(&config).await.unwrap();
        assert!(find_lowest_id(logstore.stable_ids.clone()).await.is_none());

        logstore
            .mark_stable(LocalNamespace::new(1), 100)
            .await
            .unwrap();
        assert_eq!(Some(100), find_lowest_id(logstore.stable_ids.clone()).await);

        logstore
            .mark_stable(LocalNamespace::new(2), 200)
            .await
            .unwrap();
        assert_eq!(Some(100), find_lowest_id(logstore.stable_ids.clone()).await);

        logstore
            .mark_stable(LocalNamespace::new(1), 101)
            .await
            .unwrap();
        assert_eq!(Some(101), find_lowest_id(logstore.stable_ids.clone()).await);

        logstore
            .mark_stable(LocalNamespace::new(2), 202)
            .await
            .unwrap();
        assert_eq!(Some(101), find_lowest_id(logstore.stable_ids.clone()).await);

        logstore
            .mark_stable(LocalNamespace::new(1), 300)
            .await
            .unwrap();
        assert_eq!(Some(202), find_lowest_id(logstore.stable_ids.clone()).await);
    }

    #[tokio::test]
    async fn test_compact_log_file() {
        common_telemetry::logging::init_default_ut_logging();
        let dir = TempDir::new("greptimedb-log-compact").unwrap();
        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 4096,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let logstore = LocalFileLogStore::open(&config).await.unwrap();

        for id in 0..50 {
            logstore
                .append(EntryImpl::new(
                    generate_data(990),
                    id,
                    LocalNamespace::new(42),
                ))
                .await
                .unwrap();
        }

        assert_eq!(
            vec![0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48],
            logstore
                .files
                .read()
                .await
                .keys()
                .copied()
                .collect::<Vec<_>>()
        );

        gc_inner(logstore.files.clone(), 10).await.unwrap();

        assert_eq!(
            vec![8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48],
            logstore
                .files
                .read()
                .await
                .keys()
                .copied()
                .collect::<Vec<_>>()
        );

        gc_inner(logstore.files.clone(), 28).await.unwrap();

        assert_eq!(
            vec![28, 32, 36, 40, 44, 48],
            logstore
                .files
                .read()
                .await
                .keys()
                .copied()
                .collect::<Vec<_>>()
        );

        gc_inner(logstore.files.clone(), 50).await.unwrap();

        assert_eq!(
            vec![48],
            logstore
                .files
                .read()
                .await
                .keys()
                .copied()
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_gc_task() {
        common_telemetry::logging::init_default_ut_logging();
        let dir = TempDir::new("greptimedb-log-compact").unwrap();
        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 4096,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            gc_interval: Duration::from_millis(100),
        };
        let logstore = LocalFileLogStore::open(&config).await.unwrap();
        logstore.start().await.unwrap();

        for id in 0..50 {
            logstore
                .append(EntryImpl::new(
                    generate_data(990),
                    id,
                    LocalNamespace::new(42),
                ))
                .await
                .unwrap();
        }
        logstore
            .mark_stable(LocalNamespace::new(42), 30)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let file_ids = logstore
            .files
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(vec![28, 32, 36, 40, 44, 48], file_ids);

        let mut files = vec![];
        let mut readir = tokio::fs::read_dir(dir.path()).await.unwrap();
        while let Some(r) = readir.next_entry().await.transpose() {
            let entry = r.unwrap();
            files.push(entry.file_name().to_str().unwrap().to_string());
        }

        assert_eq!(
            vec![
                "00000000000000000028.log".to_string(),
                "00000000000000000048.log".to_string(),
                "00000000000000000040.log".to_string(),
                "00000000000000000044.log".to_string(),
                "00000000000000000036.log".to_string(),
                "00000000000000000032.log".to_string()
            ]
            .into_iter()
            .collect::<HashSet<String>>(),
            files.into_iter().collect::<HashSet<String>>()
        );
    }
}
