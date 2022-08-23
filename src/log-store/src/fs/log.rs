use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_stream::stream;
use common_telemetry::{error, info, warn};
use futures::pin_mut;
use futures::StreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::{Encode, Entry, Id};
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::{Id as NamespaceId, Namespace};
use store_api::logstore::LogStore;
use tokio::sync::RwLock;

use crate::error::{
    CreateDirSnafu, DuplicateFileSnafu, Error, FileNameIllegalSnafu, InternalSnafu, IoSnafu,
    ReadPathSnafu, Result,
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
    files: RwLock<FileMap>,
    active: ArcSwap<LogFile>,
    config: LogConfig,
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
            files: RwLock::new(files),
            active: ArcSwap::new(active_file_cloned),
            config: config.clone(),
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

#[async_trait::async_trait]
impl LogStore for LocalFileLogStore {
    type Error = Error;
    type Namespace = LocalNamespace;
    type Entry = EntryImpl;
    type AppendResponse = AppendResponseImpl;

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
                    let s = file.create_stream(&ns, *start_id);
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
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use rand::{distributions::Alphanumeric, Rng};
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
}
