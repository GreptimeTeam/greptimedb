use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use common_telemetry::{error, info};
use snafu::ResultExt;
use store_api::logstore::entry::Id;
use store_api::logstore::LogStore;
use tokio::sync::RwLock;

use crate::error::{Error, IOSnafu};
use crate::fs::config::LogConfig;
use crate::fs::entry::EntryImpl;
use crate::fs::file::{LogFile, LogFileRef};
use crate::fs::file_name::FileName;
use crate::fs::namespace::LocalNamespace;

type FileMap = BTreeMap<u64, LogFileRef>;

pub struct LocalFileLogStoreImpl {
    files: RwLock<FileMap>,
    active: LogFileRef,
    config: LogConfig,
}

impl LocalFileLogStoreImpl {
    /// Opens a directory as log store directory, initialize directory if it is empty.

    #[allow(unused)]
    pub async fn open(config: &LogConfig) -> Result<Self, Error> {
        let mut files = Self::load_dir(config.log_file_dir.as_str(), config).await?;

        if files.is_empty() {
            Self::init_on_empty(&mut files, config).await?;
            info!(
                "Initialized log store directory: {}",
                config.log_file_dir.to_string()
            )
        }

        let id = *files.keys().max().ok_or(Error::Internal {
            msg: format!(
                "log store directory is empty after initialization: {}",
                config.log_file_dir
            ),
        })?;

        info!(
            "Successfully loaded log store directory, files: {:?}",
            files
        );

        let active_file_ref = files.get_mut(&id).ok_or(Error::Internal {
            msg: format!(
                "log store directory is empty after initialization: {}",
                config.log_file_dir
            ),
        })?;

        let active_file_name = active_file_ref.to_string();
        info!("Log store active log file: {}", active_file_name);

        // Start active log file
        Arc::get_mut(active_file_ref)
            .ok_or(Error::Internal {
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

        let active_cloned = active_file_ref.clone();
        Ok(Self {
            files: RwLock::new(files),
            active: active_cloned,
            config: config.clone(),
        })
    }

    pub async fn init_on_empty(files: &mut FileMap, config: &LogConfig) -> Result<(), Error> {
        let path = Path::new(config.log_file_dir.as_str()).join(FileName::new(0).to_string());
        let file_path = path.to_str().ok_or(Error::FileNameIllegal {
            file_name: config.log_file_dir.as_str().to_string(),
        })?;
        let file = LogFile::open(file_path, config).await?;
        files.insert(0, Arc::new(file));
        Ok(())
    }

    pub async fn load_dir(p: impl AsRef<str>, config: &LogConfig) -> Result<FileMap, Error> {
        let mut map = FileMap::new();
        let mut dir = tokio::fs::read_dir(Path::new(p.as_ref()))
            .await
            .context(IOSnafu)?;

        while let Some(f) = dir.next_entry().await.context(IOSnafu)? {
            let path_buf = f.path();
            let path = path_buf.to_str().ok_or(Error::FileNameIllegal {
                file_name: p.as_ref().to_string(),
            })?;
            let file_name = FileName::try_from(path)?;
            let start_id = file_name.entry_id();
            let file = LogFile::open(path, config).await?;
            info!("Load log store file {}: {}", start_id, file.to_string());
            if map.contains_key(&start_id) {
                error!("Log file with start entry id: {} already exists", start_id);
                return Err(Error::FileNameIllegal {
                    file_name: start_id.to_string() + " already exists",
                });
            }
            map.insert(start_id, Arc::new(file));
        }
        Ok(map)
    }

    /// Mark current active file as closed and create a new log file for writing.
    async fn roll_next(&mut self) -> Result<(), Error> {
        // todo need a lock
        let mut file_lock = self.files.write().await;

        // create and start a new log file
        let entry_id = self.active.next_entry_id();
        let path_buf =
            Path::new(self.config.log_file_dir.as_str()).join(FileName::new(entry_id).to_string());
        let path = path_buf.to_str().ok_or(Error::FileNameIllegal {
            file_name: self.config.log_file_dir.clone(),
        })?;
        let mut new_file = LogFile::open(path, &self.config).await?;
        new_file.start().await?;

        let new_file_ref = Arc::new(new_file);

        file_lock.insert(new_file_ref.start_entry_id(), new_file_ref.clone());

        let start_entry_id = std::mem::replace(&mut self.active, new_file_ref).start_entry_id();

        Arc::get_mut(file_lock.get_mut(&start_entry_id).unwrap())
            .ok_or(Error::Internal {
                msg: "Concurrent modify log file set on rolling".to_string(),
            })?
            .stop()
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl LogStore for LocalFileLogStoreImpl {
    type Error = Error;
    type Namespace = LocalNamespace;
    type Entry = EntryImpl;

    async fn append(
        &mut self,
        _ns: Self::Namespace,
        mut e: Self::Entry,
    ) -> Result<Id, Self::Error> {
        let entry_ref = &mut e;

        // todo(hl): configurable retry times
        for _ in 0..3 {
            match self.active.append(entry_ref).await {
                Ok(r) => return Ok(r),
                Err(e) => match e {
                    Error::Eof => {
                        self.roll_next().await?;
                        info!("Rolled to next file, retry append");
                        continue;
                    }
                    _ => {
                        return Err(e);
                    }
                },
            }
        }

        return Err(Error::Internal {
            msg: "Failed to append entry with max retry time exceeds".to_string(),
        });
    }

    async fn append_batch(
        &self,
        _ns: Self::Namespace,
        _e: Vec<Self::Entry>,
    ) -> Result<Id, Self::Error> {
        todo!()
    }

    async fn read(
        &self,
        _ns: Self::Namespace,
        _id: Id,
    ) -> Result<
        store_api::logstore::entry_stream::SendableEntryStream<'_, Self::Entry, Self::Error>,
        Self::Error,
    > {
        todo!()
    }

    async fn create_namespace(&mut self, _ns: Self::Namespace) -> Result<(), Self::Error> {
        todo!()
    }

    async fn delete_namespace(&mut self, _ns: Self::Namespace) -> Result<(), Self::Error> {
        todo!()
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error> {
        todo!()
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
        let dir = TempDir::new("greptimedb").unwrap();
        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 128,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
        };

        let mut logstore = LocalFileLogStoreImpl::open(&config).await.unwrap();
        assert_eq!(
            0,
            logstore
                .append(
                    LocalNamespace::default(),
                    EntryImpl::new(generate_data(100)),
                )
                .await
                .unwrap()
        );

        assert_eq!(
            1,
            logstore
                .append(
                    LocalNamespace::default(),
                    EntryImpl::new(generate_data(100)),
                )
                .await
                .unwrap()
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
        let dir = TempDir::new("greptimedb").unwrap();
        let config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 128,
            log_file_dir: dir.path().to_str().unwrap().to_string(),
        };
        let mut logstore = LocalFileLogStoreImpl::open(&config).await.unwrap();
        let id = logstore
            .append(
                LocalNamespace::default(),
                EntryImpl::new(generate_data(100)),
            )
            .await
            .unwrap();
        assert_eq!(0, id);

        let stream = logstore.active.create_stream(LocalNamespace::default(), 0);
        tokio::pin!(stream);

        let entries = stream.next().await.unwrap().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id(), 0);
    }
}
