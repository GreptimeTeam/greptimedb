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

//! OpenDAL filesystem service rooted in a caller-supplied directory capability.
//!
//! See the crate README for the trust boundary and supported operations.

#![forbid(unsafe_code)]

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;

use cap_std::fs::{Dir, DirEntry, OpenOptions, ReadDir};
use opendal_core::raw::{
    OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, ServiceInfo, Timestamp,
    new_std_io_error, oio,
};
use opendal_core::{
    Buffer, Builder, BytesRange, Capability, EntryMode, Error, ErrorKind, Metadata,
    MetadataBuilder, OperationContext, Result,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const LIST_BATCH_SIZE: usize = 128;

/// Builds an OpenDAL filesystem service with an explicitly supplied directory handle.
#[derive(Default)]
pub struct SecureFs {
    dir: Option<Dir>,
}

impl SecureFs {
    /// Uses the caller's already open directory as the root of filesystem access.
    pub fn from_dir(dir: Dir) -> Self {
        Self { dir: Some(dir) }
    }
}

impl Builder for SecureFs {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        let dir = self.dir.ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "SecureFs requires an open directory",
            )
        })?;
        Ok(SecureFsBackend::new(Arc::new(dir)))
    }
}

fn backend_path(path: &str) -> io::Result<PathBuf> {
    // OpenDAL uses "/" to represent the operator root.
    if path.is_empty() || path == "/" {
        return Ok(PathBuf::new());
    }

    let path = path.trim_end_matches('/');
    if path.split('/').all(valid_component) {
        Ok(PathBuf::from(path))
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid filesystem object path",
        ))
    }
}

fn valid_component(component: &str) -> bool {
    let has_windows_alias = cfg!(windows) && component.ends_with('.');
    !(component.is_empty()
        || component.trim() != component
        || matches!(component, "." | "..")
        || component.contains(['\\', ':', '\0'])
        || has_windows_alias)
}

fn task_join_error(source: tokio::task::JoinError) -> Error {
    Error::new(ErrorKind::Unexpected, "filesystem task failed to join").set_source(source)
}

fn parse_write_error(error: io::Error, if_not_exists: bool) -> Error {
    if if_not_exists && error.kind() == io::ErrorKind::AlreadyExists {
        Error::new(
            ErrorKind::ConditionNotMatch,
            "the file already exists in the filesystem",
        )
        .set_source(error)
    } else {
        new_std_io_error(error)
    }
}

fn metadata_from_fs(metadata: cap_std::fs::Metadata) -> Result<Metadata> {
    let mut builder = if metadata.is_dir() {
        MetadataBuilder::dir()
    } else if metadata.is_file() {
        MetadataBuilder::file(metadata.len())
    } else {
        MetadataBuilder::unknown()
    };

    builder.last_modified(Timestamp::try_from(
        metadata.modified().map_err(new_std_io_error)?.into_std(),
    )?);
    Ok(builder.build())
}

#[derive(Clone, Debug)]
struct SecureFsBackend {
    root: Arc<Dir>,
    info: ServiceInfo,
    capability: Capability,
}

impl SecureFsBackend {
    fn new(root: Arc<Dir>) -> Self {
        let info = ServiceInfo::new("secure-fs", "/", "");
        let capability = Capability {
            stat: true,
            read: true,
            write: true,
            write_can_empty: true,
            write_can_append: true,
            write_can_multi: true,
            write_with_if_not_exists: true,
            create_dir: true,
            delete: true,
            delete_with_recursive: true,
            list: true,
            shared: true,
            ..Default::default()
        };
        Self {
            root,
            info,
            capability,
        }
    }
}

impl Service for SecureFsBackend {
    type Reader = oio::StreamReader<SecureFsReader>;
    type Writer = SecureFsWriter;
    type Lister = SecureFsLister;
    type Deleter = oio::OneShotDeleter<SecureFsDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        if path.as_os_str().is_empty() {
            return Ok(RpCreateDir::default());
        }
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || root.create_dir_all(path))
            .await
            .map_err(task_join_error)?
            .map_err(new_std_io_error)?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        let root = self.root.clone();
        let metadata = tokio::task::spawn_blocking(move || {
            if path.as_os_str().is_empty() {
                root.dir_metadata()
            } else {
                root.metadata(path)
            }
        })
        .await
        .map_err(task_join_error)?
        .map_err(new_std_io_error)?;
        Ok(RpStat::new(metadata_from_fs(metadata)?))
    }

    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        Ok(oio::StreamReader::new(SecureFsReader {
            root: self.root.clone(),
            path,
        }))
    }

    fn write(&self, _: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        Ok(SecureFsWriter {
            root: self.root.clone(),
            path,
            args,
            file: None,
        })
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        Ok(oio::OneShotDeleter::new(SecureFsDeleter {
            root: self.root.clone(),
        }))
    }

    fn list(&self, _: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
        let prefix = path.trim_end_matches('/');
        let path = backend_path(path).map_err(new_std_io_error)?;
        let display_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        Ok(SecureFsLister {
            root: self.root.clone(),
            path,
            display_prefix,
            read_dir: None,
            entries: vec![].into_iter(),
            seeded: false,
            done: false,
        })
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

struct SecureFsReader {
    root: Arc<Dir>,
    path: PathBuf,
}

impl oio::StreamRead for SecureFsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let path = self.path.clone();
        let root = self.root.clone();
        let file = tokio::task::spawn_blocking(move || root.open(path))
            .await
            .map_err(task_join_error)?
            .map_err(new_std_io_error)?;
        let mut file = tokio::fs::File::from_std(file.into_std());
        if range.offset() != 0 {
            file.seek(io::SeekFrom::Start(range.offset()))
                .await
                .map_err(new_std_io_error)?;
        }
        Ok((
            RpRead::default(),
            Box::new(SecureFsReadStream {
                file,
                remaining: range.size().unwrap_or(u64::MAX),
            }),
        ))
    }
}

struct SecureFsReadStream {
    file: tokio::fs::File,
    remaining: u64,
}

impl oio::ReadStream for SecureFsReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.remaining == 0 {
            return Ok(Buffer::new());
        }

        let size = self.remaining.min(2 * 1024 * 1024) as usize;
        let mut buffer = vec![0; size];
        let read = self
            .file
            .read(&mut buffer)
            .await
            .map_err(new_std_io_error)?;
        self.remaining = self.remaining.saturating_sub(read as u64);
        buffer.truncate(read);
        Ok(Buffer::from(buffer))
    }
}

struct SecureFsWriter {
    root: Arc<Dir>,
    path: PathBuf,
    args: OpWrite,
    file: Option<tokio::fs::File>,
}

impl SecureFsWriter {
    async fn ensure_file(&mut self) -> Result<&mut tokio::fs::File> {
        if self.file.is_none() {
            let path = self.path.clone();
            let root = self.root.clone();
            let if_not_exists = self.args.if_not_exists();
            let append = self.args.append();
            let file = tokio::task::spawn_blocking(move || {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    root.create_dir_all(parent).map_err(new_std_io_error)?;
                }

                let mut options = OpenOptions::new();
                options.write(true);
                if if_not_exists {
                    options.create_new(true);
                } else {
                    options.create(true);
                }
                if append {
                    options.append(true);
                } else {
                    options.truncate(true);
                }
                root.open_with(path, &options)
                    .map_err(|error| parse_write_error(error, if_not_exists))
            })
            .await
            .map_err(task_join_error)??;

            self.file = Some(tokio::fs::File::from_std(file.into_std()));
        }
        self.file
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "writer has no open file"))
    }
}

impl oio::Write for SecureFsWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        self.ensure_file()
            .await?
            .write_all(&buffer.to_bytes())
            .await
            .map_err(new_std_io_error)
    }

    async fn close(&mut self) -> Result<Metadata> {
        {
            let file = self.ensure_file().await?;
            file.flush().await.map_err(new_std_io_error)?;
            file.sync_all().await.map_err(new_std_io_error)?;
        }
        let metadata = self
            .ensure_file()
            .await?
            .metadata()
            .await
            .map_err(new_std_io_error)?;
        let mut builder = MetadataBuilder::file(metadata.len());
        builder.last_modified(Timestamp::try_from(
            metadata.modified().map_err(new_std_io_error)?,
        )?);
        Ok(builder.build())
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.flush().await.map_err(new_std_io_error)?;
        }
        Err(Error::new(
            ErrorKind::Unsupported,
            "SecureFs writes directly to the destination and cannot be rolled back",
        ))
    }
}

struct SecureFsLister {
    root: Arc<Dir>,
    path: PathBuf,
    display_prefix: String,
    read_dir: Option<Arc<Mutex<ReadDir>>>,
    entries: IntoIter<oio::Entry>,
    seeded: bool,
    done: bool,
}

impl oio::List for SecureFsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if !self.seeded {
            let path = self.path.clone();
            let root = self.root.clone();
            let display_prefix = self.display_prefix.clone();
            let read_dir = tokio::task::spawn_blocking(move || {
                let result = if path.as_os_str().is_empty() {
                    root.entries()
                } else {
                    root.read_dir(path)
                };

                match result {
                    Ok(read_dir) => Ok(Some(read_dir)),
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                        ) =>
                    {
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            })
            .await
            .map_err(task_join_error)?
            .map_err(new_std_io_error)?;

            self.seeded = true;
            let Some(read_dir) = read_dir else {
                self.done = true;
                return Ok(None);
            };
            self.read_dir = Some(Arc::new(Mutex::new(read_dir)));
            let current_path = oio::Entry::new(
                if display_prefix.is_empty() {
                    "/"
                } else {
                    &display_prefix
                },
                MetadataBuilder::dir().build(),
            );
            self.entries = vec![current_path].into_iter();
        }

        loop {
            if let Some(entry) = self.entries.next() {
                return Ok(Some(entry));
            }
            if self.done {
                return Ok(None);
            }
            let Some(read_dir) = self.read_dir.clone() else {
                return Ok(None);
            };
            let display_prefix = self.display_prefix.clone();
            let (entries, done) = tokio::task::spawn_blocking(move || {
                let mut read_dir = read_dir.lock().map_err(|_| {
                    io::Error::other("filesystem directory iterator lock is poisoned")
                })?;
                read_list_batch(&mut read_dir, &display_prefix)
            })
            .await
            .map_err(task_join_error)?
            .map_err(new_std_io_error)?;

            self.entries = entries.into_iter();
            self.done = done;
        }
    }
}

fn read_list_batch(
    read_dir: &mut ReadDir,
    display_prefix: &str,
) -> io::Result<(Vec<oio::Entry>, bool)> {
    let mut entries = Vec::with_capacity(LIST_BATCH_SIZE);
    for _ in 0..LIST_BATCH_SIZE {
        let entry = match read_dir.next() {
            Some(Ok(entry)) => entry,
            Some(Err(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok((entries, true));
            }
            Some(Err(error)) => return Err(error),
            None => return Ok((entries, true)),
        };

        if let Some(entry) = read_list_entry(entry, display_prefix)? {
            entries.push(entry);
        }
    }
    Ok((entries, false))
}

fn read_list_entry(entry: DirEntry, display_prefix: &str) -> io::Result<Option<oio::Entry>> {
    let file_type = match entry.file_type() {
        Ok(file_type) => file_type,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let file_name = entry.file_name();
    let Some(name) = file_name.to_str().filter(|name| valid_component(name)) else {
        // The object path must identify the same file if passed back to OpenDAL.
        return Ok(None);
    };
    let (path, mode) = if file_type.is_dir() {
        (format!("{display_prefix}{name}/"), EntryMode::DIR)
    } else if file_type.is_file() {
        (format!("{display_prefix}{name}"), EntryMode::FILE)
    } else {
        (format!("{display_prefix}{name}"), EntryMode::Unknown)
    };
    let metadata = if mode == EntryMode::Unknown {
        MetadataBuilder::unknown().build()
    } else {
        match entry.metadata() {
            Ok(metadata) => match metadata_from_fs(metadata) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(io::Error::other(error.to_string())),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        }
    };
    Ok(Some(oio::Entry::new(&path, metadata)))
}

struct SecureFsDeleter {
    root: Arc<Dir>,
}

impl oio::OneShotDelete for SecureFsDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let path = backend_path(&path).map_err(new_std_io_error)?;
        if path.as_os_str().is_empty() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "deleting the filesystem sandbox root is not supported",
            ));
        }
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let metadata = match root.symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };

            if metadata.is_dir() {
                if args.recursive() {
                    root.remove_dir_all(path)
                } else {
                    root.remove_dir(path)
                }
            } else {
                root.remove_file(path)
            }
        })
        .await
        .map_err(task_join_error)?
        .map_err(new_std_io_error)
    }
}
