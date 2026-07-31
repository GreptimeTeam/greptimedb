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

//! A capability-based filesystem backend for untrusted object paths.

use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;
use std::{fmt, io};

use cap_std::ambient_authority;
use cap_std::fs::{Dir, DirEntry, OpenOptions, ReadDir};
use opendal::raw::*;
use opendal::{
    Buffer, Capability, EntryMode, Error, ErrorKind, Metadata, Operator, OperatorBuilder, Result,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const LIST_BATCH_SIZE: usize = 128;

/// An opened filesystem root that confines all descendant path resolution.
#[derive(Clone)]
pub struct SecureFsRoot {
    dir: Arc<Dir>,
    path: Arc<PathBuf>,
}

impl fmt::Debug for SecureFsRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecureFsRoot")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl SecureFsRoot {
    /// Creates and opens `path` using ambient authority.
    ///
    /// Callers must only pass a server-controlled path.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "filesystem sandbox root must not be a symbolic link",
            ));
        }

        let path = path.canonicalize()?;
        let dir = Dir::open_ambient_dir(&path, ambient_authority())?;
        Ok(Self {
            dir: Arc::new(dir),
            path: Arc::new(path),
        })
    }

    /// Returns the canonical path used to open this root.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Opens a descendant directory without leaving this capability root.
    pub fn open_subdir(&self, path: impl AsRef<Path>) -> io::Result<Self> {
        let path = normalize_relative_path(path.as_ref())?;
        if path.as_os_str().is_empty() {
            return Ok(self.clone());
        }

        let dir = self.dir.open_dir(&path)?;
        Ok(Self {
            dir: Arc::new(dir),
            path: Arc::new(self.path.join(path)),
        })
    }

    /// Creates and opens a descendant directory without leaving this capability root.
    pub fn create_subdir(&self, path: impl AsRef<Path>) -> io::Result<Self> {
        let path = normalize_relative_path(path.as_ref())?;
        if path.as_os_str().is_empty() {
            return Ok(self.clone());
        }

        self.dir.create_dir_all(&path)?;
        let dir = self.dir.open_dir(&path)?;
        Ok(Self {
            dir: Arc::new(dir),
            path: Arc::new(self.path.join(path)),
        })
    }

    /// Builds an OpenDAL operator confined to this root.
    pub fn build_operator(&self) -> Operator {
        OperatorBuilder::new(SecureFsBackend::new(self.clone())).finish()
    }
}

fn normalize_relative_path(path: &Path) -> io::Result<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "path escapes the filesystem sandbox",
                ));
            }
        }
    }
    Ok(normalized)
}

fn backend_path(path: &str) -> io::Result<PathBuf> {
    let path = path.trim_matches('/');
    if path.is_empty() {
        Ok(PathBuf::new())
    } else {
        normalize_relative_path(Path::new(path))
    }
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
    let mode = if metadata.is_dir() {
        EntryMode::DIR
    } else if metadata.is_file() {
        EntryMode::FILE
    } else {
        EntryMode::Unknown
    };

    Ok(Metadata::new(mode)
        .with_content_length(metadata.len())
        .with_last_modified(Timestamp::try_from(
            metadata.modified().map_err(new_std_io_error)?.into_std(),
        )?))
}

#[derive(Clone, Debug)]
struct SecureFsBackend {
    root: SecureFsRoot,
    info: Arc<AccessorInfo>,
}

impl SecureFsBackend {
    fn new(root: SecureFsRoot) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme("fs")
            .set_root(&root.path().to_string_lossy())
            .set_native_capability(Capability {
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
            });
        Self {
            root,
            info: info.into(),
        }
    }
}

impl Access for SecureFsBackend {
    type Reader = SecureFsReader;
    type Writer = SecureFsWriter;
    type Lister = Option<SecureFsLister>;
    type Deleter = oio::OneShotDeleter<SecureFsDeleter>;
    type Copier = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        let root = self.root.clone();
        common_runtime::spawn_blocking_global(move || root.dir.create_dir_all(path))
            .await
            .map_err(new_task_join_error)?
            .map_err(new_std_io_error)?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        let root = self.root.clone();
        let metadata = common_runtime::spawn_blocking_global(move || {
            if path.as_os_str().is_empty() {
                root.dir.dir_metadata()
            } else {
                root.dir.metadata(path)
            }
        })
        .await
        .map_err(new_task_join_error)?
        .map_err(new_std_io_error)?;
        Ok(RpStat::new(metadata_from_fs(metadata)?))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        let root = self.root.clone();
        let file = common_runtime::spawn_blocking_global(move || root.dir.open(path))
            .await
            .map_err(new_task_join_error)?
            .map_err(new_std_io_error)?;
        let mut file = tokio::fs::File::from_std(file.into_std());
        if args.range().offset() != 0 {
            file.seek(io::SeekFrom::Start(args.range().offset()))
                .await
                .map_err(new_std_io_error)?;
        }
        Ok((
            RpRead::default(),
            SecureFsReader {
                file,
                remaining: args.range().size().unwrap_or(u64::MAX),
            },
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        let root = self.root.clone();
        let if_not_exists = args.if_not_exists();
        let file = common_runtime::spawn_blocking_global(move || {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                root.dir.create_dir_all(parent).map_err(new_std_io_error)?;
            }

            let mut options = OpenOptions::new();
            options.write(true);
            if args.if_not_exists() {
                options.create_new(true);
            } else {
                options.create(true);
            }
            if args.append() {
                options.append(true);
            } else {
                options.truncate(true);
            }
            root.dir
                .open_with(path, &options)
                .map_err(|error| parse_write_error(error, if_not_exists))
        })
        .await
        .map_err(new_task_join_error)??;

        Ok((
            RpWrite::default(),
            SecureFsWriter {
                file: tokio::fs::File::from_std(file.into_std()),
            },
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SecureFsDeleter {
                root: self.root.clone(),
            }),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let path = backend_path(path).map_err(new_std_io_error)?;
        let display_prefix = if path.as_os_str().is_empty() {
            String::new()
        } else {
            format!("{}/", path.to_string_lossy().replace('\\', "/"))
        };
        let root = self.root.clone();
        let read_dir = common_runtime::spawn_blocking_global(move || {
            let result = (|| {
                let dir = if path.as_os_str().is_empty() {
                    root.dir.open_dir(".")?
                } else {
                    root.dir.open_dir(&path)?
                };
                dir.entries()
            })();

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
        .map_err(new_task_join_error)?
        .map_err(new_std_io_error)?;

        let Some(read_dir) = read_dir else {
            return Ok((RpList::default(), None));
        };
        let current_path = oio::Entry::new(
            if display_prefix.is_empty() {
                "/"
            } else {
                &display_prefix
            },
            Metadata::new(EntryMode::DIR),
        );
        Ok((
            RpList::default(),
            Some(SecureFsLister {
                read_dir: Arc::new(Mutex::new(read_dir)),
                display_prefix,
                entries: vec![current_path].into_iter(),
                done: false,
            }),
        ))
    }
}

struct SecureFsReader {
    file: tokio::fs::File,
    remaining: u64,
}

impl oio::Read for SecureFsReader {
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
    file: tokio::fs::File,
}

impl oio::Write for SecureFsWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        self.file
            .write_all(&buffer.to_bytes())
            .await
            .map_err(new_std_io_error)
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.file.flush().await.map_err(new_std_io_error)?;
        self.file.sync_all().await.map_err(new_std_io_error)?;
        let metadata = self.file.metadata().await.map_err(new_std_io_error)?;
        Ok(Metadata::new(EntryMode::FILE)
            .with_content_length(metadata.len())
            .with_last_modified(Timestamp::try_from(
                metadata.modified().map_err(new_std_io_error)?,
            )?))
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "filesystem writes cannot be aborted without atomic writes",
        ))
    }
}

struct SecureFsLister {
    read_dir: Arc<Mutex<ReadDir>>,
    display_prefix: String,
    entries: IntoIter<oio::Entry>,
    done: bool,
}

impl oio::List for SecureFsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if let Some(entry) = self.entries.next() {
            return Ok(Some(entry));
        }
        if self.done {
            return Ok(None);
        }

        let read_dir = self.read_dir.clone();
        let display_prefix = self.display_prefix.clone();
        let (entries, done) = common_runtime::spawn_blocking_global(move || {
            let mut read_dir = read_dir
                .lock()
                .map_err(|_| io::Error::other("filesystem directory iterator lock is poisoned"))?;
            read_list_batch(&mut read_dir, &display_prefix)
        })
        .await
        .map_err(new_task_join_error)?
        .map_err(new_std_io_error)?;

        self.entries = entries.into_iter();
        self.done = done;
        Ok(self.entries.next())
    }
}

fn read_list_batch(
    read_dir: &mut ReadDir,
    display_prefix: &str,
) -> io::Result<(Vec<oio::Entry>, bool)> {
    let mut entries = Vec::with_capacity(LIST_BATCH_SIZE);
    while entries.len() < LIST_BATCH_SIZE {
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
    let name = entry.file_name().to_string_lossy().to_string();
    let (path, mode) = if file_type.is_dir() {
        (format!("{display_prefix}{name}/"), EntryMode::DIR)
    } else if file_type.is_file() {
        (format!("{display_prefix}{name}"), EntryMode::FILE)
    } else {
        (format!("{display_prefix}{name}"), EntryMode::Unknown)
    };
    let metadata = if mode == EntryMode::Unknown {
        Metadata::new(mode)
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
    root: SecureFsRoot,
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
        common_runtime::spawn_blocking_global(move || {
            let metadata = match root.dir.symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };

            if metadata.is_dir() {
                if args.recursive() {
                    root.dir.remove_dir_all(path)
                } else {
                    root.dir.remove_dir(path)
                }
            } else {
                root.dir.remove_file(path)
            }
        })
        .await
        .map_err(new_task_join_error)?
        .map_err(new_std_io_error)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use common_test_util::temp_dir::create_temp_dir;
    use opendal::ErrorKind;
    use opendal::raw::oio::List;
    use opendal::raw::{Access, OpList};

    use super::{LIST_BATCH_SIZE, SecureFsBackend, SecureFsRoot, read_list_entry};

    #[tokio::test]
    async fn test_lister_streams_entries() {
        let temp_dir = create_temp_dir("secure_fs_lister_streams_entries");
        for index in 0..129 {
            std::fs::write(temp_dir.path().join(format!("{index}.parquet")), []).unwrap();
        }

        let root = SecureFsRoot::open(temp_dir.path()).unwrap();
        let backend = SecureFsBackend::new(root);
        let (_, lister) = backend.list("/", OpList::new()).await.unwrap();
        let mut lister = lister.unwrap();

        assert_eq!(1, lister.entries.len());

        let mut paths = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            paths.push(entry.path().to_string());
            assert!(lister.entries.len() <= LIST_BATCH_SIZE);
        }
        assert_eq!(130, paths.len());
        assert!(paths.iter().any(|path| path == "/"));
        assert!(paths.iter().any(|path| path == "128.parquet"));
    }

    #[tokio::test]
    async fn test_if_not_exists_returns_condition_not_match() {
        let temp_dir = create_temp_dir("secure_fs_if_not_exists");
        let operator = SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();
        operator
            .write("existing", Bytes::from_static(b"original"))
            .await
            .unwrap();

        let error = operator
            .write_with("existing", Bytes::from_static(b"replacement"))
            .if_not_exists(true)
            .await
            .unwrap_err();

        assert_eq!(ErrorKind::ConditionNotMatch, error.kind());
        assert_eq!(
            Bytes::from_static(b"original"),
            operator.read("existing").await.unwrap().to_bytes()
        );
    }

    #[tokio::test]
    async fn test_if_not_exists_does_not_remap_parent_directory_error() {
        let temp_dir = create_temp_dir("secure_fs_if_not_exists_parent_error");
        std::fs::write(temp_dir.path().join("parent"), []).unwrap();
        let operator = SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();

        let error = operator
            .write_with("parent/file", Bytes::new())
            .if_not_exists(true)
            .await
            .unwrap_err();

        assert_eq!(ErrorKind::AlreadyExists, error.kind());
    }

    #[tokio::test]
    async fn test_list_missing_or_non_directory_is_empty() {
        let temp_dir = create_temp_dir("secure_fs_list_missing_or_non_directory");
        std::fs::write(temp_dir.path().join("file"), []).unwrap();
        let operator = SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();

        assert!(operator.list("missing/").await.unwrap().is_empty());
        assert!(operator.list("file/").await.unwrap().is_empty());
    }

    #[test]
    fn test_lister_skips_entry_removed_during_iteration() {
        let temp_dir = create_temp_dir("secure_fs_lister_removed_entry");
        let path = temp_dir.path().join("removed");
        std::fs::write(&path, []).unwrap();
        let root = SecureFsRoot::open(temp_dir.path()).unwrap();
        let mut read_dir = root.dir.entries().unwrap();
        let entry = read_dir.next().unwrap().unwrap();
        std::fs::remove_file(path).unwrap();

        assert!(read_list_entry(entry, "").unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_root_is_unsupported() {
        let temp_dir = create_temp_dir("secure_fs_delete_root");
        let operator = SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();
        operator
            .write("nested/file", Bytes::from_static(b"data"))
            .await
            .unwrap();

        let error = operator.delete_with("/").recursive(true).await.unwrap_err();

        assert_eq!(ErrorKind::Unsupported, error.kind());
        assert!(temp_dir.path().join("nested/file").exists());

        operator
            .delete_with("nested/")
            .recursive(true)
            .await
            .unwrap();
        assert!(!temp_dir.path().join("nested").exists());
    }

    #[tokio::test]
    async fn test_writer_abort_is_unsupported_without_atomic_write() {
        let temp_dir = create_temp_dir("secure_fs_writer_abort");
        std::fs::write(temp_dir.path().join("partial"), b"original").unwrap();
        let operator = SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();
        let mut writer = operator.writer("partial").await.unwrap();
        writer.write(Bytes::from_static(b"partial")).await.unwrap();

        let error = writer.abort().await.unwrap_err();

        assert_eq!(ErrorKind::Unsupported, error.kind());
        assert_eq!(
            b"partial",
            std::fs::read(temp_dir.path().join("partial"))
                .unwrap()
                .as_slice()
        );
    }
}
