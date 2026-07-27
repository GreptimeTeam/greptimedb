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
use std::sync::Arc;
use std::vec::IntoIter;
use std::{fmt, io};

use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use opendal::raw::*;
use opendal::{Buffer, Capability, EntryMode, Metadata, Operator, OperatorBuilder, Result};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

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
    type Lister = SecureFsLister;
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
        let file = common_runtime::spawn_blocking_global(move || {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                root.dir.create_dir_all(parent)?;
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
            root.dir.open_with(path, &options)
        })
        .await
        .map_err(new_task_join_error)?
        .map_err(new_std_io_error)?;

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
        let entries = common_runtime::spawn_blocking_global(move || {
            let dir = if path.as_os_str().is_empty() {
                root.dir.open_dir(".")?
            } else {
                root.dir.open_dir(&path)?
            };
            let mut entries = Vec::new();
            entries.push(oio::Entry::new(
                if display_prefix.is_empty() {
                    "/"
                } else {
                    &display_prefix
                },
                Metadata::new(EntryMode::DIR),
            ));

            for entry in dir.entries()? {
                let entry = entry?;
                let file_type = entry.file_type()?;
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
                    metadata_from_fs(entry.metadata()?)
                        .map_err(|error| io::Error::other(error.to_string()))?
                };
                entries.push(oio::Entry::new(&path, metadata));
            }
            Ok::<_, io::Error>(entries)
        })
        .await
        .map_err(new_task_join_error)?
        .map_err(new_std_io_error)?;

        Ok((
            RpList::default(),
            SecureFsLister {
                entries: entries.into_iter(),
            },
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
        Ok(())
    }
}

struct SecureFsLister {
    entries: IntoIter<oio::Entry>,
}

impl oio::List for SecureFsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(self.entries.next())
    }
}

struct SecureFsDeleter {
    root: SecureFsRoot,
}

impl oio::OneShotDelete for SecureFsDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let path = backend_path(&path).map_err(new_std_io_error)?;
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

    use super::SecureFsRoot;

    #[tokio::test]
    async fn test_writer_abort_succeeds() {
        let temp_dir = create_temp_dir("secure_fs_writer_abort");
        let operator = SecureFsRoot::open(temp_dir.path())
            .unwrap()
            .build_operator();
        let mut writer = operator.writer("partial").await.unwrap();
        writer.write(Bytes::from_static(b"partial")).await.unwrap();
        writer.abort().await.unwrap();
    }
}
