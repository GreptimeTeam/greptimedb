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

pub mod azblob;
pub mod fs;
pub mod gcs;
pub mod oss;
pub mod s3;

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use common_telemetry::debug;
use lazy_static::lazy_static;
use object_store::ObjectStore;
use object_store::secure_fs::SecureFsRoot;
use regex::Regex;
use snafu::{IntoError, OptionExt, ResultExt};
use url::{ParseError, Url};

use self::azblob::build_azblob_backend;
use self::fs::build_fs_backend;
use self::gcs::build_gcs_backend;
use self::s3::build_s3_backend;
use crate::error::{self, Result};
use crate::object_store::oss::build_oss_backend;
use crate::util::find_dir_and_filename;

pub const FS_SCHEMA: &str = "FS";
pub const FILE_SCHEMA: &str = "FILE";
pub const S3_SCHEMA: &str = "S3";
pub const OSS_SCHEMA: &str = "OSS";
pub const GCS_SCHEMA: &str = "GCS";
pub const AZBLOB_SCHEMA: &str = "AZBLOB";

/// Controls whether SQL paths may access the local filesystem.
#[derive(Clone, Debug)]
pub enum LocalFileAccess {
    /// Local filesystem paths are rejected.
    Disabled,
    /// Local filesystem paths are confined to a server-configured root.
    Sandboxed { root: LocalFileRoot },
}

impl Default for LocalFileAccess {
    fn default() -> Self {
        Self::Disabled
    }
}

/// An opened server-controlled root for sandboxed SQL file access.
#[derive(Clone, Debug)]
pub struct LocalFileRoot {
    root: Arc<SecureFsRoot>,
    configured_path: Arc<PathBuf>,
}

impl LocalFileAccess {
    /// Creates a sandbox rooted at a server-controlled local directory.
    pub fn sandboxed(root: impl AsRef<Path>) -> Result<Self> {
        let root_path = root.as_ref();
        let configured_path = std::path::absolute(root_path).map_err(|error| {
            error::InvalidLocalFileRootSnafu {
                root: root_path.display().to_string(),
            }
            .into_error(error)
        })?;
        let root =
            SecureFsRoot::open(root_path).with_context(|_| error::InvalidLocalFileRootSnafu {
                root: root_path.display().to_string(),
            })?;
        Ok(Self::Sandboxed {
            root: LocalFileRoot {
                root: Arc::new(root),
                configured_path: Arc::new(configured_path),
            },
        })
    }

    /// Returns the canonical path of the configured sandbox root.
    pub fn sandbox_root(&self) -> Option<&Path> {
        match self {
            Self::Disabled => None,
            Self::Sandboxed { root } => Some(root.root.path()),
        }
    }

    fn authorize(&self, location: &str, path: &Path, trailing_slash: bool) -> Result<String> {
        let LocalFileAccess::Sandboxed { root } = self else {
            return error::LocalFileAccessDisabledSnafu {
                path: location.to_string(),
            }
            .fail();
        };

        let path = normalize_untrusted_path(path).map_err(|reason| {
            error::LocalFileAccessDeniedSnafu {
                path: location.to_string(),
                reason,
            }
            .build()
        })?;
        let relative = if path.is_absolute() {
            strip_local_prefix(&path, root.configured_path.as_path())
                .or_else(|| strip_local_prefix(&path, root.root.path()))
                .ok_or_else(|| {
                    error::LocalFileAccessDeniedSnafu {
                        path: location.to_string(),
                        reason: "absolute path is outside the configured copy root".to_string(),
                    }
                    .build()
                })?
        } else {
            path.as_path()
        };

        let mut authorized = relative
            .components()
            .filter_map(|component| match component {
                Component::CurDir => None,
                Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/");
        if trailing_slash && !authorized.is_empty() {
            authorized.push('/');
        }
        Ok(authorized)
    }

    async fn open_backend_root(
        &self,
        location: &str,
        relative_root: &str,
        create: bool,
    ) -> Result<SecureFsRoot> {
        let LocalFileAccess::Sandboxed { root } = self else {
            return error::LocalFileAccessDisabledSnafu {
                path: location.to_string(),
            }
            .fail();
        };

        let root = root.root.clone();
        let relative_root = relative_root.trim_matches('/').to_string();
        common_runtime::spawn_blocking_global(move || {
            if create {
                root.create_subdir(relative_root)
            } else {
                root.open_subdir(relative_root)
            }
        })
        .await
        .context(error::JoinHandleSnafu)?
        .map_err(|error| {
            debug!(
                "Failed to open an authorized local SQL path inside the copy root, path: {location}, error: {error:?}"
            );
            error::LocalFileAccessDeniedSnafu {
                path: location.to_string(),
                reason: "path could not be safely resolved within the configured copy root"
                    .to_string(),
            }
            .build()
        })
    }
}

/// Converts a configured location into a local path.
///
/// Bare paths and `file://` URLs are local. Other URL schemes return `None`.
pub fn configured_local_path(location: &str) -> Result<Option<PathBuf>> {
    #[cfg(windows)]
    if Path::new(location).is_absolute() {
        return Ok(Some(PathBuf::from(location)));
    }

    let (schema, _, path) = parse_url(location)?;
    match schema.to_uppercase().as_str() {
        FS_SCHEMA => Ok(Some(PathBuf::from(path))),
        FILE_SCHEMA => {
            let url = Url::parse(location).context(error::InvalidUrlSnafu { url: location })?;
            url.to_file_path().map(Some).map_err(|_| {
                error::InvalidLocalFileRootSnafu {
                    root: location.to_string(),
                }
                .into_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "file URL must contain a local absolute path",
                ))
            })
        }
        _ => Ok(None),
    }
}

fn strip_local_prefix<'a>(path: &'a Path, prefix: &Path) -> Option<&'a Path> {
    #[cfg(not(windows))]
    {
        path.strip_prefix(prefix).ok()
    }

    #[cfg(windows)]
    {
        let mut path_components = path.components();
        for prefix_component in prefix.components() {
            let path_component = path_components.next()?;
            if !windows_component_eq(path_component, prefix_component) {
                return None;
            }
        }
        Some(path_components.as_path())
    }
}

#[cfg(windows)]
fn windows_component_eq(left: Component<'_>, right: Component<'_>) -> bool {
    match (left, right) {
        (Component::Prefix(left), Component::Prefix(right)) => {
            windows_os_str_eq(left.as_os_str(), right.as_os_str())
        }
        (Component::Normal(left), Component::Normal(right)) => windows_os_str_eq(left, right),
        (Component::RootDir, Component::RootDir)
        | (Component::CurDir, Component::CurDir)
        | (Component::ParentDir, Component::ParentDir) => true,
        _ => false,
    }
}

#[cfg(windows)]
fn windows_os_str_eq(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt;

    fn ascii_lowercase(value: u16) -> u16 {
        if (u16::from(b'A')..=u16::from(b'Z')).contains(&value) {
            value + u16::from(b'a' - b'A')
        } else {
            value
        }
    }

    left.encode_wide()
        .map(ascii_lowercase)
        .eq(right.encode_wide().map(ascii_lowercase))
}

fn normalize_untrusted_path(path: &Path) -> std::result::Result<PathBuf, String> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => return Err("'..' path components are not allowed".to_string()),
        }
    }
    Ok(normalized)
}

/// Returns `(schema, Option<host>, path)`
pub fn parse_url(url: &str) -> Result<(String, Option<String>, String)> {
    #[cfg(windows)]
    {
        // On Windows, the URL may start with `C:/` or `C:\`.
        if handle_windows_path(url).is_some() {
            return Ok((FS_SCHEMA.to_string(), None, url.to_string()));
        }
    }
    let parsed_url = Url::parse(url);
    match parsed_url {
        Ok(url) => Ok((
            url.scheme().to_string(),
            url.host_str().map(|s| s.to_string()),
            url.path().to_string(),
        )),
        Err(ParseError::RelativeUrlWithoutBase) => {
            Ok((FS_SCHEMA.to_string(), None, url.to_string()))
        }
        Err(err) => Err(err).context(error::InvalidUrlSnafu { url }),
    }
}

pub async fn build_backend(
    url: &str,
    connection: &HashMap<String, String>,
    local_file_access: &LocalFileAccess,
) -> Result<ObjectStore> {
    build_backend_inner(url, connection, local_file_access, false).await
}

/// Builds a backend for an operation that may create the target directory.
pub async fn build_backend_for_write(
    url: &str,
    connection: &HashMap<String, String>,
    local_file_access: &LocalFileAccess,
) -> Result<ObjectStore> {
    build_backend_inner(url, connection, local_file_access, true).await
}

async fn build_backend_inner(
    url: &str,
    connection: &HashMap<String, String>,
    local_file_access: &LocalFileAccess,
    create_local_root: bool,
) -> Result<ObjectStore> {
    let (schema, host, path) = parse_url(url)?;
    let normalized_schema = schema.to_uppercase();

    if normalized_schema == FS_SCHEMA || normalized_schema == FILE_SCHEMA {
        let (local_path, trailing_slash) = if normalized_schema == FILE_SCHEMA {
            let url = Url::parse(url).context(error::InvalidUrlSnafu { url })?;
            let path = url.to_file_path().map_err(|_| {
                error::LocalFileAccessDeniedSnafu {
                    path: url.to_string(),
                    reason: "file URL must contain a local absolute path".to_string(),
                }
                .build()
            })?;
            (path, url.path().ends_with('/'))
        } else {
            (PathBuf::from(&path), path.ends_with('/'))
        };
        let authorized = local_file_access.authorize(url, &local_path, trailing_slash)?;
        let (root, _) = find_dir_and_filename(&authorized);
        let root = local_file_access
            .open_backend_root(url, &root, create_local_root)
            .await?;
        return build_fs_backend(&root);
    }

    let (root, _) = find_dir_and_filename(&path);

    match normalized_schema.as_str() {
        S3_SCHEMA => {
            let host = host.context(error::EmptyHostPathSnafu {
                url: url.to_string(),
            })?;
            Ok(build_s3_backend(&host, &root, connection)?)
        }
        OSS_SCHEMA => {
            let host = host.context(error::EmptyHostPathSnafu {
                url: url.to_string(),
            })?;
            Ok(build_oss_backend(&host, &root, connection)?)
        }
        GCS_SCHEMA => {
            let host = host.context(error::EmptyHostPathSnafu {
                url: url.to_string(),
            })?;
            Ok(build_gcs_backend(&host, &root, connection)?)
        }
        AZBLOB_SCHEMA => {
            let host = host.context(error::EmptyHostPathSnafu {
                url: url.to_string(),
            })?;
            Ok(build_azblob_backend(&host, &root, connection)?)
        }
        _ => error::UnsupportedBackendProtocolSnafu {
            protocol: schema,
            url,
        }
        .fail(),
    }
}

lazy_static! {
    static ref DISK_SYMBOL_PATTERN: Regex = Regex::new(r"^([A-Za-z]:[/\\])").unwrap();
}

pub fn handle_windows_path(url: &str) -> Option<String> {
    DISK_SYMBOL_PATTERN
        .captures(url)
        .map(|captures| captures[0].to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use common_error::ext::{ErrorExt, RetryHint};
    use common_error::status_code::StatusCode;
    use common_test_util::temp_dir::create_temp_dir;
    use url::Url;

    use super::{LocalFileAccess, build_backend, build_backend_for_write, handle_windows_path};
    use crate::error::Error;

    #[test]
    fn test_handle_windows_path() {
        assert_eq!(
            handle_windows_path("C:/to/path/file"),
            Some("C:/".to_string())
        );
        assert_eq!(
            handle_windows_path(r"C:\to\path\file"),
            Some(r"C:\".to_string())
        );
        assert_eq!(handle_windows_path("https://google.com"), None);
        assert_eq!(handle_windows_path("s3://bucket/path/to"), None);
    }

    #[cfg(windows)]
    #[test]
    fn test_windows_local_path_detection_and_prefix() {
        use std::path::{Path, PathBuf};

        let location = r"C:\gtdata";
        assert_eq!(
            super::configured_local_path(location).unwrap(),
            Some(PathBuf::from(location))
        );
        assert_eq!(
            super::parse_url(location).unwrap(),
            ("FS".to_string(), None, location.to_string())
        );
        assert_eq!(
            super::strip_local_prefix(
                Path::new(r"c:\Data\Copy\nested\data.parquet"),
                Path::new(r"C:\data\copy"),
            ),
            Some(Path::new(r"nested\data.parquet"))
        );
    }

    #[tokio::test]
    async fn test_local_file_access_policy() {
        let data_home = create_temp_dir("local_file_access_policy");
        let copy_root = data_home.path().join("copy");
        let internal_dir = data_home.path().join("data");
        fs::create_dir_all(&internal_dir).unwrap();
        fs::write(internal_dir.join("secret"), "secret").unwrap();

        let access = LocalFileAccess::sandboxed(&copy_root).unwrap();
        let connection = HashMap::new();

        let store = build_backend_for_write("nested/data.txt", &connection, &access)
            .await
            .unwrap();
        store.write("data.txt", "first").await.unwrap();
        store.write("data.txt", "second").await.unwrap();
        assert_eq!(
            fs::read_to_string(copy_root.join("nested/data.txt")).unwrap(),
            "second"
        );

        let missing = copy_root.join("missing/directory");
        assert!(
            build_backend("missing/directory/data.txt", &connection, &access)
                .await
                .is_err()
        );
        assert!(!missing.exists());

        let absolute = copy_root.join("nested/data.txt");
        let store = build_backend(absolute.to_str().unwrap(), &connection, &access)
            .await
            .unwrap();
        assert_eq!(store.read("data.txt").await.unwrap().to_vec(), b"second");

        let file_url = Url::from_file_path(&absolute).unwrap().to_string();
        let store = build_backend(&file_url, &connection, &access)
            .await
            .unwrap();
        assert_eq!(store.read("data.txt").await.unwrap().to_vec(), b"second");

        let internal_file = internal_dir.join("secret");
        assert!(matches!(
            build_backend(internal_file.to_str().unwrap(), &connection, &access).await,
            Err(Error::LocalFileAccessDenied { .. })
        ));
        assert!(
            build_backend("../escape/data.txt", &connection, &access)
                .await
                .is_err()
        );

        let outside = data_home.path().join("outside/new");
        assert!(
            build_backend(outside.to_str().unwrap(), &connection, &access)
                .await
                .is_err()
        );
        assert!(!outside.parent().unwrap().exists());

        let prefix_escape = data_home.path().join("copy-not-the-root/new");
        assert!(matches!(
            build_backend(prefix_escape.to_str().unwrap(), &connection, &access).await,
            Err(Error::LocalFileAccessDenied { .. })
        ));
        assert!(!prefix_escape.parent().unwrap().exists());

        let disabled = LocalFileAccess::Disabled;
        let error = build_backend(internal_file.to_str().unwrap(), &connection, &disabled)
            .await
            .unwrap_err();
        assert!(matches!(&error, Error::LocalFileAccessDisabled { .. }));
        assert_eq!(error.status_code(), StatusCode::InvalidArguments);
        assert_eq!(error.retry_hint(), RetryHint::NonRetryable);
        assert!(matches!(
            build_backend(&file_url, &connection, &disabled).await,
            Err(Error::LocalFileAccessDisabled { .. })
        ));
        assert!(matches!(
            build_backend(outside.to_str().unwrap(), &connection, &disabled).await,
            Err(Error::LocalFileAccessDisabled { .. })
        ));
        assert!(!outside.parent().unwrap().exists());
    }

    #[tokio::test]
    async fn test_object_storage_ignores_local_file_policy() {
        let cases = [
            (
                "s3://bucket/path/file.parquet",
                HashMap::from([
                    ("region".to_string(), "us-east-1".to_string()),
                    ("disable_ec2_metadata".to_string(), "true".to_string()),
                ]),
            ),
            (
                "oss://bucket/path/file.parquet",
                HashMap::from([
                    ("endpoint".to_string(), "http://oss.example.com".to_string()),
                    ("allow_anonymous".to_string(), "true".to_string()),
                ]),
            ),
            (
                "gcs://bucket/path/file.parquet",
                HashMap::from([(
                    "endpoint".to_string(),
                    "http://storage.example.com".to_string(),
                )]),
            ),
            (
                "azblob://container/path/file.parquet",
                HashMap::from([
                    (
                        "endpoint".to_string(),
                        "http://storage.example.com".to_string(),
                    ),
                    ("account_name".to_string(), "test".to_string()),
                ]),
            ),
        ];
        for (location, connection) in cases {
            build_backend(location, &connection, &LocalFileAccess::Disabled)
                .await
                .unwrap();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_local_file_access_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp_dir = create_temp_dir("local_file_access_symlink");
        let copy_root = temp_dir.path().join("copy");
        let outside = temp_dir.path().join("outside");
        fs::create_dir_all(&copy_root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret"), "secret").unwrap();
        symlink(&outside, copy_root.join("escape")).unwrap();
        symlink(outside.join("secret"), copy_root.join("secret-link")).unwrap();

        let access = LocalFileAccess::sandboxed(&copy_root).unwrap();
        let connection = HashMap::new();

        assert!(
            build_backend("escape/secret", &connection, &access)
                .await
                .is_err()
        );
        assert!(
            build_backend_for_write("escape/new", &connection, &access)
                .await
                .is_err()
        );
        assert!(!outside.join("new").exists());

        let store = build_backend("secret-link", &connection, &access)
            .await
            .unwrap();
        assert!(store.read("secret-link").await.is_err());
        assert!(store.write("secret-link", "overwritten").await.is_err());
        assert_eq!(
            fs::read_to_string(outside.join("secret")).unwrap(),
            "secret"
        );
    }
}
