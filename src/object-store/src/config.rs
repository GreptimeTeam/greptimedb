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

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_base::secrets::{ExposeSecret, SecretString};
use opendal::services::{Azblob, Gcs, Oss, S3};
use serde::{Deserialize, Serialize};

use crate::util;

const DEFAULT_OBJECT_STORE_CACHE_SIZE: ReadableSize = ReadableSize::gb(5);

/// Object storage config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    File(FileConfig),
    S3(S3Config),
    Oss(OssConfig),
    Azblob(AzblobConfig),
    Gcs(GcsConfig),
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::File(FileConfig {})
    }
}

impl ObjectStoreConfig {
    /// Returns the object storage type name, such as `S3`, `Oss` etc.
    pub fn provider_name(&self) -> &'static str {
        match self {
            Self::File(_) => "File",
            Self::S3(_) => "S3",
            Self::Oss(_) => "Oss",
            Self::Azblob(_) => "Azblob",
            Self::Gcs(_) => "Gcs",
        }
    }

    /// Returns true when it's a remote object storage such as AWS s3 etc.
    pub fn is_object_storage(&self) -> bool {
        !matches!(self, Self::File(_))
    }

    /// Returns the object storage configuration name, return the provider name if it's empty.
    pub fn config_name(&self) -> &str {
        let name = match self {
            // file storage doesn't support name
            Self::File(_) => self.provider_name(),
            Self::S3(s3) => &s3.name,
            Self::Oss(oss) => &oss.name,
            Self::Azblob(az) => &az.name,
            Self::Gcs(gcs) => &gcs.name,
        };

        if name.trim().is_empty() {
            return self.provider_name();
        }

        name
    }

    /// Returns the object storage cache configuration.
    pub fn cache_config(&self) -> Option<&ObjectStorageCacheConfig> {
        match self {
            Self::File(_) => None,
            Self::S3(s3) => Some(&s3.cache),
            Self::Oss(oss) => Some(&oss.cache),
            Self::Azblob(az) => Some(&az.cache),
            Self::Gcs(gcs) => Some(&gcs.cache),
        }
    }

    /// Returns the mutable object storage cache configuration.
    pub fn cache_config_mut(&mut self) -> Option<&mut ObjectStorageCacheConfig> {
        match self {
            Self::File(_) => None,
            Self::S3(s3) => Some(&mut s3.cache),
            Self::Oss(oss) => Some(&mut oss.cache),
            Self::Azblob(az) => Some(&mut az.cache),
            Self::Gcs(gcs) => Some(&mut gcs.cache),
        }
    }
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct FileConfig {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct S3Connection {
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub secret_access_key: SecretString,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Enable virtual host style so that opendal will send API requests in virtual host style instead of path style.
    /// By default, opendal will send API to https://s3.us-east-1.amazonaws.com/bucket_name
    /// Enabled, opendal will send API to https://bucket_name.s3.us-east-1.amazonaws.com
    pub enable_virtual_host_style: bool,
}

impl From<&S3Connection> for S3 {
    fn from(connection: &S3Connection) -> Self {
        let root = util::normalize_dir(&connection.root);

        let mut builder = S3::default()
            .root(&root)
            .bucket(&connection.bucket)
            .access_key_id(connection.access_key_id.expose_secret())
            .secret_access_key(connection.secret_access_key.expose_secret());

        if let Some(endpoint) = &connection.endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Some(region) = &connection.region {
            builder = builder.region(region);
        }
        if connection.enable_virtual_host_style {
            builder = builder.enable_virtual_host_style();
        }

        builder
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct S3Config {
    pub name: String,
    #[serde(flatten)]
    pub connection: S3Connection,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct OssConnection {
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub access_key_secret: SecretString,
    pub endpoint: String,
}

impl From<&OssConnection> for Oss {
    fn from(connection: &OssConnection) -> Self {
        let root = util::normalize_dir(&connection.root);
        Oss::default()
            .root(&root)
            .bucket(&connection.bucket)
            .endpoint(&connection.endpoint)
            .access_key_id(connection.access_key_id.expose_secret())
            .access_key_secret(connection.access_key_secret.expose_secret())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct OssConfig {
    pub name: String,
    #[serde(flatten)]
    pub connection: OssConnection,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct AzblobConnection {
    pub container: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub account_name: SecretString,
    #[serde(skip_serializing)]
    pub account_key: SecretString,
    pub endpoint: String,
    pub sas_token: Option<String>,
}

impl From<&AzblobConnection> for Azblob {
    fn from(connection: &AzblobConnection) -> Self {
        let root = util::normalize_dir(&connection.root);
        let mut builder = Azblob::default()
            .root(&root)
            .container(&connection.container)
            .endpoint(&connection.endpoint)
            .account_name(connection.account_name.expose_secret())
            .account_key(connection.account_key.expose_secret());

        if let Some(token) = &connection.sas_token {
            builder = builder.sas_token(token);
        };

        builder
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct AzblobConfig {
    pub name: String,
    #[serde(flatten)]
    pub connection: AzblobConnection,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct GcsConnection {
    pub root: String,
    pub bucket: String,
    pub scope: String,
    #[serde(skip_serializing)]
    pub credential_path: SecretString,
    #[serde(skip_serializing)]
    pub credential: SecretString,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct GcsConfig {
    pub name: String,
    #[serde(flatten)]
    pub connection: GcsConnection,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl From<&GcsConnection> for Gcs {
    fn from(connection: &GcsConnection) -> Self {
        let root = util::normalize_dir(&connection.root);
        Gcs::default()
            .root(&root)
            .bucket(&connection.bucket)
            .scope(&connection.scope)
            .credential_path(connection.credential_path.expose_secret())
            .credential(connection.credential.expose_secret())
            .endpoint(&connection.endpoint)
    }
}
/// The http client options to the storage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct HttpClientConfig {
    /// The maximum idle connection per host allowed in the pool.
    pub(crate) pool_max_idle_per_host: u32,

    /// The timeout for only the connect phase of a http client.
    #[serde(with = "humantime_serde")]
    pub(crate) connect_timeout: Duration,

    /// The total request timeout, applied from when the request starts connecting until the response body has finished.
    /// Also considered a total deadline.
    #[serde(with = "humantime_serde")]
    pub(crate) timeout: Duration,

    /// The timeout for idle sockets being kept-alive.
    #[serde(with = "humantime_serde")]
    pub(crate) pool_idle_timeout: Duration,

    /// Skip SSL certificate validation (insecure)
    pub skip_ssl_validation: bool,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            pool_max_idle_per_host: 1024,
            connect_timeout: Duration::from_secs(30),
            timeout: Duration::from_secs(30),
            pool_idle_timeout: Duration::from_secs(90),
            skip_ssl_validation: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ObjectStorageCacheConfig {
    /// Whether to enable read cache. If not set, the read cache will be enabled by default.
    pub enable_read_cache: bool,
    /// The local file cache directory
    pub cache_path: String,
    /// The cache capacity in bytes
    pub cache_capacity: ReadableSize,
}

impl Default for ObjectStorageCacheConfig {
    fn default() -> Self {
        Self {
            enable_read_cache: true,
            // The cache directory is set to the value of data_home in the build_cache_layer process.
            cache_path: String::default(),
            cache_capacity: DEFAULT_OBJECT_STORE_CACHE_SIZE,
        }
    }
}

impl ObjectStorageCacheConfig {
    /// Sanitize the `ObjectStorageCacheConfig` to ensure the config is valid.
    pub fn sanitize(&mut self, data_home: &str) {
        // If `cache_path` is unset, default to use `${data_home}` as the local read cache directory.
        if self.cache_path.is_empty() {
            self.cache_path = data_home.to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ObjectStoreConfig;

    #[test]
    fn test_config_name() {
        let object_store_config = ObjectStoreConfig::default();
        assert_eq!("File", object_store_config.config_name());

        let s3_config = ObjectStoreConfig::S3(S3Config::default());
        assert_eq!("S3", s3_config.config_name());
        assert_eq!("S3", s3_config.provider_name());

        let s3_config = ObjectStoreConfig::S3(S3Config {
            name: "test".to_string(),
            ..Default::default()
        });
        assert_eq!("test", s3_config.config_name());
        assert_eq!("S3", s3_config.provider_name());
    }

    #[test]
    fn test_is_object_storage() {
        let store = ObjectStoreConfig::default();
        assert!(!store.is_object_storage());
        let s3_config = ObjectStoreConfig::S3(S3Config::default());
        assert!(s3_config.is_object_storage());
        let oss_config = ObjectStoreConfig::Oss(OssConfig::default());
        assert!(oss_config.is_object_storage());
        let gcs_config = ObjectStoreConfig::Gcs(GcsConfig::default());
        assert!(gcs_config.is_object_storage());
        let azblob_config = ObjectStoreConfig::Azblob(AzblobConfig::default());
        assert!(azblob_config.is_object_storage());
    }
}
