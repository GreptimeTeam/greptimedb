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
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct FileConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub name: String,
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
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            name: String::default(),
            bucket: String::default(),
            root: String::default(),
            access_key_id: SecretString::from(String::default()),
            secret_access_key: SecretString::from(String::default()),
            enable_virtual_host_style: false,
            endpoint: Option::default(),
            region: Option::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

impl PartialEq for S3Config {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.bucket == other.bucket
            && self.root == other.root
            && self.access_key_id.expose_secret() == other.access_key_id.expose_secret()
            && self.secret_access_key.expose_secret() == other.secret_access_key.expose_secret()
            && self.endpoint == other.endpoint
            && self.region == other.region
            && self.enable_virtual_host_style == other.enable_virtual_host_style
            && self.cache == other.cache
            && self.http_client == other.http_client
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OssConfig {
    pub name: String,
    pub bucket: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub access_key_secret: SecretString,
    pub endpoint: String,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl PartialEq for OssConfig {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.bucket == other.bucket
            && self.root == other.root
            && self.access_key_id.expose_secret() == other.access_key_id.expose_secret()
            && self.access_key_secret.expose_secret() == other.access_key_secret.expose_secret()
            && self.endpoint == other.endpoint
            && self.cache == other.cache
            && self.http_client == other.http_client
    }
}

impl Default for OssConfig {
    fn default() -> Self {
        Self {
            name: String::default(),
            bucket: String::default(),
            root: String::default(),
            access_key_id: SecretString::from(String::default()),
            access_key_secret: SecretString::from(String::default()),
            endpoint: String::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AzblobConfig {
    pub name: String,
    pub container: String,
    pub root: String,
    #[serde(skip_serializing)]
    pub account_name: SecretString,
    #[serde(skip_serializing)]
    pub account_key: SecretString,
    pub endpoint: String,
    pub sas_token: Option<String>,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl PartialEq for AzblobConfig {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.container == other.container
            && self.root == other.root
            && self.account_name.expose_secret() == other.account_name.expose_secret()
            && self.account_key.expose_secret() == other.account_key.expose_secret()
            && self.endpoint == other.endpoint
            && self.sas_token == other.sas_token
            && self.cache == other.cache
            && self.http_client == other.http_client
    }
}
impl Default for AzblobConfig {
    fn default() -> Self {
        Self {
            name: String::default(),
            container: String::default(),
            root: String::default(),
            account_name: SecretString::from(String::default()),
            account_key: SecretString::from(String::default()),
            endpoint: String::default(),
            sas_token: Option::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsConfig {
    pub name: String,
    pub root: String,
    pub bucket: String,
    pub scope: String,
    #[serde(skip_serializing)]
    pub credential_path: SecretString,
    #[serde(skip_serializing)]
    pub credential: SecretString,
    pub endpoint: String,
    #[serde(flatten)]
    pub cache: ObjectStorageCacheConfig,
    pub http_client: HttpClientConfig,
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            name: String::default(),
            root: String::default(),
            bucket: String::default(),
            scope: String::default(),
            credential_path: SecretString::from(String::default()),
            credential: SecretString::from(String::default()),
            endpoint: String::default(),
            cache: ObjectStorageCacheConfig::default(),
            http_client: HttpClientConfig::default(),
        }
    }
}

impl PartialEq for GcsConfig {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.root == other.root
            && self.bucket == other.bucket
            && self.scope == other.scope
            && self.credential_path.expose_secret() == other.credential_path.expose_secret()
            && self.credential.expose_secret() == other.credential.expose_secret()
            && self.endpoint == other.endpoint
            && self.cache == other.cache
            && self.http_client == other.http_client
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct ObjectStorageCacheConfig {
    /// The local file cache directory
    pub cache_path: Option<String>,
    /// The cache capacity in bytes
    pub cache_capacity: Option<ReadableSize>,
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
