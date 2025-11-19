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

use common_base::secrets::SecretString;
use common_error::ext::BoxedError;
use object_store::services::{Azblob, Fs, Gcs, Oss, S3};
use object_store::util::{with_instrument_layers, with_retry_layers};
use object_store::{AzblobConnection, GcsConnection, ObjectStore, OssConnection, S3Connection};
use paste::paste;
use snafu::ResultExt;

use crate::error::{self};

macro_rules! wrap_with_clap_prefix {
    (
        $new_name:ident, $prefix:literal, $base:ty, {
            $( $( #[doc = $doc:expr] )? $( #[alias = $alias:literal] )? $field:ident : $type:ty $( = $default:expr )? ),* $(,)?
        }
    ) => {
        paste!{
            #[derive(clap::Parser, Debug, Clone, PartialEq, Default)]
            pub struct $new_name {
                $(
                    $( #[doc = $doc] )?
                    $( #[clap(alias = $alias)] )?
                    #[clap(long $(, default_value_t = $default )? )]
                    [<$prefix $field>]: $type,
                )*
            }

            impl From<$new_name> for $base {
                fn from(w: $new_name) -> Self {
                    Self {
                        $( $field: w.[<$prefix $field>] ),*
                    }
                }
            }
        }
    };
}

wrap_with_clap_prefix! {
    PrefixedAzblobConnection,
    "azblob-",
    AzblobConnection,
    {
        #[doc = "The container of the object store."]
        container: String = Default::default(),
        #[doc = "The root of the object store."]
        root: String = Default::default(),
        #[doc = "The account name of the object store."]
        account_name: SecretString = Default::default(),
        #[doc = "The account key of the object store."]
        account_key: SecretString = Default::default(),
        #[doc = "The endpoint of the object store."]
        endpoint: String = Default::default(),
        #[doc = "The SAS token of the object store."]
        sas_token: Option<String>,
    }
}

impl PrefixedAzblobConnection {
    /// Get the container name.
    pub fn container(&self) -> &str {
        &self.azblob_container
    }

    /// Get the root path.
    pub fn root(&self) -> &str {
        &self.azblob_root
    }

    /// Get the account name.
    pub fn account_name(&self) -> &SecretString {
        &self.azblob_account_name
    }

    /// Get the account key.
    pub fn account_key(&self) -> &SecretString {
        &self.azblob_account_key
    }

    /// Get the endpoint.
    pub fn endpoint(&self) -> &str {
        &self.azblob_endpoint
    }

    /// Get the SAS token.
    pub fn sas_token(&self) -> Option<&String> {
        self.azblob_sas_token.as_ref()
    }
}

wrap_with_clap_prefix! {
    PrefixedS3Connection,
    "s3-",
    S3Connection,
    {
        #[doc = "The bucket of the object store."]
        bucket: String = Default::default(),
        #[doc = "The root of the object store."]
        root: String = Default::default(),
        #[doc = "The access key ID of the object store."]
        access_key_id: SecretString = Default::default(),
        #[doc = "The secret access key of the object store."]
        secret_access_key: SecretString = Default::default(),
        #[doc = "The endpoint of the object store."]
        endpoint: Option<String>,
        #[doc = "The region of the object store."]
        region: Option<String>,
        #[doc = "Enable virtual host style for the object store."]
        enable_virtual_host_style: bool = Default::default(),
    }
}

impl PrefixedS3Connection {
    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.s3_bucket
    }

    /// Get the root path.
    pub fn root(&self) -> &str {
        &self.s3_root
    }

    /// Get the access key ID.
    pub fn access_key_id(&self) -> &SecretString {
        &self.s3_access_key_id
    }

    /// Get the secret access key.
    pub fn secret_access_key(&self) -> &SecretString {
        &self.s3_secret_access_key
    }

    /// Get the endpoint.
    pub fn endpoint(&self) -> Option<&String> {
        self.s3_endpoint.as_ref()
    }

    /// Get the region.
    pub fn region(&self) -> Option<&String> {
        self.s3_region.as_ref()
    }

    /// Check if virtual host style is enabled.
    pub fn enable_virtual_host_style(&self) -> bool {
        self.s3_enable_virtual_host_style
    }
}

wrap_with_clap_prefix! {
    PrefixedOssConnection,
    "oss-",
    OssConnection,
    {
        #[doc = "The bucket of the object store."]
        bucket: String = Default::default(),
        #[doc = "The root of the object store."]
        root: String = Default::default(),
        #[doc = "The access key ID of the object store."]
        access_key_id: SecretString = Default::default(),
        #[doc = "The access key secret of the object store."]
        access_key_secret: SecretString = Default::default(),
        #[doc = "The endpoint of the object store."]
        endpoint: String = Default::default(),
    }
}

impl PrefixedOssConnection {
    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.oss_bucket
    }

    /// Get the root path.
    pub fn root(&self) -> &str {
        &self.oss_root
    }

    /// Get the access key ID.
    pub fn access_key_id(&self) -> &SecretString {
        &self.oss_access_key_id
    }

    /// Get the access key secret.
    pub fn access_key_secret(&self) -> &SecretString {
        &self.oss_access_key_secret
    }

    /// Get the endpoint.
    pub fn endpoint(&self) -> &str {
        &self.oss_endpoint
    }
}

wrap_with_clap_prefix! {
    PrefixedGcsConnection,
    "gcs-",
    GcsConnection,
    {
        #[doc = "The root of the object store."]
        root: String = Default::default(),
        #[doc = "The bucket of the object store."]
        bucket: String = Default::default(),
        #[doc = "The scope of the object store."]
        scope: String = Default::default(),
        #[doc = "The credential path of the object store."]
        credential_path: SecretString = Default::default(),
        #[doc = "The credential of the object store."]
        credential: SecretString = Default::default(),
        #[doc = "The endpoint of the object store."]
        endpoint: String = Default::default(),
    }
}

impl PrefixedGcsConnection {
    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.gcs_bucket
    }

    /// Get the root path.
    pub fn root(&self) -> &str {
        &self.gcs_root
    }

    /// Get the scope.
    pub fn scope(&self) -> &str {
        &self.gcs_scope
    }

    /// Get the credential path.
    pub fn credential_path(&self) -> &SecretString {
        &self.gcs_credential_path
    }

    /// Get the credential.
    pub fn credential(&self) -> &SecretString {
        &self.gcs_credential
    }

    /// Get the endpoint.
    pub fn endpoint(&self) -> &str {
        &self.gcs_endpoint
    }
}

/// common config for object store.
#[derive(clap::Parser, Debug, Clone, PartialEq, Default)]
pub struct ObjectStoreConfig {
    /// Whether to use S3 object store.
    #[clap(long, alias = "s3")]
    pub enable_s3: bool,

    #[clap(flatten)]
    pub s3: PrefixedS3Connection,

    /// Whether to use OSS.
    #[clap(long, alias = "oss")]
    pub enable_oss: bool,

    #[clap(flatten)]
    pub oss: PrefixedOssConnection,

    /// Whether to use GCS.
    #[clap(long, alias = "gcs")]
    pub enable_gcs: bool,

    #[clap(flatten)]
    pub gcs: PrefixedGcsConnection,

    /// Whether to use Azure Blob.
    #[clap(long, alias = "azblob")]
    pub enable_azblob: bool,

    #[clap(flatten)]
    pub azblob: PrefixedAzblobConnection,
}

/// Creates a new file system object store.
pub fn new_fs_object_store(root: &str) -> std::result::Result<ObjectStore, BoxedError> {
    let builder = Fs::default().root(root);
    let object_store = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)
        .map_err(BoxedError::new)?
        .finish();

    Ok(with_instrument_layers(object_store, false))
}

impl ObjectStoreConfig {
    /// Builds the object store with S3.
    pub fn build_s3(&self) -> Result<ObjectStore, BoxedError> {
        let s3 = S3Connection::from(self.s3.clone());
        common_telemetry::info!("Building object store with s3: {:?}", s3);
        let object_store = ObjectStore::new(S3::from(&s3))
            .context(error::InitBackendSnafu)
            .map_err(BoxedError::new)?
            .finish();
        Ok(with_instrument_layers(
            with_retry_layers(object_store),
            false,
        ))
    }

    /// Builds the object store with OSS.
    pub fn build_oss(&self) -> Result<ObjectStore, BoxedError> {
        let oss = OssConnection::from(self.oss.clone());
        common_telemetry::info!("Building object store with oss: {:?}", oss);
        let object_store = ObjectStore::new(Oss::from(&oss))
            .context(error::InitBackendSnafu)
            .map_err(BoxedError::new)?
            .finish();
        Ok(with_instrument_layers(
            with_retry_layers(object_store),
            false,
        ))
    }

    /// Builds the object store with GCS.
    pub fn build_gcs(&self) -> Result<ObjectStore, BoxedError> {
        let gcs = GcsConnection::from(self.gcs.clone());
        common_telemetry::info!("Building object store with gcs: {:?}", gcs);
        let object_store = ObjectStore::new(Gcs::from(&gcs))
            .context(error::InitBackendSnafu)
            .map_err(BoxedError::new)?
            .finish();
        Ok(with_instrument_layers(
            with_retry_layers(object_store),
            false,
        ))
    }

    /// Builds the object store with Azure Blob.
    pub fn build_azblob(&self) -> Result<ObjectStore, BoxedError> {
        let azblob = AzblobConnection::from(self.azblob.clone());
        common_telemetry::info!("Building object store with azblob: {:?}", azblob);
        let object_store = ObjectStore::new(Azblob::from(&azblob))
            .context(error::InitBackendSnafu)
            .map_err(BoxedError::new)?
            .finish();
        Ok(with_instrument_layers(
            with_retry_layers(object_store),
            false,
        ))
    }

    /// Builds the object store from the config.
    pub fn build(&self) -> Result<Option<ObjectStore>, BoxedError> {
        if self.enable_s3 {
            self.build_s3().map(Some)
        } else if self.enable_oss {
            self.build_oss().map(Some)
        } else if self.enable_gcs {
            self.build_gcs().map(Some)
        } else if self.enable_azblob {
            self.build_azblob().map(Some)
        } else {
            Ok(None)
        }
    }
}
