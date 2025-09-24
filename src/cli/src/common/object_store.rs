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
    /// Builds the object store from the config.
    pub fn build(&self) -> Result<Option<ObjectStore>, BoxedError> {
        let object_store = if self.enable_s3 {
            let s3 = S3Connection::from(self.s3.clone());
            common_telemetry::info!("Building object store with s3: {:?}", s3);
            Some(
                ObjectStore::new(S3::from(&s3))
                    .context(error::InitBackendSnafu)
                    .map_err(BoxedError::new)?
                    .finish(),
            )
        } else if self.enable_oss {
            let oss = OssConnection::from(self.oss.clone());
            common_telemetry::info!("Building object store with oss: {:?}", oss);
            Some(
                ObjectStore::new(Oss::from(&oss))
                    .context(error::InitBackendSnafu)
                    .map_err(BoxedError::new)?
                    .finish(),
            )
        } else if self.enable_gcs {
            let gcs = GcsConnection::from(self.gcs.clone());
            common_telemetry::info!("Building object store with gcs: {:?}", gcs);
            Some(
                ObjectStore::new(Gcs::from(&gcs))
                    .context(error::InitBackendSnafu)
                    .map_err(BoxedError::new)?
                    .finish(),
            )
        } else if self.enable_azblob {
            let azblob = AzblobConnection::from(self.azblob.clone());
            common_telemetry::info!("Building object store with azblob: {:?}", azblob);
            Some(
                ObjectStore::new(Azblob::from(&azblob))
                    .context(error::InitBackendSnafu)
                    .map_err(BoxedError::new)?
                    .finish(),
            )
        } else {
            None
        };

        let object_store = object_store
            .map(|object_store| with_instrument_layers(with_retry_layers(object_store), false));

        Ok(object_store)
    }
}
