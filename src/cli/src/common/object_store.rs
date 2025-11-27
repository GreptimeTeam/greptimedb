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

use common_base::secrets::{ExposeSecret, SecretString};
use common_error::ext::BoxedError;
use object_store::services::{Azblob, Fs, Gcs, Oss, S3};
use object_store::util::{with_instrument_layers, with_retry_layers};
use object_store::{AzblobConnection, GcsConnection, ObjectStore, OssConnection, S3Connection};
use paste::paste;
use snafu::ResultExt;

use crate::error::{self};

/// Trait to convert CLI field types to target struct field types.
/// This enables `Option<SecretString>` (CLI) -> `SecretString` (target) conversions,
/// allowing us to distinguish "not provided" from "provided but empty".
trait IntoField<T> {
    fn into_field(self) -> T;
}

/// Identity conversion for types that are the same.
impl<T> IntoField<T> for T {
    fn into_field(self) -> T {
        self
    }
}

/// Convert `Option<SecretString>` to `SecretString`, using default for None.
impl IntoField<SecretString> for Option<SecretString> {
    fn into_field(self) -> SecretString {
        self.unwrap_or_default()
    }
}

/// Check if an `Option<SecretString>` is effectively empty.
/// Returns `true` if:
/// - `None` (user didn't provide the argument)
/// - `Some("")` (user provided an empty string)
fn is_secret_empty(secret: &Option<SecretString>) -> bool {
    secret.as_ref().is_none_or(|s| s.expose_secret().is_empty())
}

/// Check if an `Option<SecretString>` is effectively non-empty.
/// Returns `true` only if user provided a non-empty secret value.
fn is_secret_provided(secret: &Option<SecretString>) -> bool {
    !is_secret_empty(secret)
}

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
                    pub [<$prefix $field>]: $type,
                )*
            }

            impl From<$new_name> for $base {
                fn from(w: $new_name) -> Self {
                    Self {
                        // Use into_field() to handle Option<SecretString> -> SecretString conversion
                        $( $field: w.[<$prefix $field>].into_field() ),*
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
        account_name: Option<SecretString>,
        #[doc = "The account key of the object store."]
        account_key: Option<SecretString>,
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
        access_key_id: Option<SecretString>,
        #[doc = "The secret access key of the object store."]
        secret_access_key: Option<SecretString>,
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
        access_key_id: Option<SecretString>,
        #[doc = "The access key secret of the object store."]
        access_key_secret: Option<SecretString>,
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
        credential_path: Option<SecretString>,
        #[doc = "The credential of the object store."]
        credential: Option<SecretString>,
        #[doc = "The endpoint of the object store."]
        endpoint: String = Default::default(),
    }
}

/// common config for object store.
#[derive(clap::Parser, Debug, Clone, PartialEq, Default)]
#[clap(group(clap::ArgGroup::new("storage_backend").required(false).multiple(false)))]
pub struct ObjectStoreConfig {
    /// Whether to use S3 object store.
    #[clap(long, alias = "s3", group = "storage_backend")]
    pub enable_s3: bool,

    #[clap(flatten)]
    pub s3: PrefixedS3Connection,

    /// Whether to use OSS.
    #[clap(long, alias = "oss", group = "storage_backend")]
    pub enable_oss: bool,

    #[clap(flatten)]
    pub oss: PrefixedOssConnection,

    /// Whether to use GCS.
    #[clap(long, alias = "gcs", group = "storage_backend")]
    pub enable_gcs: bool,

    #[clap(flatten)]
    pub gcs: PrefixedGcsConnection,

    /// Whether to use Azure Blob.
    #[clap(long, alias = "azblob", group = "storage_backend")]
    pub enable_azblob: bool,

    #[clap(flatten)]
    pub azblob: PrefixedAzblobConnection,
}

/// This function is called when the user chooses to use local filesystem storage
/// (via `--output-dir`) instead of a remote storage backend. It ensures that the user
/// hasn't accidentally or incorrectly provided configuration for remote storage backends
/// (S3, OSS, GCS, or Azure Blob) without enabling them.
///
/// # Validation Rules
///
/// For each storage backend (S3, OSS, GCS, Azblob), this function validates:
/// 1. **When backend is enabled** (e.g., `--s3`): All required fields must be non-empty
/// 2. **When backend is disabled**: No configuration fields should be provided
///
/// The second rule is critical for filesystem usage: if a user provides something like
/// `--s3-bucket my-bucket` without `--s3`, it likely indicates a configuration error
/// that should be caught early.
///
/// # Examples
///
/// Valid usage with local filesystem:
/// ```bash
/// # No remote storage config provided - OK
/// export --output-dir /tmp/data --addr localhost:4000
/// ```
///
/// Invalid usage (caught by this function):
/// ```bash
/// # ERROR: S3 config provided but --s3 not enabled
/// export --output-dir /tmp/data --s3-bucket my-bucket --addr localhost:4000
/// ```
///
/// # Errors
///
/// Returns an error if any remote storage configuration is detected without the
/// corresponding enable flag (--s3, --oss, --gcs, or --azblob).
pub fn validate_fs(config: &ObjectStoreConfig) -> std::result::Result<(), BoxedError> {
    config.validate_s3()?;
    config.validate_oss()?;
    config.validate_gcs()?;
    config.validate_azblob()?;
    Ok(())
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

macro_rules! gen_object_store_builder {
    ($method:ident, $field:ident, $conn_type:ty, $service_type:ty) => {
        pub fn $method(&self) -> Result<ObjectStore, BoxedError> {
            let config = <$conn_type>::from(self.$field.clone());
            common_telemetry::info!(
                "Building object store with {}: {:?}",
                stringify!($field),
                config
            );
            let object_store = ObjectStore::new(<$service_type>::from(&config))
                .context(error::InitBackendSnafu)
                .map_err(BoxedError::new)?
                .finish();
            Ok(with_instrument_layers(
                with_retry_layers(object_store),
                false,
            ))
        }
    };
}

impl ObjectStoreConfig {
    gen_object_store_builder!(build_s3, s3, S3Connection, S3);

    gen_object_store_builder!(build_oss, oss, OssConnection, Oss);

    gen_object_store_builder!(build_gcs, gcs, GcsConnection, Gcs);

    gen_object_store_builder!(build_azblob, azblob, AzblobConnection, Azblob);

    pub fn validate_s3(&self) -> Result<(), BoxedError> {
        let s3 = &self.s3;

        if self.enable_s3 {
            // Check required fields (root is optional for S3)
            let mut missing = Vec::new();
            if s3.s3_bucket.is_empty() {
                missing.push("bucket");
            }
            if is_secret_empty(&s3.s3_access_key_id) {
                missing.push("access key ID");
            }
            if is_secret_empty(&s3.s3_secret_access_key) {
                missing.push("secret access key");
            }

            if !missing.is_empty() {
                return Err(BoxedError::new(
                    error::MissingConfigSnafu {
                        msg: format!(
                            "S3 {} must be set when --s3 is enabled.",
                            missing.join(", "),
                        ),
                    }
                    .build(),
                ));
            }
        } else {
            // When disabled, check that no meaningful configuration is provided
            if !s3.s3_bucket.is_empty()
                || !s3.s3_root.is_empty()
                || s3.s3_endpoint.is_some()
                || s3.s3_region.is_some()
                || s3.s3_enable_virtual_host_style
                || is_secret_provided(&s3.s3_access_key_id)
                || is_secret_provided(&s3.s3_secret_access_key)
            {
                return Err(BoxedError::new(
                    error::InvalidArgumentsSnafu {
                        msg: "S3 configuration is set but --s3 is not enabled.".to_string(),
                    }
                    .build(),
                ));
            }
        }

        Ok(())
    }

    pub fn validate_oss(&self) -> Result<(), BoxedError> {
        let oss = &self.oss;

        if self.enable_oss {
            // Check required fields
            let mut missing = Vec::new();
            if oss.oss_bucket.is_empty() {
                missing.push("bucket");
            }
            if oss.oss_root.is_empty() {
                missing.push("root");
            }
            if is_secret_empty(&oss.oss_access_key_id) {
                missing.push("access key ID");
            }
            if is_secret_empty(&oss.oss_access_key_secret) {
                missing.push("access key secret");
            }
            if oss.oss_endpoint.is_empty() {
                missing.push("endpoint");
            }

            if !missing.is_empty() {
                return Err(BoxedError::new(
                    error::MissingConfigSnafu {
                        msg: format!(
                            "OSS {} must be set when --oss is enabled.",
                            missing.join(", "),
                        ),
                    }
                    .build(),
                ));
            }
        } else {
            // When disabled, check that no meaningful configuration is provided
            if !oss.oss_bucket.is_empty()
                || !oss.oss_root.is_empty()
                || !oss.oss_endpoint.is_empty()
                || is_secret_provided(&oss.oss_access_key_id)
                || is_secret_provided(&oss.oss_access_key_secret)
            {
                return Err(BoxedError::new(
                    error::InvalidArgumentsSnafu {
                        msg: "OSS configuration is set but --oss is not enabled.".to_string(),
                    }
                    .build(),
                ));
            }
        }

        Ok(())
    }

    pub fn validate_gcs(&self) -> Result<(), BoxedError> {
        let gcs = &self.gcs;

        if self.enable_gcs {
            // Check required fields (excluding credentials)
            let mut missing = Vec::new();
            if gcs.gcs_bucket.is_empty() {
                missing.push("bucket");
            }
            if gcs.gcs_root.is_empty() {
                missing.push("root");
            }
            if gcs.gcs_scope.is_empty() {
                missing.push("scope");
            }
            if gcs.gcs_endpoint.is_empty() {
                missing.push("endpoint");
            }

            // At least one of credential_path or credential must be provided (non-empty)
            if is_secret_empty(&gcs.gcs_credential_path) && is_secret_empty(&gcs.gcs_credential) {
                missing.push("credential path, credential");
            }

            if !missing.is_empty() {
                return Err(BoxedError::new(
                    error::MissingConfigSnafu {
                        msg: format!(
                            "GCS {} must be set when --gcs is enabled.",
                            missing.join(", "),
                        ),
                    }
                    .build(),
                ));
            }
        } else {
            // When disabled, check that no meaningful configuration is provided
            if !gcs.gcs_bucket.is_empty()
                || !gcs.gcs_root.is_empty()
                || !gcs.gcs_scope.is_empty()
                || !gcs.gcs_endpoint.is_empty()
                || is_secret_provided(&gcs.gcs_credential_path)
                || is_secret_provided(&gcs.gcs_credential)
            {
                return Err(BoxedError::new(
                    error::InvalidArgumentsSnafu {
                        msg: "GCS configuration is set but --gcs is not enabled.".to_string(),
                    }
                    .build(),
                ));
            }
        }

        Ok(())
    }

    pub fn validate_azblob(&self) -> Result<(), BoxedError> {
        let azblob = &self.azblob;

        if self.enable_azblob {
            // Check required fields
            let mut missing = Vec::new();
            if azblob.azblob_container.is_empty() {
                missing.push("container");
            }
            if azblob.azblob_root.is_empty() {
                missing.push("root");
            }
            if is_secret_empty(&azblob.azblob_account_name) {
                missing.push("account name");
            }
            if azblob.azblob_endpoint.is_empty() {
                missing.push("endpoint");
            }

            // account_key is only required if sas_token is not provided
            if azblob.azblob_sas_token.is_none() && is_secret_empty(&azblob.azblob_account_key) {
                missing.push("account key");
            }

            if !missing.is_empty() {
                return Err(BoxedError::new(
                    error::MissingConfigSnafu {
                        msg: format!(
                            "Azure Blob {} must be set when --azblob is enabled.",
                            missing.join(", "),
                        ),
                    }
                    .build(),
                ));
            }
        } else {
            // When disabled, check that no meaningful configuration is provided
            if !azblob.azblob_container.is_empty()
                || !azblob.azblob_root.is_empty()
                || !azblob.azblob_endpoint.is_empty()
                || azblob.azblob_sas_token.is_some()
                || is_secret_provided(&azblob.azblob_account_name)
                || is_secret_provided(&azblob.azblob_account_key)
            {
                return Err(BoxedError::new(
                    error::InvalidArgumentsSnafu {
                        msg: "Azure Blob configuration is set but --azblob is not enabled."
                            .to_string(),
                    }
                    .build(),
                ));
            }
        }

        Ok(())
    }

    pub fn validate(&self) -> Result<(), BoxedError> {
        self.validate_s3()?;
        self.validate_oss()?;
        self.validate_gcs()?;
        self.validate_azblob()?;
        Ok(())
    }

    /// Builds the object store from the config.
    pub fn build(&self) -> Result<Option<ObjectStore>, BoxedError> {
        self.validate()?;

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
