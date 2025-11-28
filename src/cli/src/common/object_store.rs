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

/// Trait for checking if a field is effectively empty or provided.
///
/// This trait provides a unified interface for validation across different field types.
/// It defines two key semantic checks:
///
/// 1. **`is_empty()`**: Checks if the field has no meaningful value
///    - Used when backend is enabled to validate required fields
///    - `None`, `Some("")`, `false`, or `""` are considered empty
///
/// 2. **`is_provided()`**: Checks if user explicitly provided the field
///    - Used when backend is disabled to detect configuration errors
///    - Different semantics for different types (see implementations)
trait FieldValidator {
    /// Check if the field is empty (has no meaningful value).
    fn is_empty(&self) -> bool;

    /// Check if the field was explicitly provided by the user.
    fn is_provided(&self) -> bool;
}

/// String fields: empty if the string is empty
impl FieldValidator for String {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_provided(&self) -> bool {
        !self.is_empty()
    }
}

/// Bool fields: false is considered "empty", true is "provided"
impl FieldValidator for bool {
    fn is_empty(&self) -> bool {
        !self
    }

    fn is_provided(&self) -> bool {
        *self
    }
}

/// Option<String> fields: None or empty content is empty
/// IMPORTANT: is_provided() returns true for Some(_) regardless of content,
/// to detect when user provided the argument (even if value is empty)
impl FieldValidator for Option<String> {
    fn is_empty(&self) -> bool {
        self.as_ref().is_none_or(|s| s.is_empty())
    }

    fn is_provided(&self) -> bool {
        // Key difference: Some("") is considered "provided" (user typed the flag)
        // even though is_empty() would return true for it
        self.is_some()
    }
}

/// Option<SecretString> fields: None or empty secret is empty
/// For secrets, Some("") is treated as "not provided" for both checks
impl FieldValidator for Option<SecretString> {
    fn is_empty(&self) -> bool {
        self.as_ref().is_none_or(|s| s.expose_secret().is_empty())
    }

    fn is_provided(&self) -> bool {
        // For secrets, empty string is equivalent to None
        !self.is_empty()
    }
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
    config.validate()?;
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

/// Macro for declarative backend validation.
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
/// This macro generates validation logic for storage backends with two main checks:
/// 1. When enabled: verify all required fields are non-empty
/// 2. When disabled: verify no configuration fields are provided
///
/// # Syntax
///
/// ```ignore
/// validate_backend!(
///     enable: self.enable_s3,
///     name: "S3",
///     required: [field1, field2, ...],
///     optional: [field3, field4, ...],
///     custom_validator: |missing| { ... }  // optional
/// )
/// ```
///
/// # Arguments
///
/// - `enable`: Boolean expression indicating if backend is enabled
/// - `name`: Human-readable backend name for error messages
/// - `required`: Array of (field_ref, field_name) tuples for required fields
/// - `optional`: Array of field references that are optional (only checked when disabled)
/// - `custom_validator`: Optional closure for complex validation logic
///
/// # Example
///
/// ```ignore
/// validate_backend!(
///     enable: self.enable_s3,
///     name: "S3",
///     required: [
///         (&self.s3.s3_bucket, "bucket"),
///         (&self.s3.s3_access_key_id, "access key ID"),
///     ],
///     optional: [
///         &self.s3.s3_root,
///         &self.s3.s3_endpoint,
///     ]
/// )
/// ```
macro_rules! validate_backend {
    (
        enable: $enable:expr,
        name: $backend_name:expr,
        required: [ $( ($field:expr, $field_name:expr) ),* $(,)? ],
        optional: [ $( $opt_field:expr ),* $(,)? ]
        $(, custom_validator: $custom_validator:expr)?
    ) => {{
        if $enable {
            // Check required fields when backend is enabled
            let mut missing = Vec::new();
            $(
                if FieldValidator::is_empty($field) {
                    missing.push($field_name);
                }
            )*

            // Run custom validation if provided
            $(
                $custom_validator(&mut missing);
            )?

            if !missing.is_empty() {
                return Err(BoxedError::new(
                    error::MissingConfigSnafu {
                        msg: format!(
                            "{} {} must be set when --{} is enabled.",
                            $backend_name,
                            missing.join(", "),
                            $backend_name.to_lowercase().replace(" ", "")
                        ),
                    }
                    .build(),
                ));
            }
        } else {
            // Check that no configuration is provided when backend is disabled
            #[allow(unused_assignments)]
            let mut has_config = false;
            $(
                has_config = has_config || FieldValidator::is_provided($field);
            )*
            $(
                has_config = has_config || FieldValidator::is_provided($opt_field);
            )*

            if has_config {
                return Err(BoxedError::new(
                    error::InvalidArgumentsSnafu {
                        msg: format!(
                            "{} configuration is set but --{} is not enabled.",
                            $backend_name,
                            $backend_name.to_lowercase().replace(" ", "")
                        ),
                    }
                    .build(),
                ));
            }
        }

        Ok(())
    }};
}

impl ObjectStoreConfig {
    gen_object_store_builder!(build_s3, s3, S3Connection, S3);

    gen_object_store_builder!(build_oss, oss, OssConnection, Oss);

    gen_object_store_builder!(build_gcs, gcs, GcsConnection, Gcs);

    gen_object_store_builder!(build_azblob, azblob, AzblobConnection, Azblob);

    pub fn validate_s3(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: self.enable_s3,
            name: "S3",
            required: [
                (&self.s3.s3_bucket, "bucket"),
                (&self.s3.s3_access_key_id, "access key ID"),
                (&self.s3.s3_secret_access_key, "secret access key"),
            ],
            optional: [
                &self.s3.s3_root,
                &self.s3.s3_endpoint,
                &self.s3.s3_region,
                &self.s3.s3_enable_virtual_host_style
            ]
        )
    }

    pub fn validate_oss(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: self.enable_oss,
            name: "OSS",
            required: [
                (&self.oss.oss_bucket, "bucket"),
                (&self.oss.oss_root, "root"),
                (&self.oss.oss_access_key_id, "access key ID"),
                (&self.oss.oss_access_key_secret, "access key secret"),
                (&self.oss.oss_endpoint, "endpoint"),
            ],
            optional: []
        )
    }

    pub fn validate_gcs(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: self.enable_gcs,
            name: "GCS",
            required: [
                (&self.gcs.gcs_bucket, "bucket"),
                (&self.gcs.gcs_root, "root"),
                (&self.gcs.gcs_scope, "scope"),
            ],
            optional: [
                &self.gcs.gcs_endpoint,
                &self.gcs.gcs_credential_path,
                &self.gcs.gcs_credential
            ]
            // No custom_validator needed: GCS supports Application Default Credentials (ADC)
            // where neither credential_path nor credential is required.
            // Endpoint is also optional (defaults to https://storage.googleapis.com).
        )
    }

    pub fn validate_azblob(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: self.enable_azblob,
            name: "Azure Blob",
            required: [
                (&self.azblob.azblob_container, "container"),
                (&self.azblob.azblob_root, "root"),
                (&self.azblob.azblob_account_name, "account name"),
                (&self.azblob.azblob_endpoint, "endpoint"),
            ],
            optional: [
                &self.azblob.azblob_account_key,
                &self.azblob.azblob_sas_token
            ],
            custom_validator: |missing: &mut Vec<&str>| {
                // account_key is only required if sas_token is not provided
                if self.azblob.azblob_sas_token.is_none()
                    && self.azblob.azblob_account_key.is_empty()
                {
                    missing.push("account key (when sas_token is not provided)");
                }
            }
        )
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
