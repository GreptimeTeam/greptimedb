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

/// Trait for checking if a field is effectively empty.
///
/// **`is_empty()`**: Checks if the field has no meaningful value
/// - Used when backend is enabled to validate required fields
/// - `None`, `Some("")`, `false`, or `""` are considered empty
trait FieldValidator {
    /// Check if the field is empty (has no meaningful value).
    fn is_empty(&self) -> bool;
}

/// String fields: empty if the string is empty
impl FieldValidator for String {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

/// Bool fields: false is considered "empty", true is "provided"
impl FieldValidator for bool {
    fn is_empty(&self) -> bool {
        !self
    }
}

/// Option<String> fields: None or empty content is empty
impl FieldValidator for Option<String> {
    fn is_empty(&self) -> bool {
        self.as_ref().is_none_or(|s| s.is_empty())
    }
}

/// Option<SecretString> fields: None or empty secret is empty
/// For secrets, Some("") is treated as "not provided" for both checks
impl FieldValidator for Option<SecretString> {
    fn is_empty(&self) -> bool {
        self.as_ref().is_none_or(|s| s.expose_secret().is_empty())
    }
}

macro_rules! wrap_with_clap_prefix {
    (
        $new_name:ident, $prefix:literal, $enable_flag:literal, $base:ty, {
            $( $( #[doc = $doc:expr] )? $( #[alias = $alias:literal] )? $field:ident : $type:ty $( = $default:expr )? ),* $(,)?
        }
    ) => {
        paste!{
            #[derive(clap::Parser, Debug, Clone, PartialEq, Default)]
            pub struct $new_name {
                $(
                    $( #[doc = $doc] )?
                    $( #[clap(alias = $alias)] )?
                    #[clap(long, requires = $enable_flag $(, default_value_t = $default )? )]
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

/// Macro for declarative backend validation.
///
/// # Validation Rules
///
/// For each storage backend (S3, OSS, GCS, Azblob), this function validates:
/// **When backend is enabled** (e.g., `--s3`): All required fields must be non-empty
///
/// Note: When backend is disabled, clap's `requires` attribute ensures no configuration
/// fields can be provided at parse time.
///
/// # Syntax
///
/// ```ignore
/// validate_backend!(
///     enable: self.enable_s3,
///     name: "S3",
///     required: [(field1, "name1"), (field2, "name2"), ...],
///     custom_validator: |missing| { ... }  // optional
/// )
/// ```
///
/// # Arguments
///
/// - `enable`: Boolean expression indicating if backend is enabled
/// - `name`: Human-readable backend name for error messages
/// - `required`: Array of (field_ref, field_name) tuples for required fields
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
///     ]
/// )
/// ```
macro_rules! validate_backend {
    (
        enable: $enable:expr,
        name: $backend_name:expr,
        required: [ $( ($field:expr, $field_name:expr) ),* $(,)? ]
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
                            $backend_name.to_lowercase()
                        ),
                    }
                    .build(),
                ));
            }
        }

        Ok(())
    }};
}

wrap_with_clap_prefix! {
    PrefixedAzblobConnection,
    "azblob-",
    "enable_azblob",
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

impl PrefixedAzblobConnection {
    pub fn validate(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: true,
            name: "AzBlob",
            required: [
                (&self.azblob_container, "container"),
                (&self.azblob_root, "root"),
                (&self.azblob_account_name, "account name"),
                (&self.azblob_endpoint, "endpoint"),
            ],
            custom_validator: |missing: &mut Vec<&str>| {
                // account_key is only required if sas_token is not provided
                if self.azblob_sas_token.is_none()
                    && self.azblob_account_key.is_empty()
                {
                    missing.push("account key (when sas_token is not provided)");
                }
            }
        )
    }
}

wrap_with_clap_prefix! {
    PrefixedS3Connection,
    "s3-",
    "enable_s3",
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
        #[doc = "Disable EC2 metadata service for the object store."]
        disable_ec2_metadata: bool = Default::default(),
    }
}

impl PrefixedS3Connection {
    pub fn validate(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: true,
            name: "S3",
            required: [
                (&self.s3_bucket, "bucket"),
                (&self.s3_region, "region"),
            ]
        )
    }
}

wrap_with_clap_prefix! {
    PrefixedOssConnection,
    "oss-",
    "enable_oss",
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

impl PrefixedOssConnection {
    pub fn validate(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: true,
            name: "OSS",
            required: [
                (&self.oss_bucket, "bucket"),
                (&self.oss_access_key_id, "access key ID"),
                (&self.oss_access_key_secret, "access key secret"),
                (&self.oss_endpoint, "endpoint"),
            ]
        )
    }
}

wrap_with_clap_prefix! {
    PrefixedGcsConnection,
    "gcs-",
    "enable_gcs",
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

impl PrefixedGcsConnection {
    pub fn validate(&self) -> Result<(), BoxedError> {
        validate_backend!(
            enable: true,
            name: "GCS",
            required: [
                (&self.gcs_bucket, "bucket"),
                (&self.gcs_root, "root"),
                (&self.gcs_scope, "scope"),
            ]
            // No custom_validator needed: GCS supports Application Default Credentials (ADC)
            // where neither credential_path nor credential is required.
            // Endpoint is also optional (defaults to https://storage.googleapis.com).
        )
    }
}

/// Common config for object store.
///
/// # Dependency Enforcement
///
/// Each backend's configuration fields (e.g., `--s3-bucket`) requires its corresponding
/// enable flag (e.g., `--s3`) to be present. This is enforced by `clap` at parse time
/// using the `requires` attribute.
///
/// For example, attempting to use `--s3-bucket my-bucket` without `--s3` will result in:
/// ```text
/// error: The argument '--s3-bucket <BUCKET>' requires '--s3'
/// ```
///
/// This ensures that users cannot accidentally provide backend-specific configuration
/// without explicitly enabling that backend.
#[derive(clap::Parser, Debug, Clone, PartialEq, Default)]
#[clap(group(clap::ArgGroup::new("storage_backend").required(false).multiple(false)))]
pub struct ObjectStoreConfig {
    /// Whether to use S3 object store.
    #[clap(long = "s3", group = "storage_backend")]
    pub enable_s3: bool,

    #[clap(flatten)]
    pub s3: PrefixedS3Connection,

    /// Whether to use OSS.
    #[clap(long = "oss", group = "storage_backend")]
    pub enable_oss: bool,

    #[clap(flatten)]
    pub oss: PrefixedOssConnection,

    /// Whether to use GCS.
    #[clap(long = "gcs", group = "storage_backend")]
    pub enable_gcs: bool,

    #[clap(flatten)]
    pub gcs: PrefixedGcsConnection,

    /// Whether to use Azure Blob.
    #[clap(long = "azblob", group = "storage_backend")]
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

    pub fn validate(&self) -> Result<(), BoxedError> {
        if self.enable_s3 {
            self.s3.validate()?;
        }
        if self.enable_oss {
            self.oss.validate()?;
        }
        if self.enable_gcs {
            self.gcs.validate()?;
        }
        if self.enable_azblob {
            self.azblob.validate()?;
        }
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
