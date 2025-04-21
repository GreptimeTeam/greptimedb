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

use std::collections::HashMap;
use std::fmt::Display;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_time::DatabaseTimeToLive;
use futures::stream::BoxStream;
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::ensure_values;
use crate::error::{self, Error, InvalidMetadataSnafu, ParseOptionSnafu, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    DeserializedValueWithBytes, MetadataKey, SCHEMA_NAME_KEY_PATTERN, SCHEMA_NAME_KEY_PREFIX,
};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;

const OPT_KEY_TTL: &str = "ttl";

/// The schema name key, indices all schema names belong to the {catalog_name}
///
/// The layout:  `__schema_name/{catalog_name}/{schema_name}`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchemaNameKey<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
}

impl Default for SchemaNameKey<'_> {
    fn default() -> Self {
        Self {
            catalog: DEFAULT_CATALOG_NAME,
            schema: DEFAULT_SCHEMA_NAME,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaNameValue {
    #[serde(default)]
    pub ttl: Option<DatabaseTimeToLive>,
}

impl Display for SchemaNameValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ttl) = self.ttl.map(|i| i.to_string()) {
            write!(f, "ttl='{}'", ttl)?;
        }

        Ok(())
    }
}

impl TryFrom<&HashMap<String, String>> for SchemaNameValue {
    type Error = Error;

    fn try_from(value: &HashMap<String, String>) -> std::result::Result<Self, Self::Error> {
        let ttl = value
            .get(OPT_KEY_TTL)
            .map(|ttl_str| {
                ttl_str.parse::<humantime::Duration>().map_err(|_| {
                    ParseOptionSnafu {
                        key: OPT_KEY_TTL,
                        value: ttl_str.clone(),
                    }
                    .build()
                })
            })
            .transpose()?
            .map(|ttl| ttl.into());
        Ok(Self { ttl })
    }
}

impl From<SchemaNameValue> for HashMap<String, String> {
    fn from(value: SchemaNameValue) -> Self {
        let mut opts = HashMap::new();
        if let Some(ttl) = value.ttl.map(|ttl| ttl.to_string()) {
            opts.insert(OPT_KEY_TTL.to_string(), ttl);
        }
        opts
    }
}

impl<'a> SchemaNameKey<'a> {
    pub fn new(catalog: &'a str, schema: &'a str) -> Self {
        Self { catalog, schema }
    }

    pub fn range_start_key(catalog: &str) -> String {
        format!("{}/{}/", SCHEMA_NAME_KEY_PREFIX, catalog)
    }
}

impl Display for SchemaNameKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            SCHEMA_NAME_KEY_PREFIX, self.catalog, self.schema
        )
    }
}

impl<'a> MetadataKey<'a, SchemaNameKey<'a>> for SchemaNameKey<'_> {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<SchemaNameKey<'a>> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "SchemaNameKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        SchemaNameKey::try_from(key)
    }
}

/// Decodes `KeyValue` to {schema}
pub fn schema_decoder(kv: KeyValue) -> Result<String> {
    let str = std::str::from_utf8(&kv.key).context(error::ConvertRawKeySnafu)?;
    let schema_name = SchemaNameKey::try_from(str)?;

    Ok(schema_name.schema.to_string())
}

impl<'a> TryFrom<&'a str> for SchemaNameKey<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        let captures = SCHEMA_NAME_KEY_PATTERN
            .captures(s)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Illegal SchemaNameKey format: '{s}'"),
            })?;

        // Safety: pass the regex check above
        Ok(Self {
            catalog: captures.get(1).unwrap().as_str(),
            schema: captures.get(2).unwrap().as_str(),
        })
    }
}

#[derive(Clone)]
pub struct SchemaManager {
    kv_backend: KvBackendRef,
}

pub type SchemaNameDecodeResult = Result<Option<DeserializedValueWithBytes<SchemaNameValue>>>;

impl SchemaManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Creates `SchemaNameKey`.
    pub async fn create(
        &self,
        schema: SchemaNameKey<'_>,
        value: Option<SchemaNameValue>,
        if_not_exists: bool,
    ) -> Result<()> {
        let _timer = crate::metrics::METRIC_META_CREATE_SCHEMA.start_timer();

        let raw_key = schema.to_bytes();
        let raw_value = value.unwrap_or_default().try_as_raw_value()?;
        if self
            .kv_backend
            .put_conditionally(raw_key, raw_value, if_not_exists)
            .await?
        {
            crate::metrics::METRIC_META_CREATE_SCHEMA_COUNTER.inc();
        }

        Ok(())
    }

    pub async fn exists(&self, schema: SchemaNameKey<'_>) -> Result<bool> {
        let raw_key = schema.to_bytes();

        self.kv_backend.exists(&raw_key).await
    }

    pub async fn get(
        &self,
        schema: SchemaNameKey<'_>,
    ) -> Result<Option<DeserializedValueWithBytes<SchemaNameValue>>> {
        let raw_key = schema.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| DeserializedValueWithBytes::from_inner_slice(&x.value))
            .transpose()
    }

    /// Deletes a [SchemaNameKey].
    pub async fn delete(&self, schema: SchemaNameKey<'_>) -> Result<()> {
        let raw_key = schema.to_bytes();
        self.kv_backend.delete(&raw_key, false).await?;

        Ok(())
    }

    pub(crate) fn build_update_txn(
        &self,
        schema: SchemaNameKey<'_>,
        current_schema_value: &DeserializedValueWithBytes<SchemaNameValue>,
        new_schema_value: &SchemaNameValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> SchemaNameDecodeResult,
    )> {
        let raw_key = schema.to_bytes();
        let raw_value = current_schema_value.get_raw_bytes();
        let new_raw_value: Vec<u8> = new_schema_value.try_as_raw_value()?;

        let txn = Txn::compare_and_put(raw_key.clone(), raw_value, new_raw_value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Updates a [SchemaNameKey].
    pub async fn update(
        &self,
        schema: SchemaNameKey<'_>,
        current_schema_value: &DeserializedValueWithBytes<SchemaNameValue>,
        new_schema_value: &SchemaNameValue,
    ) -> Result<()> {
        let (txn, on_failure) =
            self.build_update_txn(schema, current_schema_value, new_schema_value)?;
        let mut r = self.kv_backend.txn(txn).await?;

        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_schema_value = on_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg:
                        "Reads the empty schema name value in comparing operation of updating schema name value",
                })?
                .into_inner();

            let op_name = "the updating schema name value";
            ensure_values!(&remote_schema_value, new_schema_value, op_name);
        }

        Ok(())
    }

    /// Returns a schema stream, it lists all schemas belong to the target `catalog`.
    pub fn schema_names(&self, catalog: &str) -> BoxStream<'static, Result<String>> {
        let start_key = SchemaNameKey::range_start_key(catalog);
        let req = RangeRequest::new().with_prefix(start_key.as_bytes());

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            schema_decoder,
        )
        .into_stream();

        Box::pin(stream)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct SchemaName {
    pub catalog_name: String,
    pub schema_name: String,
}

impl<'a> From<&'a SchemaName> for SchemaNameKey<'a> {
    fn from(value: &'a SchemaName) -> Self {
        Self {
            catalog: &value.catalog_name,
            schema: &value.schema_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[test]
    fn test_display_schema_value() {
        let schema_value = SchemaNameValue { ttl: None };
        assert_eq!("", schema_value.to_string());

        let schema_value = SchemaNameValue {
            ttl: Some(Duration::from_secs(9).into()),
        };
        assert_eq!("ttl='9s'", schema_value.to_string());

        let schema_value = SchemaNameValue {
            ttl: Some(Duration::from_secs(0).into()),
        };
        assert_eq!("ttl='forever'", schema_value.to_string());
    }

    #[test]
    fn test_serialization() {
        let key = SchemaNameKey::new("my-catalog", "my-schema");
        assert_eq!(key.to_string(), "__schema_name/my-catalog/my-schema");

        let parsed = SchemaNameKey::from_bytes(b"__schema_name/my-catalog/my-schema").unwrap();

        assert_eq!(key, parsed);

        let value = SchemaNameValue {
            ttl: Some(Duration::from_secs(10).into()),
        };
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("ttl".to_string(), "10s".to_string());
        let from_value = SchemaNameValue::try_from(&opts).unwrap();
        assert_eq!(value, from_value);

        let parsed = SchemaNameValue::try_from_raw_value(
            serde_json::json!({"ttl": "10s"}).to_string().as_bytes(),
        )
        .unwrap();
        assert_eq!(Some(value), parsed);

        let forever = SchemaNameValue {
            ttl: Some(Default::default()),
        };
        let parsed = SchemaNameValue::try_from_raw_value(
            serde_json::json!({"ttl": "forever"}).to_string().as_bytes(),
        )
        .unwrap();
        assert_eq!(Some(forever), parsed);

        let instant_err = SchemaNameValue::try_from_raw_value(
            serde_json::json!({"ttl": "instant"}).to_string().as_bytes(),
        );
        assert!(instant_err.is_err());

        let none = SchemaNameValue::try_from_raw_value("null".as_bytes()).unwrap();
        assert!(none.is_none());

        let err_empty = SchemaNameValue::try_from_raw_value("".as_bytes());
        assert!(err_empty.is_err());
    }

    #[tokio::test]
    async fn test_key_exist() {
        let manager = SchemaManager::new(Arc::new(MemoryKvBackend::default()));
        let schema_key = SchemaNameKey::new("my-catalog", "my-schema");
        manager.create(schema_key, None, false).await.unwrap();

        assert!(manager.exists(schema_key).await.unwrap());

        let wrong_schema_key = SchemaNameKey::new("my-catalog", "my-wrong");

        assert!(!manager.exists(wrong_schema_key).await.unwrap());
    }

    #[tokio::test]
    async fn test_update_schema_value() {
        let manager = SchemaManager::new(Arc::new(MemoryKvBackend::default()));
        let schema_key = SchemaNameKey::new("my-catalog", "my-schema");
        manager.create(schema_key, None, false).await.unwrap();

        let current_schema_value = manager.get(schema_key).await.unwrap().unwrap();
        let new_schema_value = SchemaNameValue {
            ttl: Some(Duration::from_secs(10).into()),
        };
        manager
            .update(schema_key, &current_schema_value, &new_schema_value)
            .await
            .unwrap();

        // Update with the same value, should be ok
        manager
            .update(schema_key, &current_schema_value, &new_schema_value)
            .await
            .unwrap();

        let new_schema_value = SchemaNameValue {
            ttl: Some(Duration::from_secs(40).into()),
        };
        let incorrect_schema_value = SchemaNameValue {
            ttl: Some(Duration::from_secs(20).into()),
        }
        .try_as_raw_value()
        .unwrap();
        let incorrect_schema_value =
            DeserializedValueWithBytes::from_inner_slice(&incorrect_schema_value).unwrap();

        manager
            .update(schema_key, &incorrect_schema_value, &new_schema_value)
            .await
            .unwrap_err();

        let current_schema_value = manager.get(schema_key).await.unwrap().unwrap();
        let new_schema_value = SchemaNameValue { ttl: None };
        manager
            .update(schema_key, &current_schema_value, &new_schema_value)
            .await
            .unwrap();

        let current_schema_value = manager.get(schema_key).await.unwrap().unwrap();
        assert_eq!(new_schema_value, *current_schema_value);
    }
}
