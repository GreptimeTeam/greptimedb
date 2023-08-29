// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Error, InvalidTableMetadataSnafu, Result};
use crate::key::{TableMetaKey, SCHEMA_NAME_KEY_PATTERN, SCHEMA_NAME_KEY_PREFIX};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::{PutRequest, RangeRequest};
use crate::rpc::KeyValue;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchemaNameKey<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
}

impl<'a> Default for SchemaNameKey<'a> {
    fn default() -> Self {
        Self {
            catalog: DEFAULT_CATALOG_NAME,
            schema: DEFAULT_SCHEMA_NAME,
        }
    }
}

// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// pub struct SchemaNameValue {
//     #[serde(default)]
//     #[serde(with = "humantime_serde")]
//     pub ttl: Option<Duration>,
// }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaNameValue;

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

impl TableMetaKey for SchemaNameKey<'_> {
    fn as_raw_key(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

/// Decodes `KeyValue` to ({schema},())
pub fn schema_decoder(kv: KeyValue) -> Result<(String, ())> {
    let str = std::str::from_utf8(&kv.key).context(error::ConvertRawKeySnafu)?;
    let schema_name = SchemaNameKey::try_from(str)?;

    Ok((schema_name.schema.to_string(), ()))
}

impl<'a> TryFrom<&'a str> for SchemaNameKey<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        let captures = SCHEMA_NAME_KEY_PATTERN
            .captures(s)
            .context(InvalidTableMetadataSnafu {
                err_msg: format!("Illegal SchemaNameKey format: '{s}'"),
            })?;

        // Safety: pass the regex check above
        Ok(Self {
            catalog: captures.get(1).unwrap().as_str(),
            schema: captures.get(2).unwrap().as_str(),
        })
    }
}

pub struct SchemaManager {
    kv_backend: KvBackendRef,
}

impl SchemaManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Creates `SchemaNameKey`.
    pub async fn create(&self, schema: SchemaNameKey<'_>) -> Result<()> {
        let raw_key = schema.as_raw_key();
        let req = PutRequest::new()
            .with_key(raw_key)
            .with_value(SchemaNameValue.try_as_raw_value()?);

        self.kv_backend.put(req).await?;

        Ok(())
    }

    pub async fn exist(&self, schema: SchemaNameKey<'_>) -> Result<bool> {
        let raw_key = schema.as_raw_key();

        Ok(self.kv_backend.get(&raw_key).await?.is_some())
    }

    /// Returns a schema stream, it lists all schemas belong to the target `catalog`.
    pub async fn schema_names(&self, catalog: &str) -> BoxStream<'static, Result<String>> {
        let start_key = SchemaNameKey::range_start_key(catalog);
        let req = RangeRequest::new().with_prefix(start_key.as_bytes());

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(schema_decoder),
        );

        Box::pin(stream.map(|kv| kv.map(|kv| kv.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[test]
    fn test_serialization() {
        let key = SchemaNameKey::new("my-catalog", "my-schema");

        assert_eq!(key.to_string(), "__schema_name/my-catalog/my-schema");

        let parsed: SchemaNameKey<'_> = "__schema_name/my-catalog/my-schema".try_into().unwrap();

        assert_eq!(key, parsed);
    }

    #[tokio::test]
    async fn test_key_exist() {
        let manager = SchemaManager::new(Arc::new(MemoryKvBackend::default()));
        let schema_key = SchemaNameKey::new("my-catalog", "my-schema");
        manager.create(schema_key).await.unwrap();

        assert!(manager.exist(schema_key).await.unwrap());

        let wrong_schema_key = SchemaNameKey::new("my-catalog", "my-wrong");

        assert!(!manager.exist(wrong_schema_key).await.unwrap());
    }
}
