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

use std::ops::Deref;

use snafu::OptionExt;

use crate::error::{self, Result};

/// The delimiter of key.
pub const DELIMITER: u8 = b'/';

/// The key of metadata.
pub trait MetaKey<T> {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Result<T>;
}

#[derive(Debug, PartialEq)]
pub struct CatalogScoped<T> {
    inner: T,
    catalog: String,
}

impl<T> Deref for CatalogScoped<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> CatalogScoped<T> {
    /// Returns a new [CatalogScoped] key.
    pub fn new(catalog: String, inner: T) -> CatalogScoped<T> {
        CatalogScoped { inner, catalog }
    }

    /// Returns the `catalog`.
    pub fn catalog(&self) -> &str {
        &self.catalog
    }
}

impl<T: MetaKey<T>> MetaKey<CatalogScoped<T>> for CatalogScoped<T> {
    fn to_bytes(&self) -> Vec<u8> {
        let prefix = self.catalog.as_bytes();
        let inner = self.inner.to_bytes();
        let mut bytes = Vec::with_capacity(prefix.len() + inner.len() + 1);
        bytes.extend(prefix);
        bytes.push(DELIMITER);
        bytes.extend(inner);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CatalogScoped<T>> {
        let pos = bytes
            .iter()
            .position(|c| *c == DELIMITER)
            .with_context(|| error::DelimiterNotFoundSnafu {
                key: String::from_utf8_lossy(bytes),
            })?;
        let catalog = String::from_utf8_lossy(&bytes[0..pos]).to_string();
        // Safety: We don't need the `DELIMITER` char.
        let inner = T::from_bytes(&bytes[pos + 1..])?;
        Ok(CatalogScoped { inner, catalog })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BytesAdapter(Vec<u8>);

impl From<Vec<u8>> for BytesAdapter {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl MetaKey<BytesAdapter> for BytesAdapter {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn from_bytes(bytes: &[u8]) -> Result<BytesAdapter> {
        Ok(BytesAdapter(bytes.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::error::Result;

    #[derive(Debug)]
    struct MockKey {
        inner: Vec<u8>,
    }

    impl MetaKey<MockKey> for MockKey {
        fn to_bytes(&self) -> Vec<u8> {
            self.inner.clone()
        }

        fn from_bytes(bytes: &[u8]) -> Result<MockKey> {
            Ok(MockKey {
                inner: bytes.to_vec(),
            })
        }
    }

    #[test]
    fn test_catalog_scoped_from_bytes() {
        let key = "test_catalog_name/key";
        let scoped_key = CatalogScoped::<MockKey>::from_bytes(key.as_bytes()).unwrap();
        assert_eq!(scoped_key.catalog, "test_catalog_name");
        assert_eq!(scoped_key.inner.inner, b"key".to_vec());
        assert_eq!(key.as_bytes(), &scoped_key.to_bytes());
    }

    #[test]
    fn test_catalog_scoped_from_bytes_delimiter_not_found() {
        let key = "test_catalog_name";
        let err = CatalogScoped::<MockKey>::from_bytes(key.as_bytes()).unwrap_err();
        assert_matches!(err, error::Error::DelimiterNotFound { .. });
    }

    #[test]
    fn test_catalog_scoped_to_bytes() {
        let scoped_key = CatalogScoped {
            inner: MockKey {
                inner: b"hi".to_vec(),
            },
            catalog: "test_catalog".to_string(),
        };
        assert_eq!(b"test_catalog/hi".to_vec(), scoped_key.to_bytes());
    }
}
