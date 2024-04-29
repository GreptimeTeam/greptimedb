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

use crate::error::Result;

/// The key of metadata.
pub trait MetaKey<T> {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Result<T>;
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
}
