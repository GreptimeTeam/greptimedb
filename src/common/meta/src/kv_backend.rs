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

pub mod memory;

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use futures::{Stream, StreamExt};

use crate::error::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct Kv(pub Vec<u8>, pub Vec<u8>);

pub type ValueIter<'a, E> = Pin<Box<dyn Stream<Item = Result<Kv, E>> + Send + 'a>>;

pub type KvBackendRef = Arc<dyn KvBackend<Error = Error>>;

#[async_trait]
pub trait KvBackend: Send + Sync {
    type Error: ErrorExt;

    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Self::Error>
    where
        'a: 'b;

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Self::Error>;

    /// Compare and set value of key. `expect` is the expected value, if backend's current value associated
    /// with key is the same as `expect`, the value will be updated to `val`.
    ///
    /// - If the compare-and-set operation successfully updated value, this method will return an `Ok(Ok())`
    /// - If associated value is not the same as `expect`, no value will be updated and an `Ok(Err(Vec<u8>))`
    /// will be returned, the `Err(Vec<u8>)` indicates the current associated value of key.
    /// - If any error happens during operation, an `Err(Error)` will be returned.
    async fn compare_and_set(
        &self,
        key: &[u8],
        expect: &[u8],
        val: &[u8],
    ) -> Result<Result<(), Option<Vec<u8>>>, Self::Error>;

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Self::Error>;

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.delete_range(key, &[]).await
    }

    /// Default get is implemented based on `range` method.
    async fn get(&self, key: &[u8]) -> Result<Option<Kv>, Self::Error> {
        let mut iter = self.range(key);
        while let Some(r) = iter.next().await {
            let kv = r?;
            if kv.0 == key {
                return Ok(Some(kv));
            }
        }
        return Ok(None);
    }

    /// MoveValue atomically renames the key to the given updated key.
    async fn move_value(&self, from_key: &[u8], to_key: &[u8]) -> Result<(), Self::Error>;

    fn as_any(&self) -> &dyn Any;
}
