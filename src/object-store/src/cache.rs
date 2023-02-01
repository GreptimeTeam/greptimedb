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

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use opendal::layers::CachePolicy;
use opendal::raw::output::Reader;
use opendal::raw::{Accessor, RpRead};
use opendal::{ErrorKind, OpRead, OpWrite, Result};

#[derive(Debug, Default)]
pub struct ObjectStoreCachePolicy {}

impl ObjectStoreCachePolicy {
    fn cache_path(&self, path: &str, args: &OpRead) -> String {
        format!("{}.cache-{}", path, args.range().to_header())
    }
}

#[async_trait]
impl CachePolicy for ObjectStoreCachePolicy {
    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> BoxFuture<'static, Result<(RpRead, Reader)>> {
        let path = path.to_string();
        let cache_path = self.cache_path(&path, &args);
        Box::pin(async move {
            match cache.read(&cache_path, OpRead::default()).await {
                Ok(v) => Ok(v),
                Err(err) if err.kind() == ErrorKind::ObjectNotFound => {
                    let (rp, reader) = inner.read(&path, args.clone()).await?;
                    let size = rp.clone().into_metadata().content_length();
                    let _ = cache
                        .write(&cache_path, OpWrite::new(size), Box::new(reader))
                        .await?;
                    match cache.read(&cache_path, OpRead::default()).await {
                        Ok(v) => Ok(v),
                        Err(_) => return inner.read(&path, args).await,
                    }
                }
                Err(_) => return inner.read(&path, args).await,
            }
        })
    }
}
