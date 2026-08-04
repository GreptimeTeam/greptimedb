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

use std::fmt::Debug;
use std::sync::Arc;

use derive_builder::Builder;
pub use oio::*;
pub use opendal::raw::{Layer, OpCopy, OpDelete, OpList, OpRead, OpWrite, Service, Servicer, oio};
pub use opendal::{Buffer, Error, ErrorKind, Metadata, OperationContext, Result};

pub type MockWriterFactory = Arc<dyn Fn(&str, OpWrite, oio::Writer) -> oio::Writer + Send + Sync>;
pub type MockReaderFactory = Arc<dyn Fn(&str, OpRead, oio::Reader) -> oio::Reader + Send + Sync>;
pub type MockListerFactory = Arc<dyn Fn(&str, OpList, oio::Lister) -> oio::Lister + Send + Sync>;
pub type MockDeleterFactory = Arc<dyn Fn(oio::Deleter) -> oio::Deleter + Send + Sync>;
pub type CopyInterceptor = Arc<dyn Fn(&str, &str, OpCopy) -> Option<Result<()>> + Send + Sync>;

#[derive(Builder)]
pub struct MockLayer {
    #[builder(setter(strip_option), default)]
    writer_factory: Option<MockWriterFactory>,
    #[builder(setter(strip_option), default)]
    reader_factory: Option<MockReaderFactory>,
    #[builder(setter(strip_option), default)]
    lister_factory: Option<MockListerFactory>,
    #[builder(setter(strip_option), default)]
    deleter_factory: Option<MockDeleterFactory>,
    #[builder(setter(strip_option), default)]
    copy_interceptor: Option<CopyInterceptor>,
}

impl Clone for MockLayer {
    fn clone(&self) -> Self {
        Self {
            writer_factory: self.writer_factory.clone(),
            reader_factory: self.reader_factory.clone(),
            lister_factory: self.lister_factory.clone(),
            deleter_factory: self.deleter_factory.clone(),
            copy_interceptor: self.copy_interceptor.clone(),
        }
    }
}

impl Debug for MockLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockLayer").finish_non_exhaustive()
    }
}

impl Layer for MockLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(MockService {
            inner,
            writer_factory: self.writer_factory.clone(),
            reader_factory: self.reader_factory.clone(),
            lister_factory: self.lister_factory.clone(),
            deleter_factory: self.deleter_factory.clone(),
            copy_interceptor: self.copy_interceptor.clone(),
        })
    }
}

struct MockService {
    inner: Servicer,
    writer_factory: Option<MockWriterFactory>,
    reader_factory: Option<MockReaderFactory>,
    lister_factory: Option<MockListerFactory>,
    deleter_factory: Option<MockDeleterFactory>,
    copy_interceptor: Option<CopyInterceptor>,
}

impl Debug for MockService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Service for MockService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> opendal::raw::ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> opendal::Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: opendal::raw::OpCreateDir,
    ) -> Result<opendal::raw::RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: opendal::raw::OpStat,
    ) -> Result<opendal::raw::RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let reader = self.inner.read(ctx, path, args.clone())?;
        if let Some(reader_factory) = self.reader_factory.as_ref() {
            Ok(reader_factory(path, args, reader))
        } else {
            Ok(reader)
        }
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let writer = self.inner.write(ctx, path, args.clone())?;
        if let Some(writer_factory) = self.writer_factory.as_ref() {
            Ok(writer_factory(path, args, writer))
        } else {
            Ok(writer)
        }
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let deleter = self.inner.delete(ctx)?;
        if let Some(deleter_factory) = self.deleter_factory.as_ref() {
            Ok(deleter_factory(deleter))
        } else {
            Ok(deleter)
        }
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let lister = self.inner.list(ctx, path, args.clone())?;
        if let Some(lister_factory) = self.lister_factory.as_ref() {
            Ok(lister_factory(path, args, lister))
        } else {
            Ok(lister)
        }
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: opendal::raw::OpCopier,
    ) -> Result<Self::Copier> {
        if let Some(result) = self
            .copy_interceptor
            .as_ref()
            .and_then(|copy_interceptor| copy_interceptor(from, to, args.clone()))
        {
            result?;
            return Ok(Box::new(oio::OneShotCopier::completed()) as oio::Copier);
        }

        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: opendal::raw::OpRename,
    ) -> Result<opendal::raw::RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: opendal::raw::OpPresign,
    ) -> Result<opendal::raw::RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

pub struct MockReader {
    inner: oio::Reader,
}

impl oio::Read for MockReader {
    async fn open(
        &self,
        range: opendal::BytesRange,
    ) -> Result<(opendal::raw::RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.inner.open(range).await
    }

    async fn read(&self, range: opendal::BytesRange) -> Result<(opendal::raw::RpRead, Buffer)> {
        self.inner.read(range).await
    }
}

pub struct MockWriter {
    inner: oio::Writer,
}

impl oio::Write for MockWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

pub struct MockLister {
    inner: oio::Lister,
}

impl oio::List for MockLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next().await
    }
}

pub struct MockDeleter {
    inner: oio::Deleter,
}

impl oio::Delete for MockDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}
