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
pub use opendal::raw::{
    Access, Layer, LayeredAccess, OpDelete, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead,
    RpWrite, oio,
};
use opendal::raw::{OpCopy, RpCopy};
pub use opendal::{Buffer, Error, ErrorKind, Metadata, Result};

pub type MockWriterFactory = Arc<dyn Fn(&str, OpWrite, oio::Writer) -> oio::Writer + Send + Sync>;
pub type MockReaderFactory = Arc<dyn Fn(&str, OpRead, oio::Reader) -> oio::Reader + Send + Sync>;
pub type MockListerFactory = Arc<dyn Fn(&str, OpList, oio::Lister) -> oio::Lister + Send + Sync>;
pub type MockDeleterFactory = Arc<dyn Fn(oio::Deleter) -> oio::Deleter + Send + Sync>;
pub type CopyInterceptor = Arc<dyn Fn(&str, &str, OpCopy) -> Option<Result<RpCopy>> + Send + Sync>;

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

impl<A: Access> Layer<A> for MockLayer {
    type LayeredAccess = MockAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        MockAccessor {
            inner,
            writer_factory: self.writer_factory.clone(),
            reader_factory: self.reader_factory.clone(),
            lister_factory: self.lister_factory.clone(),
            deleter_factory: self.deleter_factory.clone(),
            copy_interceptor: self.copy_interceptor.clone(),
        }
    }
}

pub struct MockAccessor<A> {
    inner: A,
    writer_factory: Option<MockWriterFactory>,
    reader_factory: Option<MockReaderFactory>,
    lister_factory: Option<MockListerFactory>,
    deleter_factory: Option<MockDeleterFactory>,
    copy_interceptor: Option<CopyInterceptor>,
}

impl<A: Debug> Debug for MockAccessor<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockAccessor")
            .field("inner", &self.inner)
            .finish()
    }
}

pub struct MockReader {
    inner: oio::Reader,
}

impl oio::Read for MockReader {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
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
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        self.inner.flush().await
    }
}

impl<A: Access> LayeredAccess for MockAccessor<A> {
    type Inner = A;
    type Reader = MockReader;
    type Writer = MockWriter;
    type Lister = MockLister;
    type Deleter = MockDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        if let Some(reader_factory) = self.reader_factory.as_ref() {
            let (rp_read, reader) = self.inner.read(path, args.clone()).await?;
            let reader = reader_factory(path, args, Box::new(reader));
            Ok((rp_read, MockReader { inner: reader }))
        } else {
            self.inner.read(path, args).await.map(|(rp_read, reader)| {
                (
                    rp_read,
                    MockReader {
                        inner: Box::new(reader),
                    },
                )
            })
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if let Some(writer_factory) = self.writer_factory.as_ref() {
            let (rp_write, writer) = self.inner.write(path, args.clone()).await?;
            let writer = writer_factory(path, args, Box::new(writer));
            Ok((rp_write, MockWriter { inner: writer }))
        } else {
            self.inner
                .write(path, args)
                .await
                .map(|(rp_write, writer)| {
                    (
                        rp_write,
                        MockWriter {
                            inner: Box::new(writer),
                        },
                    )
                })
        }
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        if let Some(deleter_factory) = self.deleter_factory.as_ref() {
            let (rp_delete, deleter) = self.inner.delete().await?;
            let deleter = deleter_factory(Box::new(deleter));
            Ok((rp_delete, MockDeleter { inner: deleter }))
        } else {
            self.inner.delete().await.map(|(rp_delete, deleter)| {
                (
                    rp_delete,
                    MockDeleter {
                        inner: Box::new(deleter),
                    },
                )
            })
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        if let Some(lister_factory) = self.lister_factory.as_ref() {
            let (rp_list, lister) = self.inner.list(path, args.clone()).await?;
            let lister = lister_factory(path, args, Box::new(lister));
            Ok((rp_list, MockLister { inner: lister }))
        } else {
            self.inner.list(path, args).await.map(|(rp_list, lister)| {
                (
                    rp_list,
                    MockLister {
                        inner: Box::new(lister),
                    },
                )
            })
        }
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let Some(copy_interceptor) = self.copy_interceptor.as_ref() else {
            return self.inner.copy(from, to, args).await;
        };

        let Some(result) = copy_interceptor(from, to, args.clone()) else {
            return self.inner.copy(from, to, args).await;
        };

        result
    }
}
