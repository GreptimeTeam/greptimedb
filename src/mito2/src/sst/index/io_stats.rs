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

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncSeek, AsyncWrite};
use pin_project::pin_project;
use prometheus::IntCounter;

struct BytesRecorder {
    bytes: usize,
    recorder: &'static IntCounter,
}

impl BytesRecorder {
    fn new(recorder: &'static IntCounter) -> Self {
        Self { bytes: 0, recorder }
    }

    fn inc_by(&mut self, bytes: usize) {
        self.bytes += bytes;
    }
}

impl Drop for BytesRecorder {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.recorder.inc_by(self.bytes as _);
        }
    }
}

#[pin_project]
pub(crate) struct InstrumentedAsyncRead<R> {
    #[pin]
    inner: R,
    recorder: BytesRecorder,
}

impl<R> InstrumentedAsyncRead<R> {
    pub(crate) fn new(inner: R, recorder: &'static IntCounter) -> Self {
        Self {
            inner,
            recorder: BytesRecorder::new(recorder),
        }
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncRead for InstrumentedAsyncRead<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let poll = self.as_mut().project().inner.poll_read(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            self.recorder.inc_by(*n);
        }
        poll
    }
}

impl<R: AsyncSeek + Unpin + Send> AsyncSeek for InstrumentedAsyncRead<R> {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        self.project().inner.poll_seek(cx, pos)
    }
}

#[pin_project]
pub(crate) struct InstrumentedAsyncWrite<W> {
    #[pin]
    inner: W,
    recorder: BytesRecorder,
}

impl<W> InstrumentedAsyncWrite<W> {
    pub(crate) fn new(inner: W, recorder: &'static IntCounter) -> Self {
        Self {
            inner,
            recorder: BytesRecorder::new(recorder),
        }
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncWrite for InstrumentedAsyncWrite<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = self.as_mut().project().inner.poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            self.recorder.inc_by(*n);
        }
        poll
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_close(cx)
    }
}
