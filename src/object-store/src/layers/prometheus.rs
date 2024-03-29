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

//! code originally from <https://github.com/apache/incubator-opendal/blob/main/core/src/layers/prometheus.rs>, make a tiny change to avoid crash in multi thread env

use std::fmt::{Debug, Formatter};
use std::io;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use common_telemetry::debug;
use futures::{FutureExt, TryFutureExt};
use lazy_static::lazy_static;
use opendal::raw::*;
use opendal::ErrorKind;
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_vec, register_int_counter_vec,
    HistogramVec, IntCounterVec,
};

type Result<T> = std::result::Result<T, opendal::Error>;

lazy_static! {
    static ref REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "opendal_requests_total",
        "Total times of all kinds of operation being called",
        &["scheme", "operation"],
    )
    .unwrap();
    static ref REQUESTS_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        histogram_opts!(
            "opendal_requests_duration_seconds",
            "Histogram of the time spent on specific operation",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        ),
        &["scheme", "operation"]
    )
    .unwrap();
    static ref BYTES_TOTAL: HistogramVec = register_histogram_vec!(
        histogram_opts!(
            "opendal_bytes_total",
            "Total size of sync or async Read/Write",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        ),
        &["scheme", "operation"]
    )
    .unwrap();
}

#[inline]
fn increment_errors_total(op: Operation, kind: ErrorKind) {
    debug!(
        "Prometheus statistics metrics error, operation {} error {}",
        op.into_static(),
        kind.into_static()
    );
}

/// Please refer to [prometheus](https://docs.rs/prometheus) for every operations.
///
/// # Prometheus Metrics
///
/// In this section, we will introduce three metrics that are currently being exported by opendal. These metrics are essential for understanding the behavior and performance of opendal.
///
///
/// | Metric Name                       | Type      | Description                                          | Labels              |
/// |-----------------------------------|-----------|------------------------------------------------------|---------------------|
/// | opendal_requests_total            | Counter   | Total times of all kinds of operation being called   | scheme, operation   |
/// | opendal_requests_duration_seconds | Histogram | Histogram of the time spent on specific operation    | scheme, operation   |
/// | opendal_bytes_total               | Histogram | Total size of sync or async Read/Write               | scheme, operation   |
///
/// For a more detailed explanation of these metrics and how they are used, please refer to the [Prometheus documentation](https://prometheus.io/docs/introduction/overview/).
///
/// # Histogram Configuration
///
/// The metric buckets for these histograms are automatically generated based on the `exponential_buckets(0.01, 2.0, 16)` configuration.
#[derive(Default, Debug, Clone)]
pub struct PrometheusMetricsLayer;

impl<A: Accessor> Layer<A> for PrometheusMetricsLayer {
    type LayeredAccessor = PrometheusAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();
        let scheme = meta.scheme();

        PrometheusAccessor {
            inner,
            scheme: scheme.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct PrometheusAccessor<A: Accessor> {
    inner: A,
    scheme: String,
}

impl<A: Accessor> Debug for PrometheusAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for PrometheusAccessor<A> {
    type Inner = A;
    type Reader = PrometheusMetricWrapper<A::Reader>;
    type BlockingReader = PrometheusMetricWrapper<A::BlockingReader>;
    type Writer = PrometheusMetricWrapper<A::Writer>;
    type BlockingWriter = PrometheusMetricWrapper<A::BlockingWriter>;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::CreateDir.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::CreateDir.into_static()])
            .start_timer();
        let create_res = self.inner.create_dir(path, args).await;

        timer.observe_duration();
        create_res.map_err(|e| {
            increment_errors_total(Operation::CreateDir, e.kind());
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::Read.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::Read.into_static()])
            .start_timer();

        let read_res = self
            .inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        PrometheusMetricWrapper::new(r, Operation::Read, &self.scheme),
                    )
                })
            })
            .await;
        timer.observe_duration();
        read_res.map_err(|e| {
            increment_errors_total(Operation::Read, e.kind());
            e
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::Write.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::Write.into_static()])
            .start_timer();

        let write_res = self
            .inner
            .write(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        PrometheusMetricWrapper::new(r, Operation::Write, &self.scheme),
                    )
                })
            })
            .await;
        timer.observe_duration();
        write_res.map_err(|e| {
            increment_errors_total(Operation::Write, e.kind());
            e
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::Stat.into_static()])
            .inc();
        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::Stat.into_static()])
            .start_timer();

        let stat_res = self
            .inner
            .stat(path, args)
            .inspect_err(|e| {
                increment_errors_total(Operation::Stat, e.kind());
            })
            .await;
        timer.observe_duration();
        stat_res.map_err(|e| {
            increment_errors_total(Operation::Stat, e.kind());
            e
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::Delete.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::Delete.into_static()])
            .start_timer();

        let delete_res = self.inner.delete(path, args).await;
        timer.observe_duration();
        delete_res.map_err(|e| {
            increment_errors_total(Operation::Delete, e.kind());
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::List.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::List.into_static()])
            .start_timer();

        let list_res = self.inner.list(path, args).await;

        timer.observe_duration();
        list_res.map_err(|e| {
            increment_errors_total(Operation::List, e.kind());
            e
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::Batch.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::Batch.into_static()])
            .start_timer();
        let result = self.inner.batch(args).await;

        timer.observe_duration();
        result.map_err(|e| {
            increment_errors_total(Operation::Batch, e.kind());
            e
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::Presign.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::Presign.into_static()])
            .start_timer();
        let result = self.inner.presign(path, args).await;
        timer.observe_duration();

        result.map_err(|e| {
            increment_errors_total(Operation::Presign, e.kind());
            e
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::BlockingCreateDir.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::BlockingCreateDir.into_static()])
            .start_timer();
        let result = self.inner.blocking_create_dir(path, args);

        timer.observe_duration();

        result.map_err(|e| {
            increment_errors_total(Operation::BlockingCreateDir, e.kind());
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::BlockingRead.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::BlockingRead.into_static()])
            .start_timer();
        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(r, Operation::BlockingRead, &self.scheme),
            )
        });
        timer.observe_duration();
        result.map_err(|e| {
            increment_errors_total(Operation::BlockingRead, e.kind());
            e
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::BlockingWrite.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::BlockingWrite.into_static()])
            .start_timer();
        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(r, Operation::BlockingWrite, &self.scheme),
            )
        });
        timer.observe_duration();
        result.map_err(|e| {
            increment_errors_total(Operation::BlockingWrite, e.kind());
            e
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::BlockingStat.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::BlockingStat.into_static()])
            .start_timer();
        let result = self.inner.blocking_stat(path, args);
        timer.observe_duration();
        result.map_err(|e| {
            increment_errors_total(Operation::BlockingStat, e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::BlockingDelete.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::BlockingDelete.into_static()])
            .start_timer();
        let result = self.inner.blocking_delete(path, args);
        timer.observe_duration();

        result.map_err(|e| {
            increment_errors_total(Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::BlockingList.into_static()])
            .inc();

        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::BlockingList.into_static()])
            .start_timer();
        let result = self.inner.blocking_list(path, args);
        timer.observe_duration();

        result.map_err(|e| {
            increment_errors_total(Operation::BlockingList, e.kind());
            e
        })
    }
}

pub struct PrometheusMetricWrapper<R> {
    inner: R,

    op: Operation,
    scheme: String,
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(inner: R, op: Operation, scheme: &String) -> Self {
        Self {
            inner,
            op,
            scheme: scheme.to_string(),
        }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.inner.poll_read(cx, buf).map(|res| match res {
            Ok(bytes) => {
                BYTES_TOTAL
                    .with_label_values(&[&self.scheme, Operation::Read.into_static()])
                    .observe(bytes as f64);
                Ok(bytes)
            }
            Err(e) => {
                increment_errors_total(self.op, e.kind());
                Err(e)
            }
        })
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        self.inner.poll_seek(cx, pos).map(|res| match res {
            Ok(n) => Ok(n),
            Err(e) => {
                increment_errors_total(self.op, e.kind());
                Err(e)
            }
        })
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.inner.poll_next(cx).map(|res| match res {
            Some(Ok(bytes)) => {
                BYTES_TOTAL
                    .with_label_values(&[&self.scheme, Operation::Read.into_static()])
                    .observe(bytes.len() as f64);
                Some(Ok(bytes))
            }
            Some(Err(e)) => {
                increment_errors_total(self.op, e.kind());
                Some(Err(e))
            }
            None => None,
        })
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for PrometheusMetricWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner
            .read(buf)
            .map(|n| {
                BYTES_TOTAL
                    .with_label_values(&[&self.scheme, Operation::BlockingRead.into_static()])
                    .observe(n as f64);
                n
            })
            .map_err(|e| {
                increment_errors_total(self.op, e.kind());
                e
            })
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos).map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next().map(|res| match res {
            Ok(bytes) => {
                BYTES_TOTAL
                    .with_label_values(&[&self.scheme, Operation::BlockingRead.into_static()])
                    .observe(bytes.len() as f64);
                Ok(bytes)
            }
            Err(e) => {
                increment_errors_total(self.op, e.kind());
                Err(e)
            }
        })
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for PrometheusMetricWrapper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        self.inner
            .poll_write(cx, bs)
            .map_ok(|n| {
                BYTES_TOTAL
                    .with_label_values(&[&self.scheme, Operation::Write.into_static()])
                    .observe(n as f64);
                n
            })
            .map_err(|err| {
                increment_errors_total(self.op, err.kind());
                err
            })
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_abort(cx).map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_close(cx).map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        self.inner
            .write(bs)
            .map(|n| {
                BYTES_TOTAL
                    .with_label_values(&[&self.scheme, Operation::BlockingWrite.into_static()])
                    .observe(n as f64);
                n
            })
            .map_err(|err| {
                increment_errors_total(self.op, err.kind());
                err
            })
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close().map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }
}
