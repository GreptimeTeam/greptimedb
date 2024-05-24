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

use async_trait::async_trait;
use common_telemetry::debug;
use lazy_static::lazy_static;
use opendal::raw::*;
use opendal::{Buffer, ErrorKind};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_vec, register_int_counter_vec,
    Histogram, HistogramTimer, HistogramVec, IntCounterVec,
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

impl<A: Access> Layer<A> for PrometheusMetricsLayer {
    type LayeredAccess = PrometheusAccess<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let meta = inner.info();
        let scheme = meta.scheme();

        PrometheusAccess {
            inner,
            scheme: scheme.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct PrometheusAccess<A: Access> {
    inner: A,
    scheme: String,
}

impl<A: Access> Debug for PrometheusAccess<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<A: Access> LayeredAccess for PrometheusAccess<A> {
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

        self.inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        PrometheusMetricWrapper::new(
                            r,
                            Operation::Read,
                            BYTES_TOTAL
                                .with_label_values(&[&self.scheme, Operation::Read.into_static()]),
                            timer,
                        ),
                    )
                })
            })
            .await
            .map_err(|e| {
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

        let (rp, r) = self.inner.write(path, args).await.map_err(|e| {
            increment_errors_total(Operation::Write, e.kind());
            e
        })?;

        Ok((
            rp,
            PrometheusMetricWrapper::new(
                r,
                Operation::Write,
                BYTES_TOTAL
                    .with_label_values(&[&self.scheme, Operation::Write.into_static()]),
                timer,
            ),
        ))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        REQUESTS_TOTAL
            .with_label_values(&[&self.scheme, Operation::Stat.into_static()])
            .inc();
        let timer = REQUESTS_DURATION_SECONDS
            .with_label_values(&[&self.scheme, Operation::Stat.into_static()])
            .start_timer();

        let stat_res = self.inner.stat(path, args).await;
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

        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| {
                (
                    rp,
                    PrometheusMetricWrapper::new(
                        r,
                        Operation::BlockingRead,
                        BYTES_TOTAL.with_label_values(&[
                            &self.scheme,
                            Operation::BlockingRead.into_static(),
                        ]),
                        timer,
                    ),
                )
            })
            .map_err(|e| {
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

        self.inner
            .blocking_write(path, args)
            .map(|(rp, r)| {
                (
                    rp,
                    PrometheusMetricWrapper::new(
                        r,
                        Operation::BlockingWrite,
                        BYTES_TOTAL.with_label_values(&[
                            &self.scheme,
                            Operation::BlockingWrite.into_static(),
                        ]),
                        timer,
                    ),
                )
            })
            .map_err(|e| {
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
    bytes_counter: Histogram,
    _requests_duration_timer: HistogramTimer,
    bytes: u64,
}

impl<R> Drop for PrometheusMetricWrapper<R> {
    fn drop(&mut self) {
        self.bytes_counter.observe(self.bytes as f64);
    }
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(
        inner: R,
        op: Operation,
        bytes_counter: Histogram,
        requests_duration_timer: HistogramTimer,
    ) -> Self {
        Self {
            inner,
            op,
            bytes_counter,
            _requests_duration_timer: requests_duration_timer,
            bytes: 0,
        }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<Buffer> {
        self.inner.read_at(offset, limit).await.map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for PrometheusMetricWrapper<R> {
    fn read_at(&self, offset: u64, limit: usize) -> opendal::Result<Buffer> {
        self.inner.read_at(offset, limit).map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }
}

impl<R: oio::Write> oio::Write for PrometheusMetricWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<usize> {
        match self.inner.write(bs).await {
            Ok(n) => {
                self.bytes += n as u64;
                Ok(n)
            }
            Err(err) => {
                increment_errors_total(self.op, err.kind());
                Err(err)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await.map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.close().await.map_err(|err| {
            increment_errors_total(self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: Buffer) -> Result<usize> {
        self.inner
            .write(bs)
            .map(|n| {
                self.bytes += n as u64;
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
