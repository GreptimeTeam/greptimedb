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

use std::any::Any;
use std::time::Duration;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use prost::Message;
use snafu::{Location, ResultExt, Snafu};

#[derive(Snafu)]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to create profiler guard"))]
    CreateGuard {
        #[snafu(source)]
        error: pprof::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create report"))]
    CreateReport {
        #[snafu(source)]
        error: pprof::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create flamegraph"))]
    CreateFlamegraph {
        #[snafu(source)]
        error: pprof::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create pprof report"))]
    ReportPprof {
        #[snafu(source)]
        error: pprof::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::Unexpected
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// CPU profiler utility.
// Inspired by https://github.com/datafuselabs/databend/blob/67f445e83cd4eceda98f6c1c114858929d564029/src/common/base/src/base/profiling.rs
#[derive(Debug)]
pub struct Profiling {
    /// Sample duration.
    duration: Duration,
    /// Sample frequency.
    frequency: i32,
}

impl Profiling {
    /// Creates a new profiler.
    pub fn new(duration: Duration, frequency: i32) -> Profiling {
        Profiling {
            duration,
            frequency,
        }
    }

    /// Profiles and returns a generated pprof report.
    pub async fn report(&self) -> Result<pprof::Report> {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(self.frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .context(CreateGuardSnafu)?;
        tokio::time::sleep(self.duration).await;
        guard.report().build().context(CreateReportSnafu)
    }

    /// Profiles and returns a generated flamegraph.
    pub async fn dump_flamegraph(&self) -> Result<Vec<u8>> {
        let mut body: Vec<u8> = Vec::new();

        let report = self.report().await?;
        report
            .flamegraph(&mut body)
            .context(CreateFlamegraphSnafu)?;

        Ok(body)
    }

    /// Profiles and returns a generated proto.
    pub async fn dump_proto(&self) -> Result<Vec<u8>> {
        let report = self.report().await?;
        // Generate google’s pprof format report.
        let profile = report.pprof().context(ReportPprofSnafu)?;
        let body = profile.encode_to_vec();

        Ok(body)
    }
}
