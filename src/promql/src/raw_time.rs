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

//! The unit of the timestamps a raw source hands over, and how a sample stored in it is placed on
//! the millisecond timeline PromQL evaluates on.
//!
//! A raw source hands its time index over as stored. The existing PromQL plan decides which
//! samples a window or a lookback holds on those stored ticks, and only the timestamp a kernel reads
//! is converted to milliseconds, truncated toward zero after the offset is applied
//! (`RangeManipulate`, and `timestamp()` through an Arrow cast). Two samples less than a millisecond
//! apart therefore stay two samples, sharing one millisecond.
//!
//! Every bound the evaluation compares against is a whole millisecond, so a sample at `s` lies at
//! or before the bound `t` exactly when `ceil(s) <= t`, and after it exactly when `ceil(s) > t`.
//! The ceiling is the sample's *window* time: comparing it reproduces the stored-tick decision.
//! The truncated time is its *payload*, the time a kernel reads. They differ only for a positive
//! sample that does not fall on a whole millisecond.

use datafusion::arrow::datatypes::TimeUnit;
use datafusion::error::{DataFusionError, Result};

const NANOS_PER_MILLI: i128 = 1_000_000;

/// The unit of a raw input's timestamps: its time index's own.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum RawTimeUnit {
    Second,
    #[default]
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<TimeUnit> for RawTimeUnit {
    fn from(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

impl RawTimeUnit {
    fn nanos_per_tick(self) -> i128 {
        match self {
            Self::Second => 1_000_000_000,
            Self::Millisecond => 1_000_000,
            Self::Microsecond => 1_000,
            Self::Nanosecond => 1,
        }
    }

    /// Whether every tick is a whole millisecond, so a sample's window time and payload agree.
    pub fn is_whole_millis(self) -> bool {
        matches!(self, Self::Second | Self::Millisecond)
    }

    /// The window time of `tick` shifted by `offset_ms`: the millisecond the sample is compared at.
    pub fn window_ms(self, tick: i64, offset_ms: i64) -> Result<i64> {
        let nanos = i128::from(tick) * self.nanos_per_tick();
        let ceil = -(-nanos).div_euclid(NANOS_PER_MILLI);
        narrow(ceil + i128::from(offset_ms), tick, offset_ms)
    }

    /// The payload of `tick` shifted by `offset_ms`: the millisecond a kernel reads.
    pub fn payload_ms(self, tick: i64, offset_ms: i64) -> Result<i64> {
        let nanos =
            i128::from(tick) * self.nanos_per_tick() + i128::from(offset_ms) * NANOS_PER_MILLI;
        narrow(nanos / NANOS_PER_MILLI, tick, offset_ms)
    }

    /// The payloads of `ticks` shifted by `offset_ms` into `payloads`, and, when the unit is finer
    /// than a millisecond, their window times into `windows`. Both are cleared first.
    pub fn convert(
        self,
        ticks: &[i64],
        offset_ms: i64,
        payloads: &mut Vec<i64>,
        windows: &mut Vec<i64>,
    ) -> Result<()> {
        payloads.clear();
        windows.clear();
        payloads.reserve(ticks.len());
        for &tick in ticks {
            payloads.push(self.payload_ms(tick, offset_ms)?);
        }
        if !self.is_whole_millis() {
            windows.reserve(ticks.len());
            for &tick in ticks {
                windows.push(self.window_ms(tick, offset_ms)?);
            }
        }
        Ok(())
    }
}

fn narrow(millis: i128, tick: i64, offset_ms: i64) -> Result<i64> {
    i64::try_from(millis).map_err(|_| {
        DataFusionError::Execution(format!(
            "sample timestamp {tick} overflows with offset {offset_ms} ms"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_time_rounds_up_and_payload_truncates_toward_zero() {
        let us = RawTimeUnit::Microsecond;
        // 1s + 1us: after the 1s bound, read as 1000ms.
        assert_eq!(us.window_ms(1_000_001, 0).unwrap(), 1001);
        assert_eq!(us.payload_ms(1_000_001, 0).unwrap(), 1000);
        // A negative sub-millisecond time truncates toward zero, as `RangeManipulate` does.
        assert_eq!(us.window_ms(-298_999_999, 0).unwrap(), -298_999);
        assert_eq!(us.payload_ms(-298_999_999, 0).unwrap(), -298_999);
        // The payload is truncated after the offset is applied, so a shift across zero moves it.
        assert_eq!(us.payload_ms(1_500, -2).unwrap(), 0);
        assert_eq!(us.window_ms(1_500, -2).unwrap(), 0);
        let s = RawTimeUnit::Second;
        assert_eq!(s.window_ms(-3, 500).unwrap(), -2500);
        assert_eq!(s.payload_ms(-3, 500).unwrap(), -2500);
        assert!(s.payload_ms(i64::MAX, 0).is_err());
    }
}
