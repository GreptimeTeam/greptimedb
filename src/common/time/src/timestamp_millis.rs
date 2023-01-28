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

use std::cmp::Ordering;

/// Unix timestamp in millisecond resolution.
///
/// Negative timestamp is allowed, which represents timestamp before '1970-01-01T00:00:00'.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TimestampMillis(i64);

impl TimestampMillis {
    /// Positive infinity.
    pub const INF: TimestampMillis = TimestampMillis::new(i64::MAX);
    /// Maximum value of a timestamp.
    ///
    /// The maximum value of i64 is reserved for infinity.
    pub const MAX: TimestampMillis = TimestampMillis::new(i64::MAX - 1);
    /// Minimum value of a timestamp.
    pub const MIN: TimestampMillis = TimestampMillis::new(i64::MIN);

    /// Create a new timestamp from unix timestamp in milliseconds.
    pub const fn new(ms: i64) -> TimestampMillis {
        TimestampMillis(ms)
    }

    /// Returns the timestamp value as i64.
    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

impl From<i64> for TimestampMillis {
    fn from(ms: i64) -> TimestampMillis {
        TimestampMillis::new(ms)
    }
}

impl From<TimestampMillis> for i64 {
    fn from(ts: TimestampMillis) -> Self {
        ts.0
    }
}

impl PartialEq<i64> for TimestampMillis {
    fn eq(&self, other: &i64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<TimestampMillis> for i64 {
    fn eq(&self, other: &TimestampMillis) -> bool {
        *self == other.0
    }
}

impl PartialOrd<i64> for TimestampMillis {
    fn partial_cmp(&self, other: &i64) -> Option<Ordering> {
        Some(self.0.cmp(other))
    }
}

impl PartialOrd<TimestampMillis> for i64 {
    fn partial_cmp(&self, other: &TimestampMillis) -> Option<Ordering> {
        Some(self.cmp(&other.0))
    }
}

pub trait BucketAligned {
    /// Returns the timestamp aligned by `bucket_duration` in milliseconds or
    /// `None` if overflow occurred.
    ///
    /// # Panics
    /// Panics if `bucket_duration <= 0`.
    fn align_by_bucket(self, bucket_duration: i64) -> Option<TimestampMillis>;
}

impl<T: Into<i64>> BucketAligned for T {
    fn align_by_bucket(self, bucket_duration: i64) -> Option<TimestampMillis> {
        assert!(bucket_duration > 0, "{}", bucket_duration);
        self.into()
            .checked_div_euclid(bucket_duration)
            .and_then(|val| val.checked_mul(bucket_duration))
            .map(TimestampMillis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp() {
        let ts = 123456;
        let timestamp = TimestampMillis::from(ts);
        assert_eq!(timestamp, ts);
        assert_eq!(ts, timestamp);
        assert_eq!(ts, timestamp.as_i64());

        assert_ne!(TimestampMillis::new(0), timestamp);
        assert!(TimestampMillis::new(-123) < TimestampMillis::new(0));
        assert!(TimestampMillis::new(10) < 20);
        assert!(10 < TimestampMillis::new(20));

        assert_eq!(i64::MAX, TimestampMillis::INF);
        assert_eq!(i64::MAX - 1, TimestampMillis::MAX);
        assert_eq!(i64::MIN, TimestampMillis::MIN);
    }

    #[test]
    fn test_align_by_bucket() {
        let bucket = 100;
        assert_eq!(0, TimestampMillis::new(0).align_by_bucket(bucket).unwrap());
        assert_eq!(0, TimestampMillis::new(1).align_by_bucket(bucket).unwrap());
        assert_eq!(0, TimestampMillis::new(99).align_by_bucket(bucket).unwrap());
        assert_eq!(
            100,
            TimestampMillis::new(100).align_by_bucket(bucket).unwrap()
        );
        assert_eq!(
            100,
            TimestampMillis::new(199).align_by_bucket(bucket).unwrap()
        );

        assert_eq!(0, TimestampMillis::MAX.align_by_bucket(i64::MAX).unwrap());
        assert_eq!(
            i64::MAX,
            TimestampMillis::INF.align_by_bucket(i64::MAX).unwrap()
        );

        assert_eq!(None, TimestampMillis::MIN.align_by_bucket(bucket));
    }
}
