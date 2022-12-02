// Copyright 2022 Greptime Team
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

use crate::timestamp_millis::TimestampMillis;

/// A half-open time range.
///
/// The time range contains all timestamp `ts` that `ts >= start` and `ts < end`. It is
/// empty if `start == end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeRange<T> {
    start: T,
    end: T,
}

impl<T> TimeRange<T> {
    /// Creates a new range that contains timestamp in `[start, end)`.
    ///
    /// Returns `None` if `start` > `end`.
    pub fn new<U: PartialOrd + Into<T>>(start: U, end: U) -> Option<TimeRange<T>> {
        if start <= end {
            let (start, end) = (start.into(), end.into());
            Some(TimeRange { start, end })
        } else {
            None
        }
    }

    /// Given a value, creates an empty time range that `start == end == value`.
    pub fn empty_with_value<U: Clone + Into<T>>(value: U) -> TimeRange<T> {
        TimeRange {
            start: value.clone().into(),
            end: value.into(),
        }
    }

    /// Returns the lower bound of the range (inclusive).
    #[inline]
    pub fn start(&self) -> &T {
        &self.start
    }

    /// Returns the upper bound of the range (exclusive).
    #[inline]
    pub fn end(&self) -> &T {
        &self.end
    }

    /// Returns true if `timestamp` is contained in the range.
    pub fn contains<U: PartialOrd<T>>(&self, timestamp: &U) -> bool {
        *timestamp >= self.start && *timestamp < self.end
    }
}

impl<T: PartialOrd> TimeRange<T> {
    /// Returns true if the range contains no timestamps.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// Time range in milliseconds.
pub type RangeMillis = TimeRange<TimestampMillis>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_range() {
        let (start, end) = (TimestampMillis::new(0), TimestampMillis::new(100));
        let range = RangeMillis::new(start, end).unwrap();

        assert_eq!(start, *range.start());
        assert_eq!(end, *range.end());

        let range2 = RangeMillis::new(0, 100).unwrap();
        assert_eq!(range, range2);

        let range_eq = RangeMillis::new(123, 123).unwrap();
        assert_eq!(range_eq.start(), range_eq.end());

        assert_eq!(None, RangeMillis::new(1, 0));

        let range = RangeMillis::empty_with_value(1024);
        assert_eq!(range.start(), range.end());
        assert_eq!(1024, *range.start());
    }

    #[test]
    fn test_range_contains() {
        let range = RangeMillis::new(-10, 10).unwrap();
        assert!(!range.is_empty());
        assert!(range.contains(&-10));
        assert!(range.contains(&0));
        assert!(range.contains(&9));
        assert!(!range.contains(&10));

        let range = RangeMillis::new(i64::MIN, i64::MAX).unwrap();
        assert!(!range.is_empty());
        assert!(range.contains(&TimestampMillis::MIN));
        assert!(range.contains(&TimestampMillis::MAX));
        assert!(!range.contains(&TimestampMillis::INF));

        let range = RangeMillis::new(0, 0).unwrap();
        assert!(range.is_empty());
        assert!(!range.contains(&0));
    }
}
