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

use crate::timestamp::TimeUnit;
use crate::timestamp_millis::TimestampMillis;
use crate::Timestamp;

/// A half-open time range.
///
/// The time range contains all timestamp `ts` that `ts >= start` and `ts < end`. It is
/// empty if `start >= end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GenericRange<T> {
    start: Option<T>,
    end: Option<T>,
}

impl<T> GenericRange<T>
where
    T: Copy + PartialOrd,
{
    pub fn and(&self, other: &GenericRange<T>) -> GenericRange<T> {
        let start = match (self.start(), other.start()) {
            (Some(l), Some(r)) => {
                if l > r {
                    Some(*l)
                } else {
                    Some(*r)
                }
            }
            (Some(l), None) => Some(*l),
            (None, Some(r)) => Some(*r),
            (None, None) => None,
        };

        let end = match (self.end(), other.end()) {
            (Some(l), Some(r)) => {
                if l > r {
                    Some(*r)
                } else {
                    Some(*l)
                }
            }
            (Some(l), None) => Some(*l),
            (None, Some(r)) => Some(*r),
            (None, None) => None,
        };

        Self { start, end }
    }

    /// Compute the OR'ed range of two ranges.
    /// Notice: this method does not compute the exact OR'ed operation for simplicity.
    /// For example, `[1, 2)` or'ed with `[4, 5)` will produce `[1, 5)`
    /// instead of `[1, 2) ∪ [4, 5)`
    pub fn or(&self, other: &GenericRange<T>) -> GenericRange<T> {
        if self.is_empty() {
            return *other;
        }

        if other.is_empty() {
            return *self;
        }
        let start = match (self.start(), other.start()) {
            (Some(l), Some(r)) => {
                if l > r {
                    Some(*r)
                } else {
                    Some(*l)
                }
            }
            (Some(_), None) => None,
            (None, Some(_)) => None,
            (None, None) => None,
        };

        let end = match (self.end(), other.end()) {
            (Some(l), Some(r)) => {
                if l > r {
                    Some(*l)
                } else {
                    Some(*r)
                }
            }
            (Some(_), None) => None,
            (None, Some(_)) => None,
            (None, None) => None,
        };

        Self { start, end }
    }
}

impl<T> GenericRange<T> {
    /// Creates a new range that contains timestamp in `[start, end)`.
    ///
    /// Returns `None` if `start` > `end`.
    pub fn new<U: PartialOrd + Into<T>>(start: U, end: U) -> Option<GenericRange<T>> {
        if start <= end {
            let (start, end) = (Some(start.into()), Some(end.into()));
            Some(GenericRange { start, end })
        } else {
            None
        }
    }

    /// Given a value, creates an empty time range that `start == end == value`.
    pub fn empty_with_value<U: Clone + Into<T>>(value: U) -> GenericRange<T> {
        GenericRange {
            start: Some(value.clone().into()),
            end: Some(value.into()),
        }
    }

    pub fn min_to_max() -> GenericRange<T> {
        Self {
            start: None,
            end: None,
        }
    }

    /// Returns the lower bound of the range (inclusive).
    #[inline]
    pub fn start(&self) -> &Option<T> {
        &self.start
    }

    /// Returns the upper bound of the range (exclusive).
    #[inline]
    pub fn end(&self) -> &Option<T> {
        &self.end
    }

    /// Returns true if `timestamp` is contained in the range.
    pub fn contains<U: PartialOrd<T>>(&self, timestamp: &U) -> bool {
        match (&self.start, &self.end) {
            (Some(start), Some(end)) => *timestamp >= *start && *timestamp < *end,
            (Some(start), None) => *timestamp >= *start,
            (None, Some(end)) => *timestamp < *end,
            (None, None) => true,
        }
    }
}

impl<T: PartialOrd> GenericRange<T> {
    /// Returns true if the range contains no timestamps.
    #[inline]
    pub fn is_empty(&self) -> bool {
        match (&self.start, &self.end) {
            (Some(start), Some(end)) => start >= end,
            _ => false,
        }
    }
}

pub type TimestampRange = GenericRange<Timestamp>;
impl TimestampRange {
    pub fn with_unit(start: i64, end: i64, unit: TimeUnit) -> Option<Self> {
        let start = Timestamp::new(start, unit);
        let end = Timestamp::new(end, unit);
        Self::new(start, end)
    }
}

/// Time range in milliseconds.
pub type RangeMillis = GenericRange<TimestampMillis>;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_new_range() {
        let (start, end) = (TimestampMillis::new(0), TimestampMillis::new(100));
        let range = RangeMillis::new(start, end).unwrap();

        assert_eq!(Some(start), *range.start());
        assert_eq!(Some(end), *range.end());

        let range2 = RangeMillis::new(0, 100).unwrap();
        assert_eq!(range, range2);

        let range_eq = RangeMillis::new(123, 123).unwrap();
        assert_eq!(range_eq.start(), range_eq.end());

        assert_eq!(None, RangeMillis::new(1, 0));

        let range = RangeMillis::empty_with_value(1024);
        assert_eq!(range.start(), range.end());
        assert_eq!(Some(TimestampMillis::new(1024)), *range.start());
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

    #[test]
    fn test_range_with_diff_unit() {
        let r1 = TimestampRange::with_unit(1, 2, TimeUnit::Second).unwrap();
        let r2 = TimestampRange::with_unit(1000, 2000, TimeUnit::Millisecond).unwrap();
        assert_eq!(r2, r1);

        let r3 = TimestampRange::with_unit(0, 0, TimeUnit::Second).unwrap();
        let r4 = TimestampRange::with_unit(0, 0, TimeUnit::Millisecond).unwrap();
        assert_eq!(r3, r4);

        let r5 = TimestampRange::with_unit(0, 0, TimeUnit::Millisecond).unwrap();
        let r6 = TimestampRange::with_unit(0, 0, TimeUnit::Microsecond).unwrap();
        assert_eq!(r5, r6);

        let r5 = TimestampRange::with_unit(-1, 1, TimeUnit::Second).unwrap();
        let r6 = TimestampRange::with_unit(-1000, 1000, TimeUnit::Millisecond).unwrap();
        assert_eq!(r5, r6);
    }

    #[test]
    fn test_range_and() {
        assert_eq!(
            TimestampRange::with_unit(5, 10, TimeUnit::Millisecond).unwrap(),
            TimestampRange::with_unit(1, 10, TimeUnit::Millisecond)
                .unwrap()
                .and(&TimestampRange::with_unit(5, 20, TimeUnit::Millisecond).unwrap())
        );

        let empty = TimestampRange::with_unit(0, 10, TimeUnit::Millisecond)
            .unwrap()
            .and(&TimestampRange::with_unit(11, 20, TimeUnit::Millisecond).unwrap());
        assert!(empty.is_empty());

        // whatever AND'ed with empty shall return empty
        let empty_and_all = empty.and(&TimestampRange::min_to_max());
        assert!(empty_and_all.is_empty());
        assert!(empty.and(&empty).is_empty());
        assert!(empty
            .and(&TimestampRange::with_unit(0, 10, TimeUnit::Millisecond).unwrap())
            .is_empty());

        // AND TimestampRange with different unit
        let anded = TimestampRange::with_unit(0, 10, TimeUnit::Millisecond)
            .unwrap()
            .and(&TimestampRange::with_unit(1000, 12000, TimeUnit::Microsecond).unwrap());
        assert_eq!(
            TimestampRange::with_unit(1, 10, TimeUnit::Millisecond).unwrap(),
            anded
        );

        let anded = TimestampRange::with_unit(0, 10, TimeUnit::Millisecond)
            .unwrap()
            .and(&TimestampRange::with_unit(1000, 12000, TimeUnit::Microsecond).unwrap());
        assert_eq!(
            TimestampRange::with_unit(1, 10, TimeUnit::Millisecond).unwrap(),
            anded
        );
    }

    #[test]
    fn test_range_or() {
        assert_eq!(
            TimestampRange::with_unit(1, 20, TimeUnit::Millisecond).unwrap(),
            TimestampRange::with_unit(1, 10, TimeUnit::Millisecond)
                .unwrap()
                .or(&TimestampRange::with_unit(5, 20, TimeUnit::Millisecond).unwrap())
        );

        assert_eq!(
            TimestampRange::min_to_max(),
            TimestampRange::min_to_max().or(&TimestampRange::min_to_max())
        );

        let empty = TimestampRange::empty_with_value(Timestamp::new_millisecond(1)).or(
            &TimestampRange::empty_with_value(Timestamp::new_millisecond(2)),
        );
        assert!(empty.is_empty());

        let t1 = TimestampRange::with_unit(-10, 0, TimeUnit::Second).unwrap();
        let t2 = TimestampRange::with_unit(-30, -20, TimeUnit::Second).unwrap();
        assert_eq!(
            TimestampRange::with_unit(-30, 0, TimeUnit::Second).unwrap(),
            t1.or(&t2)
        );

        let t1 = TimestampRange::with_unit(-10, 0, TimeUnit::Second).unwrap();
        assert_eq!(t1, t1.or(&t1));
    }
}
