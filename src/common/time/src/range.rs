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

use std::fmt::{Debug, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::timestamp::TimeUnit;
use crate::timestamp_millis::TimestampMillis;
use crate::Timestamp;

/// A half-open time range.
///
/// The range contains values that `value >= start` and `val < end`.
///
/// The range is empty iff `start == end == "the default value of T"`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GenericRange<T> {
    start: Option<T>,
    end: Option<T>,
}

impl<T> GenericRange<T>
where
    T: Copy + PartialOrd + Default,
{
    /// Computes the AND'ed range with other.  
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

        Self::from_optional(start, end)
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

        Self::from_optional(start, end)
    }

    /// Checks if current range intersect with target.
    pub fn intersects(&self, target: &GenericRange<T>) -> bool {
        !self.and(target).is_empty()
    }

    /// Create an empty range.
    pub fn empty() -> GenericRange<T> {
        GenericRange {
            start: Some(T::default()),
            end: Some(T::default()),
        }
    }

    /// Create GenericRange from optional start and end.
    /// If the present value of start >= the present value of end, it will return an empty range
    /// with the default value of `T`.
    fn from_optional(start: Option<T>, end: Option<T>) -> GenericRange<T> {
        match (start, end) {
            (Some(start_val), Some(end_val)) => {
                if start_val < end_val {
                    Self {
                        start: Some(start_val),
                        end: Some(end_val),
                    }
                } else {
                    Self::empty()
                }
            }
            (s, e) => Self { start: s, end: e },
        }
    }
}

impl<T> GenericRange<T> {
    /// Creates a new range that contains values in `[start, end)`.
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

    /// Return a range containing all possible values.
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
    pub fn contains<U: PartialOrd<T>>(&self, target: &U) -> bool {
        match (&self.start, &self.end) {
            (Some(start), Some(end)) => *target >= *start && *target < *end,
            (Some(start), None) => *target >= *start,
            (None, Some(end)) => *target < *end,
            (None, None) => true,
        }
    }
}

impl<T: PartialOrd> GenericRange<T> {
    /// Returns true if the range contains no timestamps.
    #[inline]
    pub fn is_empty(&self) -> bool {
        match (&self.start, &self.end) {
            (Some(start), Some(end)) => start == end,
            _ => false,
        }
    }
}

pub type TimestampRange = GenericRange<Timestamp>;

impl Display for TimestampRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match (&self.start, &self.end) {
            (Some(start), Some(end)) => {
                format!(
                    "TimestampRange{{[{}{},{}{})}}",
                    start.value(),
                    start.unit().short_name(),
                    end.value(),
                    end.unit().short_name()
                )
            }
            (Some(start), None) => {
                format!(
                    "TimestampRange{{[{}{},#)}}",
                    start.value(),
                    start.unit().short_name()
                )
            }
            (None, Some(end)) => {
                format!(
                    "TimestampRange{{[#,{}{})}}",
                    end.value(),
                    end.unit().short_name()
                )
            }
            (None, None) => "TimestampRange{{[#,#)}}".to_string(),
        };
        f.write_str(&s)
    }
}

impl TimestampRange {
    /// Create a TimestampRange with optional inclusive end timestamp.
    /// If end timestamp is present and is less than start timestamp, this method will return
    /// an empty range.
    /// ### Caveat
    /// If the given end timestamp's value is `i64::MAX`, which will result into overflow when added
    /// by 1(the end is inclusive), this method does not try to convert the time unit of end
    /// timestamp, instead it just return `[start, INF)`. This exaggerates the range but does not
    /// affect correctness.  
    pub fn new_inclusive(start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        // check for emptiness
        if let (Some(start_ts), Some(end_ts)) = (start, end) {
            if start_ts > end_ts {
                return Self::empty();
            }
        }

        let end = if let Some(end) = end {
            end.value()
                .checked_add(1)
                .map(|v| Timestamp::new(v, end.unit()))
        } else {
            None
        };
        Self::from_optional(start, end)
    }

    /// Shortcut method to create a timestamp range with given start/end value and time unit.
    /// Returns empty iff `start` > `end`.
    pub fn with_unit(start: i64, end: i64, unit: TimeUnit) -> Option<Self> {
        let start = Timestamp::new(start, unit);
        let end = Timestamp::new(end, unit);
        Self::new(start, end)
    }

    /// Create a range that containing only given `ts`.
    /// ### Notice:
    /// Left-close right-open range cannot properly represent range with a single value.
    /// For simplicity, this implementation returns an approximate range `[ts, ts+1)` instead.
    pub fn single(ts: Timestamp) -> Self {
        let unit = ts.unit();
        let start = Some(ts);
        let end = ts.value().checked_add(1).map(|v| Timestamp::new(v, unit));

        Self::from_optional(start, end)
    }

    /// Create a range `[start, INF)`.
    /// ### Notice
    /// Left-close right-open range cannot properly represent range with exclusive start like: `(start, ...)`.
    /// You may resort to `[start-1, ...)` instead.
    pub fn from_start(start: Timestamp) -> Self {
        Self {
            start: Some(start),
            end: None,
        }
    }

    /// Create a range `[-INF, end)`.
    /// ### Notice
    /// Left-close right-open range cannot properly represent range with inclusive end like: `[..., END]`.
    /// If `inclusive` is true, this method returns `[-INF, end+1)` instead.
    pub fn until_end(end: Timestamp, inclusive: bool) -> Self {
        let end = if inclusive {
            end.value()
                .checked_add(1)
                .map(|v| Timestamp::new(v, end.unit()))
        } else {
            Some(end)
        };
        Self { start: None, end }
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

        let range = RangeMillis::empty();
        assert_eq!(range.start(), range.end());
        assert_eq!(Some(TimestampMillis::new(0)), *range.start());
    }

    #[test]
    fn test_timestamp_range_new_inclusive() {
        let range = TimestampRange::new_inclusive(
            Some(Timestamp::new(i64::MAX - 1, TimeUnit::Second)),
            Some(Timestamp::new(i64::MAX, TimeUnit::Millisecond)),
        );
        assert!(range.is_empty());

        let range = TimestampRange::new_inclusive(
            Some(Timestamp::new(1, TimeUnit::Second)),
            Some(Timestamp::new(1, TimeUnit::Millisecond)),
        );
        assert!(range.is_empty());

        let range = TimestampRange::new_inclusive(
            Some(Timestamp::new(1, TimeUnit::Second)),
            Some(Timestamp::new(i64::MAX, TimeUnit::Millisecond)),
        );
        assert!(range.end.is_none());
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

        let empty = TimestampRange::empty().or(&TimestampRange::empty());
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

    #[test]
    fn test_intersect() {
        let t1 = TimestampRange::with_unit(-10, 0, TimeUnit::Second).unwrap();
        let t2 = TimestampRange::with_unit(-30, -20, TimeUnit::Second).unwrap();
        assert!(!t1.intersects(&t2));

        let t1 = TimestampRange::with_unit(10, 20, TimeUnit::Second).unwrap();
        let t2 = TimestampRange::with_unit(0, 30, TimeUnit::Second).unwrap();
        assert!(t1.intersects(&t2));

        let t1 = TimestampRange::with_unit(-20, -10, TimeUnit::Second).unwrap();
        let t2 = TimestampRange::with_unit(-10, 0, TimeUnit::Second).unwrap();
        assert!(!t1.intersects(&t2));

        let t1 = TimestampRange::with_unit(0, 1, TimeUnit::Second).unwrap();
        let t2 = TimestampRange::with_unit(999, 1000, TimeUnit::Millisecond).unwrap();
        assert!(t1.intersects(&t2));

        let t1 = TimestampRange::with_unit(1, 2, TimeUnit::Second).unwrap();
        let t2 = TimestampRange::with_unit(1000, 2000, TimeUnit::Millisecond).unwrap();
        assert!(t1.intersects(&t2));

        let t1 = TimestampRange::with_unit(0, 1, TimeUnit::Second).unwrap();
        assert!(t1.intersects(&t1));

        let t1 = TimestampRange::with_unit(0, 1, TimeUnit::Second).unwrap();
        let t2 = TimestampRange::empty();
        assert!(!t1.intersects(&t2));

        // empty range does not intersect with empty range
        let empty = TimestampRange::empty();
        assert!(!empty.intersects(&empty));

        // full range intersects with full range
        let full = TimestampRange::min_to_max();
        assert!(full.intersects(&full));

        assert!(!full.intersects(&empty));
    }

    #[test]
    fn test_new_inclusive() {
        let range = TimestampRange::new_inclusive(
            Some(Timestamp::new_millisecond(1)),
            Some(Timestamp::new_millisecond(3)),
        );
        assert!(!range.is_empty());
        assert!(range.contains(&Timestamp::new_millisecond(1)));
        assert!(range.contains(&Timestamp::new_millisecond(3)));

        let range = TimestampRange::new_inclusive(
            Some(Timestamp::new_millisecond(1)),
            Some(Timestamp::new_millisecond(1)),
        );
        assert!(!range.is_empty());
        assert_eq!(1, range.start.unwrap().value());
        assert!(range.contains(&Timestamp::new_millisecond(1)));

        let range = TimestampRange::new_inclusive(
            Some(Timestamp::new_millisecond(2)),
            Some(Timestamp::new_millisecond(1)),
        );
        assert!(range.is_empty());
    }

    #[test]
    fn test_serialize_timestamp_range() {
        macro_rules! test_serde_for_unit {
            ($($unit: expr),*) => {
                $(
                let original_range = TimestampRange::with_unit(0, 10, $unit).unwrap();
                let string = serde_json::to_string(&original_range).unwrap();
                let deserialized: TimestampRange = serde_json::from_str(&string).unwrap();
                assert_eq!(original_range, deserialized);
                )*
            };
        }

        test_serde_for_unit!(
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond
        );
    }
}
