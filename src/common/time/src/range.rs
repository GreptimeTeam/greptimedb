use crate::timestamp::TimestampMillis;

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
    /// Create a new range that contains timestamp in `[start, end)`.
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
