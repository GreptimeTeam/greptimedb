use std::cmp::Ordering;

/// Unix timestamp in millisecond resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
}

impl From<i64> for TimestampMillis {
    fn from(ms: i64) -> TimestampMillis {
        TimestampMillis::new(ms)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp() {
        let ts = 123456;
        let timestamp = TimestampMillis::from(ts);
        assert_eq!(timestamp, ts);
        assert_eq!(ts, timestamp);

        assert_ne!(TimestampMillis::new(0), timestamp);
        assert!(TimestampMillis::new(-123) < TimestampMillis::new(0));
        assert!(TimestampMillis::new(10) < 20);
        assert!(10 < TimestampMillis::new(20));

        assert_eq!(i64::MAX, TimestampMillis::INF);
        assert_eq!(i64::MAX - 1, TimestampMillis::MAX);
        assert_eq!(i64::MIN, TimestampMillis::MIN);
    }
}
