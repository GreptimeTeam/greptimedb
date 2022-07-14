use std::cmp::Ordering;

/// Unix timestamp in millisecond resolution.
///
/// Negative timestamp is allowed, which represents timestamp before '1970-01-01T00:00:00'.
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

    /// Returns the timestamp aligned by `bucket_duration` in milliseconds or
    /// `None` if overflow occurred.
    ///
    /// # Panics
    /// Panics if `bucket_duration <= 0`.
    pub fn aligned_by_bucket(self, bucket_duration: i64) -> Option<TimestampMillis> {
        assert!(bucket_duration > 0);

        let ts = if self.0 >= 0 {
            self.0
        } else {
            // `bucket_duration > 0` implies `bucket_duration - 1` won't overflow.
            self.0.checked_sub(bucket_duration - 1)?
        };

        Some(TimestampMillis(ts / bucket_duration * bucket_duration))
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
    fn test_aligned_by_bucket() {
        let bucket = 100;
        assert_eq!(
            0,
            TimestampMillis::new(0).aligned_by_bucket(bucket).unwrap()
        );
        assert_eq!(
            0,
            TimestampMillis::new(1).aligned_by_bucket(bucket).unwrap()
        );
        assert_eq!(
            0,
            TimestampMillis::new(99).aligned_by_bucket(bucket).unwrap()
        );
        assert_eq!(
            100,
            TimestampMillis::new(100).aligned_by_bucket(bucket).unwrap()
        );
        assert_eq!(
            100,
            TimestampMillis::new(199).aligned_by_bucket(bucket).unwrap()
        );

        assert_eq!(0, TimestampMillis::MAX.aligned_by_bucket(i64::MAX).unwrap());
        assert_eq!(
            i64::MAX,
            TimestampMillis::INF.aligned_by_bucket(i64::MAX).unwrap()
        );

        assert_eq!(None, TimestampMillis::MIN.aligned_by_bucket(bucket));
    }
}
