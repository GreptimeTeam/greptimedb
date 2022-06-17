//! Common types.

/// Represents a sequence number of data in storage. The offset of logstore can be used
/// as a sequence number.
pub type SequenceNumber = u64;

/// Operation type of the value to write to storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueType {
    /// Put operation.
    Put,
}

impl ValueType {
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Minimum value type after casting to u8.
    pub const fn min_type() -> ValueType {
        ValueType::Put
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type() {
        assert_eq!(0, ValueType::Put.as_u8());
        assert_eq!(0, ValueType::min_type().as_u8());
    }
}
