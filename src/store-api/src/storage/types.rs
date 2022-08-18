//! Common types.

/// Represents a sequence number of data in storage. The offset of logstore can be used
/// as a sequence number.
pub type SequenceNumber = u64;

/// Operation type of the value to write to storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OpType {
    /// Put operation.
    Put,
}

impl OpType {
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Minimal op type after casting to u8.
    pub const fn min_type() -> OpType {
        OpType::Put
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_type() {
        assert_eq!(0, OpType::Put.as_u8());
        assert_eq!(0, OpType::min_type().as_u8());
    }
}
