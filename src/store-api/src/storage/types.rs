//! Common types.

/// Represents a sequence number of data in storage. The offset of logstore can be used
/// as a sequence number.
pub type SequenceNumber = u64;

/// Operation type of the value to write to storage.
#[derive(Debug)]
pub enum ValueType {
    /// Put operation.
    Put,
}
