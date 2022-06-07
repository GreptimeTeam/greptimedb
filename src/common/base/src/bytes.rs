use serde::Serialize;
/// Bytes buffer.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
//TODO: impl From and Deref to remove pub declaration
pub struct Bytes(pub Vec<u8>);

/// String buffer with arbitrary encoding.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
//TODO: impl From and Deref to remove pub declaration
pub struct StringBytes(pub Vec<u8>);
