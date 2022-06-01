use serde::Serialize;
 /// Bytes buffer.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct Bytes(pub Vec<u8>);

/// String buffer with arbitrary encoding.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct StringBytes(pub Vec<u8>);
