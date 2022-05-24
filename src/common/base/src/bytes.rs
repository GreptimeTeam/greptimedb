/// Bytes buffer.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Bytes(Vec<u8>);

/// String buffer with arbitrary encoding.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StringBytes(Vec<u8>);
