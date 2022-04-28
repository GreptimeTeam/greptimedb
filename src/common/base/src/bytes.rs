/// Bytes buffer.
#[derive(Debug, Default, Clone)]
pub struct Bytes(Vec<u8>);

/// String buffer with arbitrary encoding.
#[derive(Debug, Default, Clone)]
pub struct StringBytes(Vec<u8>);
