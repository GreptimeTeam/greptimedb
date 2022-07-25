use common_error::prelude::ErrorExt;

pub trait Encoder {
    /// The type that is decoded.
    type Item;
    type Error: ErrorExt;

    /// Encodes a message into the bytes buffer.
    fn encode(&self, item: &Self::Item, dst: &mut Vec<u8>) -> Result<(), Self::Error>;
}

pub trait Decoder {
    /// The type that is decoded.
    type Item;
    type Error: ErrorExt;

    /// Decodes a message from the bytes buffer.
    fn decode(&self, src: &[u8]) -> Result<Option<Self::Item>, Self::Error>;
}
