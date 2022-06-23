use common_base::buffer::{Buffer, BufferMut};
use common_error::ext::ErrorExt;

pub type Offset = usize;
pub type Epoch = u64;
pub type Id = u64;

/// Entry is the minimal data storage unit in `LogStore`.
pub trait Entry: Encode + Send + Sync {
    type Error: ErrorExt + Send + Sync;

    /// Return contained data of entry.
    fn data(&self) -> &[u8];

    /// Return entry id that monotonically increments.
    fn id(&self) -> Id;

    /// Return file offset of entry.
    fn offset(&self) -> Offset;

    fn set_offset(&mut self, offset: Offset);

    fn set_id(&mut self, id: Id);

    /// Returns epoch of entry.
    fn epoch(&self) -> Epoch;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

pub trait Encode: Sized {
    type Error: ErrorExt + Send + Sync + 'static;

    /// Encodes item to given byte slice and return encoded size on success;
    /// # Panics
    /// If given buffer is not large enough to hold the encoded item.
    fn encode_to<T: BufferMut>(&self, buf: &mut T) -> Result<usize, Self::Error>;

    /// Decodes item from given buffer.
    fn decode<T: Buffer>(buf: &mut T) -> Result<Self, Self::Error>;

    /// Return the size in bytes of the encoded item;
    fn encoded_size(&self) -> usize;
}
