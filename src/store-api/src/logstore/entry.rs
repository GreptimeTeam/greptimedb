use common_error::ext::ErrorExt;

pub type Offset = usize;
pub type Epoch = u64;
pub type Id = u64;

/// Entry is the minimal data storage unit in `LogStore`.
pub trait Entry: Send + Sync {
    type Error: ErrorExt + Send + Sync;

    /// Return contained data of entry.
    fn data(&self) -> &[u8];

    /// Return entry id
    fn id(&self) -> Id;

    /// Return offset of entry.
    fn offset(&self) -> Offset;

    fn set_offset(&mut self, offset: Offset);

    fn set_id(&mut self, id: Id);

    /// Returns epoch of entry.
    fn epoch(&self) -> Epoch;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn serialize(&self) -> Vec<u8>;

    fn deserialize(b: impl AsRef<[u8]>) -> Result<Self, Self::Error>
    where
        Self: Sized;
}
