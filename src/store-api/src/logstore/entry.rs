pub type Offset = u64;
pub type Epoch = u64;

/// Entry is the minimal data storage unit in `LogStore`.
pub trait Entry {
    /// Return contained data of entry.
    fn data(&self) -> &[u8];

    /// Return offset of entry.
    fn offset(&self) -> Offset;

    /// Returns epoch of entry.
    fn epoch(&self) -> Epoch;
}
