pub type Offset = u64;
pub type Epoch = u64;

pub struct Entry {
    /// Offset of current entry
    offset: Offset,
    /// Epoch of current entry
    epoch: Epoch,
    /// Binary data of current entry
    data: Vec<u8>,
}

impl Entry {
    pub fn new(data: impl AsRef<[u8]>, offset: Offset, epoch: u64) -> Self {
        let data = data.as_ref().to_vec();
        Self {
            data,
            offset,
            epoch,
        }
    }

    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn offset(&self) -> Offset {
        self.offset
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Return total length of entry after serialization(maybe CRC and length field)
    pub fn len(&self) -> usize {
        self.data.len() + 8
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }
}
