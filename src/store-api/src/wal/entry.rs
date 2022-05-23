pub type Offset = u64;

pub struct Entry {
    offset: Offset,
    epoch: u64,
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

    pub fn offset(&self) -> Offset {
        self.offset
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Return total length of entry after serialization(maybe CRC and length field)
    pub fn len(&self) -> usize {
        self.data.len() + 8
    }
}
