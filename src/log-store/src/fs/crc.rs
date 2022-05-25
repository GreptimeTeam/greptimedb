use crc::{Crc, CRC_32_ISCSI};

pub const CRC_ALGO: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
