pub(crate) mod handler;
pub mod insert;
pub mod select;

use bitvec::prelude as bv;

pub type BitVec = bv::BitVec<u8, bv::Lsb0>;
