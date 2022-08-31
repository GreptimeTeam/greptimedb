use bitvec::prelude as bv;

// `Lsb0` provides the best codegen for bit manipulation,
// see https://github.com/bitvecto-rs/bitvec/blob/main/doc/order/Lsb0.md
pub type BitVec = bv::BitVec<u8, bv::Lsb0>;
