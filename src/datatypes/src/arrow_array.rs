use arrow::array::{
    BinaryArray as ArrowBinaryArray, MutableBinaryArray as ArrowMutableBinaryArray,
    MutableUtf8Array, Utf8Array,
};

pub type BinaryArray = ArrowBinaryArray<i64>;
pub type MutableBinaryArray = ArrowMutableBinaryArray<i64>;
pub type MutableStringArray = MutableUtf8Array<i64>;
pub type StringArray = Utf8Array<i64>;
