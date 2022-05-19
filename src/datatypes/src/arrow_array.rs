use arrow::array::{BinaryArray, MutableBinaryArray, MutableUtf8Array, Utf8Array};

pub type LargeBinaryArray = BinaryArray<i64>;
pub type MutableLargeBinaryArray = MutableBinaryArray<i64>;
pub type MutableStringArray = MutableUtf8Array<i32>;
pub type StringArray = Utf8Array<i32>;
