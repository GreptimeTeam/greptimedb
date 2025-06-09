// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Builders for time-series memtable

use std::sync::Arc;

use datatypes::arrow;
use datatypes::arrow::array::{
    Array, ArrayDataBuilder, BufferBuilder, GenericByteArray, NullBufferBuilder, UInt8BufferBuilder,
};
use datatypes::arrow_array::StringArray;
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, MutableVector, VectorRef};
use datatypes::value::ValueRef;
use datatypes::vectors::StringVector;

/// Field builder with special implementation for strings.
pub(crate) enum FieldBuilder {
    String(StringBuilder),
    Other(Box<dyn MutableVector>),
}

impl FieldBuilder {
    /// Creates a [FieldBuilder] instance with given type and capacity.
    pub fn create(data_type: &ConcreteDataType, init_cap: usize) -> Self {
        if let ConcreteDataType::String(_) = data_type {
            Self::String(StringBuilder::with_capacity(init_cap / 16, init_cap))
        } else {
            Self::Other(data_type.create_mutable_vector(init_cap))
        }
    }

    /// Pushes a value into builder.
    pub(crate) fn push(&mut self, value: ValueRef) -> datatypes::error::Result<()> {
        match self {
            FieldBuilder::String(b) => {
                if let Some(s) = value.as_string()? {
                    b.append(s);
                } else {
                    b.append_null();
                }
                Ok(())
            }
            FieldBuilder::Other(b) => b.try_push_value_ref(value),
        }
    }

    /// Push n null values into builder.
    pub(crate) fn push_nulls(&mut self, n: usize) {
        match self {
            FieldBuilder::String(s) => {
                s.append_n_nulls(n);
            }
            FieldBuilder::Other(v) => {
                v.push_nulls(n);
            }
        }
    }

    /// Finishes builder and builder a [VectorRef].
    pub(crate) fn finish(&mut self) -> VectorRef {
        match self {
            FieldBuilder::String(s) => Arc::new(StringVector::from(s.build())) as _,
            FieldBuilder::Other(v) => v.to_vector(),
        }
    }
}

/// [StringBuilder] serves as a workaround for lacking [`GenericStringBuilder::append_array`](https://docs.rs/arrow-array/latest/arrow_array/builder/type.GenericStringBuilder.html#method.append_array)
/// which is only available since arrow-rs 55.0.0.
pub(crate) struct StringBuilder {
    value_builder: UInt8BufferBuilder,
    offsets_builder: BufferBuilder<i32>,
    null_buffer_builder: NullBufferBuilder,
}

impl Default for StringBuilder {
    fn default() -> Self {
        Self::with_capacity(16, 256)
    }
}

impl StringBuilder {
    /// Creates a new [`GenericByteBuilder`].
    ///
    /// - `item_capacity` is the number of items to pre-allocate.
    ///   The size of the preallocated buffer of offsets is the number of items plus one.
    /// - `data_capacity` is the total number of bytes of data to pre-allocate
    ///   (for all items, not per item).
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<i32>::new(item_capacity + 1);
        offsets_builder.append(0);
        Self {
            value_builder: UInt8BufferBuilder::new(data_capacity),
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(item_capacity),
        }
    }

    pub fn append(&mut self, data: &str) {
        self.value_builder.append_slice(data.as_bytes());
        self.null_buffer_builder.append(true);
        self.offsets_builder.append(self.next_offset());
    }

    #[inline]
    fn next_offset(&self) -> i32 {
        i32::try_from(self.value_builder.len()).expect("byte array offset overflow")
    }

    pub fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Based on arrow-rs' GenericByteBuilder:
    /// https://github.com/apache/arrow-rs/blob/7905545537c50590fdb4dc645e3e0130fce80b57/arrow-array/src/builder/generic_bytes_builder.rs#L135
    pub fn append_array(&mut self, array: &StringArray) {
        if array.len() == 0 {
            return;
        }

        let offsets = array.offsets();

        // If the offsets are contiguous, we can append them directly avoiding the need to align
        // for example, when the first appended array is not sliced (starts at offset 0)
        if self.next_offset() == offsets[0] {
            self.offsets_builder.append_slice(&offsets[1..]);
        } else {
            // Shifting all the offsets
            let shift: i32 = self.next_offset() - offsets[0];

            // Creating intermediate offsets instead of pushing each offset is faster
            // (even if we make MutableBuffer to avoid updating length on each push
            //  and reserve the necessary capacity, it's still slower)
            let mut intermediate = Vec::with_capacity(offsets.len() - 1);

            for &offset in &offsets[1..] {
                intermediate.push(offset + shift)
            }

            self.offsets_builder.append_slice(&intermediate);
        }

        // Append underlying values, starting from the first offset and ending at the last offset
        self.value_builder.append_slice(
            &array.values().as_slice()[offsets[0] as usize..offsets[array.len()] as usize],
        );

        if let Some(null_buffer) = array.nulls() {
            let data: Vec<_> = null_buffer.inner().iter().collect();
            self.null_buffer_builder.append_slice(&data);
        } else {
            self.null_buffer_builder.append_n_non_nulls(array.len());
        }
    }

    pub fn append_null(&mut self) {
        self.null_buffer_builder.append(false);
        self.offsets_builder.append(self.next_offset());
    }

    pub fn append_n_nulls(&mut self, n: usize) {
        self.null_buffer_builder.append_n_nulls(n);
        self.offsets_builder.append_n(n, self.next_offset());
    }

    pub fn build(&mut self) -> StringArray {
        let array_builder = ArrayDataBuilder::new(arrow::datatypes::DataType::Utf8)
            .len(self.len())
            .add_buffer(self.offsets_builder.finish())
            .add_buffer(self.value_builder.finish())
            .nulls(self.null_buffer_builder.finish());

        self.offsets_builder.append(self.next_offset());
        let array_data = unsafe { array_builder.build_unchecked() };
        GenericByteArray::from(array_data)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::StringArray;

    use super::*;

    #[test]
    fn test_append() {
        let mut builder = StringBuilder::default();
        builder.append_n_nulls(10);
        let array = builder.build();
        assert_eq!(vec![None; 10], array.iter().collect::<Vec<_>>());

        let mut builder = StringBuilder::default();
        builder.append_n_nulls(3);
        builder.append("hello");
        builder.append_null();
        builder.append("world");
        assert_eq!(
            vec![None, None, None, Some("hello"), None, Some("world")],
            builder.build().iter().collect::<Vec<_>>()
        )
    }

    #[test]
    fn test_append_empty_string() {
        let mut builder = StringBuilder::default();
        builder.append("");
        builder.append_null();
        builder.append("");
        let array = builder.build();
        assert_eq!(
            vec![Some(""), None, Some("")],
            array.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_append_large_string() {
        let large_str = "a".repeat(1024);
        let mut builder = StringBuilder::default();
        builder.append(&large_str);
        let array = builder.build();
        assert_eq!(large_str.as_str(), array.value(0));
    }

    #[test]
    fn test_append_array() {
        let mut builder_1 = StringBuilder::default();
        builder_1.append("hello");
        builder_1.append_null();
        builder_1.append("world");

        let mut builder_2 = StringBuilder::default();
        builder_2.append_null();
        builder_2.append("!");
        builder_2.append_array(&builder_1.build());
        assert_eq!(
            vec![None, Some("!"), Some("hello"), None, Some("world")],
            builder_2.build().iter().collect::<Vec<_>>()
        )
    }

    #[test]
    fn test_append_empty_array() {
        let mut builder = StringBuilder::default();
        builder.append_array(&StringArray::from(vec![] as Vec<&str>));
        let array = builder.build();
        assert_eq!(0, array.len());
    }

    #[test]
    fn test_append_partial_array() {
        let source = StringArray::from(vec![Some("a"), None, Some("b"), Some("c")]);
        let sliced = source.slice(1, 2); // [None, Some("b")]

        let mut builder = StringBuilder::default();
        builder.append_array(&sliced);
        let array = builder.build();
        assert_eq!(vec![None, Some("b")], array.iter().collect::<Vec<_>>());
    }

    #[test]
    fn test_builder_capacity() {
        let mut builder = StringBuilder::with_capacity(10, 100);
        assert_eq!(0, builder.len());

        for i in 0..10 {
            builder.append(&format!("string-{}", i));
        }

        let array = builder.build();
        assert_eq!(10, array.len());
        assert_eq!("string-0", array.value(0));
        assert_eq!("string-9", array.value(9));
    }

    #[test]
    fn test_builder_reset_after_build() {
        let mut builder = StringBuilder::default();
        builder.append("first");
        let array1 = builder.build();
        assert_eq!(1, array1.len());

        builder.append("second");
        let array2 = builder.build();
        assert_eq!(1, array2.len()); // Not 2 because build() doesn't reset
    }
}
