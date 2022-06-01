use crate::vectors::Vector;

/// A sub trait of Vector to add scalar operation support.
// This implementation refers to Datebend's [ScalarColumn](https://github.com/datafuselabs/databend/blob/main/common/datavalues/src/scalars/type_.rs)
// and skyzh's [type-exercise-in-rust](https://github.com/skyzh/type-exercise-in-rust).
pub trait ScalarVector: Vector {
    /// The reference item of this vector.
    type RefItem<'a>: Copy
    where
        Self: 'a;

    /// Iterator type of this vector.
    type Iter<'a>: Iterator<Item = Option<Self::RefItem<'a>>>
    where
        Self: 'a;

    /// Builder type to build this vector.
    type Builder: ScalarVectorBuilder<VectorType = Self>;

    /// Returns the reference to an element at given position.
    ///
    /// Note: `get()` has bad performance, avoid call this function inside loop.
    ///
    /// # Panics
    /// Panics if `idx >= self.len()`.
    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>>;

    /// Returns iterator of current vector.
    fn iter_data(&self) -> Self::Iter<'_>;
}

/// A trait over all vector builders.
pub trait ScalarVectorBuilder {
    type VectorType: ScalarVector<Builder = Self>;

    /// Create a new builder with initial `capacity`.
    fn with_capacity(capacity: usize) -> Self;

    /// Push a value into the builder.
    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>);

    /// Finish build and return a new vector.
    fn finish(self) -> Self::VectorType;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vectors::binary::BinaryVector;
    use crate::vectors::primitive::Int32Vector;

    fn build_vector_from_slice<T: ScalarVector>(items: &[Option<T::RefItem<'_>>]) -> T {
        let mut builder = T::Builder::with_capacity(items.len());
        for item in items {
            builder.push(*item);
        }
        builder.finish()
    }

    fn assert_vector_eq<'a, T: ScalarVector>(expect: &[Option<T::RefItem<'a>>], vector: &'a T)
    where
        T::RefItem<'a>: PartialEq + std::fmt::Debug,
    {
        for (a, b) in expect.iter().zip(vector.iter_data()) {
            assert_eq!(*a, b);
        }
    }

    #[test]
    fn test_build_i32_vector() {
        let expect = vec![Some(1), Some(2), Some(3), None, Some(5)];
        let vector: Int32Vector = build_vector_from_slice(&expect);
        assert_vector_eq(&expect, &vector);
    }

    #[test]
    fn test_build_binary_vector() {
        let expect: Vec<Option<&'static [u8]>> = vec![
            Some(b"a"),
            Some(b"b"),
            Some(b"c"),
            None,
            Some(b"e"),
            Some(b""),
        ];
        let vector: BinaryVector = build_vector_from_slice(&expect);
        assert_vector_eq(&expect, &vector);
    }
}
