use std::any::Any;
use std::default;

use common_time::{Date, DateTime, Timestamp};

use crate::prelude::*;
use crate::value::{GeometryValue, GeometryValueRef, ListValue, ListValueRef};
use crate::vectors::all::GeometryVector;
use crate::vectors::*;

fn get_iter_capacity<T, I: Iterator<Item = T>>(iter: &I) -> usize {
    match iter.size_hint() {
        (_lower, Some(upper)) => upper,
        (0, None) => 1024,
        (lower, None) => lower,
    }
}

/// Owned scalar value
/// primitive types, bool, Vec<u8> ...
pub trait Scalar: 'static + Sized + Default + Any
where
    for<'a> Self::VectorType: ScalarVector<RefItem<'a> = Self::RefType<'a>>,
{
    type VectorType: ScalarVector<OwnedItem = Self>;
    type RefType<'a>: ScalarRef<'a, ScalarType = Self, VectorType = Self::VectorType>
    where
        Self: 'a;
    /// Get a reference of the current value.
    fn as_scalar_ref(&self) -> Self::RefType<'_>;

    /// Upcast GAT type's lifetime.
    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short>;
}

pub trait ScalarRef<'a>: std::fmt::Debug + Clone + Copy + Send + 'a {
    type VectorType: ScalarVector<RefItem<'a> = Self>;
    /// The corresponding [`Scalar`] type.
    type ScalarType: Scalar<RefType<'a> = Self>;

    /// Convert the reference into an owned value.
    fn to_owned_scalar(&self) -> Self::ScalarType;
}

/// A sub trait of Vector to add scalar operation support.
// This implementation refers to Datebend's [ScalarColumn](https://github.com/datafuselabs/databend/blob/main/common/datavalues/src/scalars/type_.rs)
// and skyzh's [type-exercise-in-rust](https://github.com/skyzh/type-exercise-in-rust).
pub trait ScalarVector: Vector + Send + Sync + Sized + 'static
where
    for<'a> Self::OwnedItem: Scalar<RefType<'a> = Self::RefItem<'a>>,
{
    type OwnedItem: Scalar<VectorType = Self>;
    /// The reference item of this vector.
    type RefItem<'a>: ScalarRef<'a, ScalarType = Self::OwnedItem, VectorType = Self>
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

    fn from_slice(data: &[Self::RefItem<'_>]) -> Self {
        let mut builder = Self::Builder::with_capacity(data.len());
        for item in data {
            builder.push(Some(*item));
        }
        builder.finish()
    }

    fn from_iterator<'a>(it: impl Iterator<Item = Self::RefItem<'a>>) -> Self {
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            builder.push(Some(item));
        }
        builder.finish()
    }

    fn from_owned_iterator(it: impl Iterator<Item = Option<Self::OwnedItem>>) -> Self {
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            match item {
                Some(item) => builder.push(Some(item.as_scalar_ref())),
                None => builder.push(None),
            }
        }
        builder.finish()
    }

    fn from_vec<I: Into<Self::OwnedItem>>(values: Vec<I>) -> Self {
        let it = values.into_iter();
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            builder.push(Some(item.into().as_scalar_ref()));
        }
        builder.finish()
    }
}

/// A trait over all vector builders.
pub trait ScalarVectorBuilder: MutableVector {
    type VectorType: ScalarVector<Builder = Self>;

    /// Create a new builder with initial `capacity`.
    fn with_capacity(capacity: usize) -> Self;

    /// Push a value into the builder.
    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>);

    /// Finish build and return a new vector.
    fn finish(&mut self) -> Self::VectorType;
}

macro_rules! impl_primitive_scalar_type {
    ($native:ident) => {
        impl Scalar for $native {
            type VectorType = PrimitiveVector<$native>;
            type RefType<'a> = $native;

            #[inline]
            fn as_scalar_ref(&self) -> $native {
                *self
            }

            #[allow(clippy::needless_lifetimes)]
            #[inline]
            fn upcast_gat<'short, 'long: 'short>(long: $native) -> $native {
                long
            }
        }

        /// Implement [`ScalarRef`] for primitive types. Note that primitive types are both [`Scalar`] and [`ScalarRef`].
        impl<'a> ScalarRef<'a> for $native {
            type VectorType = PrimitiveVector<$native>;
            type ScalarType = $native;

            #[inline]
            fn to_owned_scalar(&self) -> $native {
                *self
            }
        }
    };
}

impl_primitive_scalar_type!(u8);
impl_primitive_scalar_type!(u16);
impl_primitive_scalar_type!(u32);
impl_primitive_scalar_type!(u64);
impl_primitive_scalar_type!(i8);
impl_primitive_scalar_type!(i16);
impl_primitive_scalar_type!(i32);
impl_primitive_scalar_type!(i64);
impl_primitive_scalar_type!(f32);
impl_primitive_scalar_type!(f64);

impl Scalar for bool {
    type VectorType = BooleanVector;
    type RefType<'a> = bool;

    #[inline]
    fn as_scalar_ref(&self) -> bool {
        *self
    }

    #[allow(clippy::needless_lifetimes)]
    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: bool) -> bool {
        long
    }
}

impl<'a> ScalarRef<'a> for bool {
    type VectorType = BooleanVector;
    type ScalarType = bool;

    #[inline]
    fn to_owned_scalar(&self) -> bool {
        *self
    }
}

impl Scalar for String {
    type VectorType = StringVector;
    type RefType<'a> = &'a str;

    #[inline]
    fn as_scalar_ref(&self) -> &str {
        self
    }

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long str) -> &'short str {
        long
    }
}

impl<'a> ScalarRef<'a> for &'a str {
    type VectorType = StringVector;
    type ScalarType = String;

    #[inline]
    fn to_owned_scalar(&self) -> String {
        self.to_string()
    }
}

impl Scalar for Vec<u8> {
    type VectorType = BinaryVector;
    type RefType<'a> = &'a [u8];

    #[inline]
    fn as_scalar_ref(&self) -> &[u8] {
        self
    }

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long [u8]) -> &'short [u8] {
        long
    }
}

impl<'a> ScalarRef<'a> for &'a [u8] {
    type VectorType = BinaryVector;
    type ScalarType = Vec<u8>;

    #[inline]
    fn to_owned_scalar(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl Scalar for Date {
    type VectorType = DateVector;
    type RefType<'a> = Date;

    fn as_scalar_ref(&self) -> Self::RefType<'_> {
        *self
    }

    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for Date {
    type VectorType = DateVector;
    type ScalarType = Date;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        *self
    }
}

impl Scalar for DateTime {
    type VectorType = DateTimeVector;
    type RefType<'a> = DateTime;

    fn as_scalar_ref(&self) -> Self::RefType<'_> {
        *self
    }

    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for DateTime {
    type VectorType = DateTimeVector;
    type ScalarType = DateTime;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        *self
    }
}

impl Scalar for Timestamp {
    type VectorType = TimestampVector;
    type RefType<'a> = Timestamp;

    fn as_scalar_ref(&self) -> Self::RefType<'_> {
        *self
    }

    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for Timestamp {
    type VectorType = TimestampVector;
    type ScalarType = Timestamp;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        *self
    }
}

impl Scalar for ListValue {
    type VectorType = ListVector;
    type RefType<'a> = ListValueRef<'a>;

    fn as_scalar_ref(&self) -> Self::RefType<'_> {
        ListValueRef::Ref { val: self }
    }

    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for ListValueRef<'a> {
    type VectorType = ListVector;
    type ScalarType = ListValue;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        match self {
            ListValueRef::Indexed { vector, idx } => match vector.get(*idx) {
                // Normally should not get `Value::Null` if the `ListValueRef` comes
                // from the iterator of the ListVector, but we avoid panic and just
                // returns a default list value in such case since `ListValueRef` may
                // be constructed manually.
                Value::Null => ListValue::default(),
                Value::List(v) => v,
                _ => unreachable!(),
            },
            ListValueRef::Ref { val } => (*val).clone(),
        }
    }
}

impl Scalar for GeometryValue {
    type VectorType = GeometryVector;

    type RefType<'a> = GeometryValueRef<'a>;

    fn as_scalar_ref(&self) -> Self::RefType<'_> {
        GeometryValueRef::Ref { val: self }
    }

    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for GeometryValueRef<'a> {
    type VectorType = GeometryVector;

    type ScalarType = GeometryValue;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        match self {
            GeometryValueRef::Indexed { vector, idx } => match vector.get(*idx) {
                Value::Geometry(value) => value,
                _ => unreachable!(),
            },
            GeometryValueRef::Ref { val } => (*val).clone(),
        }
    }
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

    #[test]
    fn test_build_date_vector() {
        let expect: Vec<Option<Date>> = vec![
            Some(Date::new(0)),
            Some(Date::new(-1)),
            None,
            Some(Date::new(1)),
        ];
        let vector: DateVector = build_vector_from_slice(&expect);
        assert_vector_eq(&expect, &vector);
    }

    #[test]
    fn test_date_scalar() {
        let date = Date::new(1);
        assert_eq!(date, date.as_scalar_ref());
        assert_eq!(date, date.to_owned_scalar());
    }

    #[test]
    fn test_datetime_scalar() {
        let dt = DateTime::new(123);
        assert_eq!(dt, dt.as_scalar_ref());
        assert_eq!(dt, dt.to_owned_scalar());
    }

    #[test]
    fn test_list_value_scalar() {
        let list_value = ListValue::new(
            Some(Box::new(vec![Value::Int32(123)])),
            ConcreteDataType::int32_datatype(),
        );
        let list_ref = ListValueRef::Ref { val: &list_value };
        assert_eq!(list_ref, list_value.as_scalar_ref());
        assert_eq!(list_value, list_ref.to_owned_scalar());

        let mut builder =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::int32_datatype(), 1);
        builder.push(None);
        builder.push(Some(list_value.as_scalar_ref()));
        let vector = builder.finish();

        let ref_on_vec = ListValueRef::Indexed {
            vector: &vector,
            idx: 0,
        };
        assert_eq!(ListValue::default(), ref_on_vec.to_owned_scalar());
        let ref_on_vec = ListValueRef::Indexed {
            vector: &vector,
            idx: 1,
        };
        assert_eq!(list_value, ref_on_vec.to_owned_scalar());
    }

    #[test]
    fn test_build_timestamp_vector() {
        let expect: Vec<Option<Timestamp>> = vec![Some(10.into()), None, Some(42.into())];
        let vector: TimestampVector = build_vector_from_slice(&expect);
        assert_vector_eq(&expect, &vector);
        let val = vector.get_data(0).unwrap();
        assert_eq!(val, val.as_scalar_ref());
        assert_eq!(10, val.to_owned_scalar().value());
    }
}
