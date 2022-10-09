use std::sync::Arc;

use crate::data_type::DataType;
use crate::vectors::{
    BinaryVector, BooleanVector, ConstantVector, DateTimeVector, DateVector, ListVector,
    PrimitiveVector, StringVector, TimestampVector, Vector,
};
use crate::with_match_primitive_type_id;

impl Eq for dyn Vector + '_ {}

impl PartialEq for dyn Vector + '_ {
    fn eq(&self, other: &dyn Vector) -> bool {
        equal(self, other)
    }
}

impl PartialEq<dyn Vector> for Arc<dyn Vector + '_> {
    fn eq(&self, other: &dyn Vector) -> bool {
        equal(&**self, other)
    }
}

macro_rules! is_vector_eq {
    ($VectorType: ident, $lhs: ident, $rhs: ident) => {{
        let lhs = $lhs.as_any().downcast_ref::<$VectorType>().unwrap();
        let rhs = $rhs.as_any().downcast_ref::<$VectorType>().unwrap();

        lhs == rhs
    }};
}

fn equal(lhs: &dyn Vector, rhs: &dyn Vector) -> bool {
    if lhs.data_type() != rhs.data_type() || lhs.len() != rhs.len() {
        return false;
    }

    if lhs.is_const() || rhs.is_const() {
        // Length has been checked before, so we only need to compare inner
        // vector here.
        return equal(
            &**lhs
                .as_any()
                .downcast_ref::<ConstantVector>()
                .unwrap()
                .inner(),
            &**lhs
                .as_any()
                .downcast_ref::<ConstantVector>()
                .unwrap()
                .inner(),
        );
    }

    use crate::data_type::ConcreteDataType::*;

    let lhs_type = lhs.data_type();
    match lhs.data_type() {
        Null(_) => true,
        Boolean(_) => is_vector_eq!(BooleanVector, lhs, rhs),
        Binary(_) => is_vector_eq!(BinaryVector, lhs, rhs),
        String(_) => is_vector_eq!(StringVector, lhs, rhs),
        Date(_) => is_vector_eq!(DateVector, lhs, rhs),
        DateTime(_) => is_vector_eq!(DateTimeVector, lhs, rhs),
        Timestamp(_) => is_vector_eq!(TimestampVector, lhs, rhs),
        List(_) => is_vector_eq!(ListVector, lhs, rhs),
        UInt8(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int8(_) | Int16(_) | Int32(_) | Int64(_)
        | Float32(_) | Float64(_) => {
            with_match_primitive_type_id!(lhs_type.logical_type_id(), |$T| {
                let lhs = lhs.as_any().downcast_ref::<PrimitiveVector<$T>>().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveVector<$T>>().unwrap();

                lhs == rhs
            },
            {
                unreachable!("should not compare {} with {}", lhs.vector_type_name(), rhs.vector_type_name())
            })
        }
        Geometry(_) => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ListArray, MutableListArray, MutablePrimitiveArray, TryExtend};

    use super::*;
    use crate::vectors::{
        Float32Vector, Float64Vector, Int16Vector, Int32Vector, Int64Vector, Int8Vector,
        NullVector, TimestampVector, UInt16Vector, UInt32Vector, UInt64Vector, UInt8Vector,
        VectorRef,
    };

    fn assert_vector_ref_eq(vector: VectorRef) {
        let rhs = vector.clone();
        assert_eq!(vector, rhs);
        assert_dyn_vector_eq(&*vector, &*rhs);
    }

    fn assert_dyn_vector_eq(lhs: &dyn Vector, rhs: &dyn Vector) {
        assert_eq!(lhs, rhs);
    }

    fn assert_vector_ref_ne(lhs: VectorRef, rhs: VectorRef) {
        assert_ne!(lhs, rhs);
    }

    #[test]
    fn test_vector_eq() {
        assert_vector_ref_eq(Arc::new(BinaryVector::from(vec![
            Some(b"hello".to_vec()),
            Some(b"world".to_vec()),
        ])));
        assert_vector_ref_eq(Arc::new(BooleanVector::from(vec![true, false])));
        assert_vector_ref_eq(Arc::new(ConstantVector::new(
            Arc::new(BooleanVector::from(vec![true])),
            5,
        )));
        assert_vector_ref_eq(Arc::new(BooleanVector::from(vec![true, false])));
        assert_vector_ref_eq(Arc::new(DateVector::from(vec![Some(100), Some(120)])));
        assert_vector_ref_eq(Arc::new(DateTimeVector::from(vec![Some(100), Some(120)])));
        assert_vector_ref_eq(Arc::new(TimestampVector::from_values([100, 120])));

        let mut arrow_array = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
        arrow_array
            .try_extend(vec![Some(vec![Some(1), Some(2), Some(3)])])
            .unwrap();
        let arrow_array: ListArray<i32> = arrow_array.into();
        assert_vector_ref_eq(Arc::new(ListVector::from(arrow_array)));

        assert_vector_ref_eq(Arc::new(NullVector::new(4)));
        assert_vector_ref_eq(Arc::new(StringVector::from(vec![
            Some("hello"),
            Some("world"),
        ])));

        assert_vector_ref_eq(Arc::new(Int8Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(UInt8Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(Int16Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(UInt16Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(Int32Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(UInt32Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(Int64Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(UInt64Vector::from_slice(&[1, 2, 3, 4])));
        assert_vector_ref_eq(Arc::new(Float32Vector::from_slice(&[1.0, 2.0, 3.0, 4.0])));
        assert_vector_ref_eq(Arc::new(Float64Vector::from_slice(&[1.0, 2.0, 3.0, 4.0])));
    }

    #[test]
    fn test_vector_ne() {
        assert_vector_ref_ne(
            Arc::new(Int32Vector::from_slice(&[1, 2, 3, 4])),
            Arc::new(Int32Vector::from_slice(&[1, 2])),
        );
        assert_vector_ref_ne(
            Arc::new(Int32Vector::from_slice(&[1, 2, 3, 4])),
            Arc::new(Int8Vector::from_slice(&[1, 2, 3, 4])),
        );
        assert_vector_ref_ne(
            Arc::new(Int32Vector::from_slice(&[1, 2, 3, 4])),
            Arc::new(BooleanVector::from(vec![true, true])),
        );
        assert_vector_ref_ne(
            Arc::new(ConstantVector::new(
                Arc::new(BooleanVector::from(vec![true])),
                5,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(BooleanVector::from(vec![true])),
                4,
            )),
        );
        assert_vector_ref_ne(
            Arc::new(ConstantVector::new(
                Arc::new(BooleanVector::from(vec![true])),
                5,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(BooleanVector::from(vec![false])),
                4,
            )),
        );
        assert_vector_ref_ne(
            Arc::new(ConstantVector::new(
                Arc::new(BooleanVector::from(vec![true])),
                5,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(Int32Vector::from_slice(vec![1])),
                4,
            )),
        );
        assert_vector_ref_ne(Arc::new(NullVector::new(5)), Arc::new(NullVector::new(8)));
    }
}
