use crate::prelude::*;
pub(crate) use crate::vectors::constant::replicate_constant;
pub(crate) use crate::vectors::date::replicate_date;
pub(crate) use crate::vectors::datetime::replicate_datetime;
pub(crate) use crate::vectors::null::replicate_null;
pub(crate) use crate::vectors::primitive::replicate_primitive;
pub(crate) use crate::vectors::timestamp::replicate_timestamp;

pub(crate) fn replicate_scalar<C: ScalarVector>(c: &C, offsets: &[usize]) -> VectorRef {
    assert_eq!(offsets.len(), c.len());

    if offsets.is_empty() {
        return c.slice(0, 0);
    }
    let mut builder = <<C as ScalarVector>::Builder>::with_capacity(c.len());

    let mut previous_offset = 0;
    for (i, offset) in offsets.iter().enumerate() {
        let data = c.get_data(i);
        for _ in previous_offset..*offset {
            builder.push(data);
        }
        previous_offset = *offset;
    }
    builder.to_vector()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::vectors::{ConstantVector, Int32Vector, NullVector, StringVector, VectorOp};

    #[test]
    fn test_replicate_primitive() {
        let v = Int32Vector::from_iterator(0..5);
        let offsets = [0, 1, 2, 3, 4];

        let v = v.replicate(&offsets);
        assert_eq!(4, v.len());

        for i in 0..4 {
            assert_eq!(Value::Int32(i as i32 + 1), v.get(i));
        }
    }

    #[test]
    fn test_replicate_scalar() {
        let v = StringVector::from_slice(&["0", "1", "2", "3"]);
        let offsets = [1, 3, 5, 6];

        let v = v.replicate(&offsets);
        assert_eq!(6, v.len());

        let expect: VectorRef = Arc::new(StringVector::from_slice(&["0", "1", "1", "2", "2", "3"]));
        assert_eq!(expect, v);
    }

    #[test]
    fn test_replicate_constant() {
        let v = Arc::new(StringVector::from_slice(&["hello"]));
        let cv = ConstantVector::new(v.clone(), 2);
        let offsets = [1, 4];

        let cv = cv.replicate(&offsets);
        assert_eq!(4, cv.len());

        let expect: VectorRef = Arc::new(ConstantVector::new(v, 4));
        assert_eq!(expect, cv);
    }

    #[test]
    fn test_replicate_null() {
        let v = NullVector::new(0);
        let offsets = [];
        let v = v.replicate(&offsets);
        assert!(v.is_empty());

        let v = NullVector::new(3);
        let offsets = [1, 3, 5];

        let v = v.replicate(&offsets);
        assert_eq!(5, v.len());
    }

    macro_rules! impl_replicate_date_like_test {
        ($VectorType: ident, $ValueType: ident, $method: ident) => {{
            use common_time::$ValueType;
            use $crate::vectors::$VectorType;

            let v = $VectorType::from_iterator((0..5).map($ValueType::$method));
            let offsets = [0, 1, 2, 3, 4];

            let v = v.replicate(&offsets);
            assert_eq!(4, v.len());

            for i in 0..4 {
                assert_eq!(
                    Value::$ValueType($ValueType::$method((i as i32 + 1).into())),
                    v.get(i)
                );
            }
        }};
    }

    #[test]
    fn test_replicate_date_like() {
        impl_replicate_date_like_test!(DateVector, Date, new);
        impl_replicate_date_like_test!(DateTimeVector, DateTime, new);
        impl_replicate_date_like_test!(TimestampVector, Timestamp, from_millis);
    }
}
