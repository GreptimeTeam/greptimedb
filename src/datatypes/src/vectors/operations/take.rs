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

macro_rules! take_indices {
    ($vector: expr, $VectorType: ty, $indices: ident) => {{
        use std::sync::Arc;

        use arrow::compute;
        use snafu::ResultExt;

        let arrow_array = $vector.as_arrow();
        let taken = compute::take(arrow_array, $indices.as_arrow(), None)
            .context(crate::error::ArrowComputeSnafu)?;
        Ok(Arc::new(<$VectorType>::try_from_arrow_array(taken)?))
    }};
}

pub(crate) use take_indices;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{PrimitiveArray, UInt32Array};
    use common_time::{Date, DateTime};

    use crate::prelude::VectorRef;
    use crate::scalars::ScalarVector;
    use crate::timestamp::{
        TimestampMicrosecond, TimestampMillisecond, TimestampNanosecond, TimestampSecond,
    };
    use crate::types::{LogicalPrimitiveType, WrapperType};
    use crate::vectors::operations::VectorOp;
    use crate::vectors::{
        BooleanVector, ConstantVector, Int32Vector, NullVector, PrimitiveVector, StringVector,
        UInt32Vector,
    };

    fn check_take_primitive<T>(
        input: Vec<Option<T::Native>>,
        indices: Vec<Option<u32>>,
        expect: Vec<Option<T::Native>>,
    ) where
        T: LogicalPrimitiveType,
        PrimitiveArray<T::ArrowPrimitive>: From<Vec<Option<T::Native>>>,
    {
        let v = PrimitiveVector::<T>::new(PrimitiveArray::<T::ArrowPrimitive>::from(input));
        let indices = UInt32Vector::new(UInt32Array::from(indices));
        let output = v.take(&indices).unwrap();

        let expected: VectorRef = Arc::new(PrimitiveVector::<T>::new(PrimitiveArray::<
            T::ArrowPrimitive,
        >::from(expect)));
        assert_eq!(expected, output);
    }

    macro_rules! take_time_like_test {
        ($VectorType: ident, $ValueType: ident, $method: ident) => {{
            use $crate::vectors::{$VectorType, VectorRef};

            let v = $VectorType::from_iterator((0..5).map($ValueType::$method));
            let indices = UInt32Vector::from_slice(&[3, 0, 1, 4]);
            let out = v.take(&indices).unwrap();

            let expect: VectorRef = Arc::new($VectorType::from_iterator(
                [3, 0, 1, 4].into_iter().map($ValueType::$method),
            ));
            assert_eq!(expect, out);
        }};
    }

    #[test]
    fn test_take_primitive() {
        // nullable int32
        check_take_primitive::<crate::types::Int32Type>(
            vec![Some(1), None, Some(3), Some(4), Some(-5)],
            vec![Some(3), None, Some(0), Some(1), Some(4)],
            vec![Some(4), None, Some(1), None, Some(-5)],
        );

        // nullable float32
        check_take_primitive::<crate::types::Float32Type>(
            vec![Some(3.24), None, Some(1.34), Some(4.13), Some(5.13)],
            vec![Some(3), None, Some(0), Some(1), Some(4)],
            vec![Some(4.13), None, Some(3.24), None, Some(5.13)],
        );

        // nullable uint32
        check_take_primitive::<crate::types::UInt32Type>(
            vec![Some(0), None, Some(2), Some(3), Some(4)],
            vec![Some(4), None, Some(2), Some(1), Some(3)],
            vec![Some(4), None, Some(2), None, Some(3)],
        );

        // test date like type
        take_time_like_test!(DateVector, Date, new);
        take_time_like_test!(DateTimeVector, DateTime, new);
        take_time_like_test!(TimestampSecondVector, TimestampSecond, from_native);
        take_time_like_test!(
            TimestampMillisecondVector,
            TimestampMillisecond,
            from_native
        );
        take_time_like_test!(
            TimestampMicrosecondVector,
            TimestampMicrosecond,
            from_native
        );
        take_time_like_test!(TimestampNanosecondVector, TimestampNanosecond, from_native);
    }

    fn check_take_constant(expect_length: usize, input_length: usize, indices: &[u32]) {
        let v = ConstantVector::new(Arc::new(Int32Vector::from_slice([111])), input_length);
        let indices = UInt32Vector::from_slice(indices);
        let out = v.take(&indices).unwrap();

        assert!(out.is_const());
        assert_eq!(expect_length, out.len());
    }

    #[test]
    fn test_take_constant() {
        check_take_constant(2, 5, &[3, 4]);
        check_take_constant(3, 10, &[1, 2, 3]);
        check_take_constant(4, 10, &[1, 5, 3, 6]);
        check_take_constant(5, 10, &[1, 9, 8, 7, 3]);
    }

    #[test]
    #[should_panic]
    fn test_take_constant_out_of_index() {
        check_take_constant(2, 5, &[3, 5]);
    }

    #[test]
    #[should_panic]
    fn test_take_out_of_index() {
        let v = Int32Vector::from_slice([1, 2, 3, 4, 5]);
        let indies = UInt32Vector::from_slice([1, 5, 6]);
        let _ = v.take(&indies);
    }

    #[test]
    fn test_take_null() {
        let v = NullVector::new(5);
        let indices = UInt32Vector::from_slice([1, 3, 2]);
        let out = v.take(&indices).unwrap();

        let expect: VectorRef = Arc::new(NullVector::new(3));
        assert_eq!(expect, out);
    }

    #[test]
    fn test_take_scalar() {
        let v = StringVector::from_slice(&["0", "1", "2", "3"]);
        let indices = UInt32Vector::from_slice([1, 3, 2]);
        let out = v.take(&indices).unwrap();

        let expect: VectorRef = Arc::new(StringVector::from_slice(&["1", "3", "2"]));
        assert_eq!(expect, out);
    }

    #[test]
    fn test_take_bool() {
        let v = BooleanVector::from_slice(&[false, true, false, true, false, false, true]);
        let indices = UInt32Vector::from_slice([1, 3, 5, 6]);
        let out = v.take(&indices).unwrap();
        let expected: VectorRef = Arc::new(BooleanVector::from_slice(&[true, true, false, true]));
        assert_eq!(out, expected);

        let v = BooleanVector::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
        ]);
        let indices = UInt32Vector::from(vec![Some(1), None, Some(3), Some(5), Some(6)]);
        let out = v.take(&indices).unwrap();
        let expected: VectorRef = Arc::new(BooleanVector::from(vec![
            None,
            None,
            Some(true),
            Some(false),
            Some(true),
        ]));
        assert_eq!(out, expected);
    }
}
