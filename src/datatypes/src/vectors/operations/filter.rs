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

macro_rules! filter_non_constant {
    ($vector: expr, $VectorType: ty, $filter: ident) => {{
        use std::sync::Arc;

        use arrow::compute;
        use snafu::ResultExt;

        let arrow_array = $vector.as_arrow();
        let filtered = compute::filter(arrow_array, $filter.as_boolean_array())
            .context(crate::error::ArrowComputeSnafu)?;
        Ok(Arc::new(<$VectorType>::try_from_arrow_array(filtered)?))
    }};
}

pub(crate) use filter_non_constant;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::{Date, DateTime};

    use crate::scalars::ScalarVector;
    use crate::timestamp::{
        TimestampMicrosecond, TimestampMillisecond, TimestampNanosecond, TimestampSecond,
    };
    use crate::types::WrapperType;
    use crate::vectors::constant::ConstantVector;
    use crate::vectors::{
        BooleanVector, Int32Vector, NullVector, StringVector, VectorOp, VectorRef,
    };

    fn check_filter_primitive(expect: &[i32], input: &[i32], filter: &[bool]) {
        let v = Int32Vector::from_slice(input);
        let filter = BooleanVector::from_slice(filter);
        let out = v.filter(&filter).unwrap();

        let expect: VectorRef = Arc::new(Int32Vector::from_slice(expect));
        assert_eq!(expect, out);
    }

    #[test]
    fn test_filter_primitive() {
        check_filter_primitive(&[], &[], &[]);
        check_filter_primitive(&[5], &[5], &[true]);
        check_filter_primitive(&[], &[5], &[false]);
        check_filter_primitive(&[], &[5, 6], &[false, false]);
        check_filter_primitive(&[5, 6], &[5, 6], &[true, true]);
        check_filter_primitive(&[], &[5, 6, 7], &[false, false, false]);
        check_filter_primitive(&[5], &[5, 6, 7], &[true, false, false]);
        check_filter_primitive(&[6], &[5, 6, 7], &[false, true, false]);
        check_filter_primitive(&[7], &[5, 6, 7], &[false, false, true]);
        check_filter_primitive(&[5, 7], &[5, 6, 7], &[true, false, true]);
    }

    fn check_filter_constant(expect_length: usize, input_length: usize, filter: &[bool]) {
        let v = ConstantVector::new(Arc::new(Int32Vector::from_slice([123])), input_length);
        let filter = BooleanVector::from_slice(filter);
        let out = v.filter(&filter).unwrap();

        assert!(out.is_const());
        assert_eq!(expect_length, out.len());
    }

    #[test]
    fn test_filter_constant() {
        check_filter_constant(0, 0, &[]);
        check_filter_constant(1, 1, &[true]);
        check_filter_constant(0, 1, &[false]);
        check_filter_constant(1, 2, &[false, true]);
        check_filter_constant(2, 2, &[true, true]);
        check_filter_constant(1, 4, &[false, false, false, true]);
        check_filter_constant(2, 4, &[false, true, false, true]);
    }

    #[test]
    fn test_filter_scalar() {
        let v = StringVector::from_slice(&["0", "1", "2", "3"]);
        let filter = BooleanVector::from_slice(&[false, true, false, true]);
        let out = v.filter(&filter).unwrap();

        let expect: VectorRef = Arc::new(StringVector::from_slice(&["1", "3"]));
        assert_eq!(expect, out);
    }

    #[test]
    fn test_filter_null() {
        let v = NullVector::new(5);
        let filter = BooleanVector::from_slice(&[false, true, false, true, true]);
        let out = v.filter(&filter).unwrap();

        let expect: VectorRef = Arc::new(NullVector::new(3));
        assert_eq!(expect, out);
    }

    macro_rules! impl_filter_date_like_test {
        ($VectorType: ident, $ValueType: ident, $method: ident) => {{
            use std::sync::Arc;

            use $crate::vectors::{$VectorType, VectorRef};

            let v = $VectorType::from_iterator((0..5).map($ValueType::$method));
            let filter = BooleanVector::from_slice(&[false, true, false, true, true]);
            let out = v.filter(&filter).unwrap();

            let expect: VectorRef = Arc::new($VectorType::from_iterator(
                [1, 3, 4].into_iter().map($ValueType::$method),
            ));
            assert_eq!(expect, out);
        }};
    }

    #[test]
    fn test_filter_date_like() {
        impl_filter_date_like_test!(DateVector, Date, new);
        impl_filter_date_like_test!(DateTimeVector, DateTime, new);

        impl_filter_date_like_test!(TimestampSecondVector, TimestampSecond, from_native);
        impl_filter_date_like_test!(
            TimestampMillisecondVector,
            TimestampMillisecond,
            from_native
        );
        impl_filter_date_like_test!(
            TimestampMicrosecondVector,
            TimestampMicrosecond,
            from_native
        );
        impl_filter_date_like_test!(TimestampNanosecondVector, TimestampNanosecond, from_native);
    }
}
