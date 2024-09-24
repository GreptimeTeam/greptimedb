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

use common_time::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::prelude::{Scalar, Value, ValueRef};
use crate::scalars::ScalarRef;
use crate::types::{
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType, WrapperType,
};
use crate::vectors::{IntervalDayTimeVector, IntervalMonthDayNanoVector, IntervalYearMonthVector};

macro_rules! define_interval_with_unit {
    ($unit: ident, $native_ty: ty) => {
        paste! {
            // impl From<[<Interval $unit>]> for Value {
            //     fn from(t: [<Interval $unit>]) -> Value {
            //         Value::[<Interval $unit>](t)
            //     }
            // }

            // impl From<[<Interval $unit>]> for ValueRef<'static> {
            //     fn from(t: [<Interval $unit>]) -> Self {
            //         ValueRef::[<Interval $unit>](t)
            //     }
            // }

            impl Scalar for [<Interval $unit>] {
                type VectorType = [<Interval $unit Vector>];
                type RefType<'a> = [<Interval $unit>];

                fn as_scalar_ref(&self) -> Self::RefType<'_> {
                    *self
                }

                fn upcast_gat<'short, 'long: 'short>(
                    long: Self::RefType<'long>,
                ) -> Self::RefType<'short> {
                    long
                }
            }

            impl<'a> ScalarRef<'a> for [<Interval $unit>] {
                type ScalarType = [<Interval $unit>];

                fn to_owned_scalar(&self) -> Self::ScalarType {
                    *self
                }
            }

            impl WrapperType for [<Interval $unit>] {
                type LogicalType = [<Interval $unit Type>];
                type Native = $native_ty;

                fn from_native(value: Self::Native) -> Self {
                    Self::[<from_ $native_ty>](value)
                }

                fn into_native(self) -> Self::Native {
                    self.[<to_ $native_ty>]()
                }
            }

            // impl TryFrom<Value> for Option<[<Interval $unit>]> {
            //     type Error = $crate::error::Error;

            //     #[inline]
            //     fn try_from(from: Value) -> std::result::Result<Self, Self::Error> {
            //         match from {
            //             Value::[<Interval $unit>](v) => {
            //                 Ok(Some(v))
            //             },
            //             Value::Null => Ok(None),
            //             _ => $crate::error::TryFromValueSnafu {
            //                 reason: format!("{:?} is not a {}", from, stringify!([<Interval $unit>])),
            //             }
            //             .fail(),
            //         }
            //     }
            // }
        }
    };
}

define_interval_with_unit!(YearMonth, i32);
define_interval_with_unit!(DayTime, i64);
define_interval_with_unit!(MonthDayNano, i128);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_scalar() {
        let interval = IntervalYearMonth::from(1000);
        assert_eq!(interval, interval.as_scalar_ref());
        assert_eq!(interval, interval.to_owned_scalar());
        assert_eq!(1000, interval.into_native());

        let interval = IntervalDayTime::from(1000);
        assert_eq!(interval, interval.as_scalar_ref());
        assert_eq!(interval, interval.to_owned_scalar());
        assert_eq!(1000, interval.into_native());

        let interval = IntervalMonthDayNano::from(1000);
        assert_eq!(interval, interval.as_scalar_ref());
        assert_eq!(interval, interval.to_owned_scalar());
        assert_eq!(1000, interval.into_native());
    }

    #[test]
    fn test_interval_convert_to_native_type() {
        let interval = IntervalMonthDayNano::from(1000);
        let native_value: i128 = interval.into();
        assert_eq!(native_value, 1000);

        let interval = IntervalDayTime::from(1000);
        let native_interval: i64 = interval.into();
        assert_eq!(native_interval, 1000);

        let interval = IntervalYearMonth::from(1000);
        let native_interval: i32 = interval.into();
        assert_eq!(native_interval, 1000);
    }
}
