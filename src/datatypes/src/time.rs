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

use common_time::time::Time;
use common_time::timestamp::TimeUnit;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::prelude::{Scalar, Value, ValueRef};
use crate::scalars::ScalarRef;
use crate::types::{
    TimeMicrosecondType, TimeMillisecondType, TimeNanosecondType, TimeSecondType, WrapperType,
};
use crate::vectors::{
    TimeMicrosecondVector, TimeMillisecondVector, TimeNanosecondVector, TimeSecondVector,
};

macro_rules! define_time_with_unit {
    ($unit: ident, $native_ty: ty) => {
        paste! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
            pub struct [<Time $unit>](pub Time);

            impl [<Time $unit>] {
                pub fn new(val: i64) -> Self {
                    Self(Time::new(val, TimeUnit::$unit))
                }
            }

            impl Default for [<Time $unit>] {
                fn default() -> Self {
                    Self::new(0)
                }
            }

            impl From<[<Time $unit>]> for Value {
                fn from(t: [<Time $unit>]) -> Value {
                    Value::Time(t.0)
                }
            }

            impl From<[<Time $unit>]> for serde_json::Value {
                fn from(t: [<Time $unit>]) -> Self {
                    t.0.into()
                }
            }

            impl From<[<Time $unit>]> for ValueRef<'static> {
                fn from(t: [<Time $unit>]) -> Self {
                    ValueRef::Time(t.0)
                }
            }

            impl Scalar for [<Time $unit>] {
                type VectorType = [<Time $unit Vector>];
                type RefType<'a> = [<Time $unit>];

                fn as_scalar_ref(&self) -> Self::RefType<'_> {
                    *self
                }

                fn upcast_gat<'short, 'long: 'short>(
                    long: Self::RefType<'long>,
                ) -> Self::RefType<'short> {
                    long
                }
            }

            impl<'a> ScalarRef<'a> for [<Time $unit>] {
                type ScalarType = [<Time $unit>];

                fn to_owned_scalar(&self) -> Self::ScalarType {
                    *self
                }
            }

            impl WrapperType for [<Time $unit>] {
                type LogicalType = [<Time $unit Type>];
                type Native = $native_ty;

                fn from_native(value: Self::Native) -> Self {
                    Self::new(value.into())
                }

                fn into_native(self) -> Self::Native {
                    self.0.into()
                }
            }

            impl From<i64> for [<Time $unit>] {
                fn from(val: i64) -> Self {
                    [<Time $unit>]::from_native(val as $native_ty)
                }
            }

            impl From<[<Time $unit>]> for i64 {
                fn from(val: [<Time $unit>]) -> Self {
                    val.0.value()
                }
            }
        }
    };
}

define_time_with_unit!(Second, i32);
define_time_with_unit!(Millisecond, i32);
define_time_with_unit!(Microsecond, i64);
define_time_with_unit!(Nanosecond, i64);

#[cfg(test)]
mod tests {
    use common_time::timezone::set_default_timezone;

    use super::*;

    #[test]
    fn test_to_serde_json_value() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        let time = TimeSecond::new(123);
        let val = serde_json::Value::from(time);
        match val {
            serde_json::Value::String(s) => {
                assert_eq!("08:02:03+0800", s);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_time_scalar() {
        let time = TimeSecond::new(123);
        assert_eq!(time, time.as_scalar_ref());
        assert_eq!(time, time.to_owned_scalar());
        assert_eq!(123, time.into_native());
        let time = TimeMillisecond::new(123);
        assert_eq!(time, time.as_scalar_ref());
        assert_eq!(time, time.to_owned_scalar());
        assert_eq!(123, time.into_native());
        let time = TimeMicrosecond::new(123);
        assert_eq!(time, time.as_scalar_ref());
        assert_eq!(time, time.to_owned_scalar());
        assert_eq!(123, time.into_native());
        let time = TimeNanosecond::new(123);
        assert_eq!(time, time.as_scalar_ref());
        assert_eq!(time, time.to_owned_scalar());
        assert_eq!(123, time.into_native());
    }
}
