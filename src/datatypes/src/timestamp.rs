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

use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::prelude::{Scalar, Value, ValueRef};
use crate::scalars::ScalarRef;
use crate::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, WrapperType,
};
use crate::vectors::{
    TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector,
};

macro_rules! define_timestamp_with_unit {
    ($unit: ident) => {
        paste! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
            pub struct [<Timestamp $unit>](pub Timestamp);

            impl [<Timestamp $unit>] {
                pub fn new(val: i64) -> Self {
                    Self(Timestamp::new(val, TimeUnit::$unit))
                }
            }

            impl Default for [<Timestamp $unit>] {
                fn default() -> Self {
                    Self::new(0)
                }
            }

            impl From<[<Timestamp $unit>]> for Value {
                fn from(t: [<Timestamp $unit>]) -> Value {
                    Value::Timestamp(t.0)
                }
            }

            impl From<[<Timestamp $unit>]> for serde_json::Value {
                fn from(t: [<Timestamp $unit>]) -> Self {
                    t.0.into()
                }
            }

            impl From<[<Timestamp $unit>]> for ValueRef<'static> {
                fn from(t: [<Timestamp $unit>]) -> Self {
                    ValueRef::Timestamp(t.0)
                }
            }

            impl Scalar for [<Timestamp $unit>] {
                type VectorType = [<Timestamp $unit Vector>];
                type RefType<'a> = [<Timestamp $unit>];

                fn as_scalar_ref(&self) -> Self::RefType<'_> {
                    *self
                }

                fn upcast_gat<'short, 'long: 'short>(
                    long: Self::RefType<'long>,
                ) -> Self::RefType<'short> {
                    long
                }
            }

            impl<'a> ScalarRef<'a> for [<Timestamp $unit>] {
                type ScalarType = [<Timestamp $unit>];

                fn to_owned_scalar(&self) -> Self::ScalarType {
                    *self
                }
            }

            impl WrapperType for [<Timestamp $unit>] {
                type LogicalType = [<Timestamp $unit Type>];
                type Native = i64;

                fn from_native(value: Self::Native) -> Self {
                    Self::new(value)
                }

                fn into_native(self) -> Self::Native {
                    self.0.into()
                }
            }

            impl From<i64> for [<Timestamp $unit>] {
                fn from(val: i64) -> Self {
                    [<Timestamp $unit>]::from_native(val)
                }
            }

            impl From<[<Timestamp $unit>]> for i64{
                fn from(val: [<Timestamp $unit>]) -> Self {
                    val.0.value()
                }
            }
        }
    };
}

define_timestamp_with_unit!(Second);
define_timestamp_with_unit!(Millisecond);
define_timestamp_with_unit!(Microsecond);
define_timestamp_with_unit!(Nanosecond);

#[cfg(test)]
mod tests {
    use common_time::timezone::set_default_timezone;

    use super::*;

    #[test]
    fn test_to_serde_json_value() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        let ts = TimestampSecond::new(123);
        let val = serde_json::Value::from(ts);
        match val {
            serde_json::Value::String(s) => {
                assert_eq!("1970-01-01 08:02:03+0800", s);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_timestamp_scalar() {
        let ts = TimestampSecond::new(123);
        assert_eq!(ts, ts.as_scalar_ref());
        assert_eq!(ts, ts.to_owned_scalar());
        let ts = TimestampMillisecond::new(123);
        assert_eq!(ts, ts.as_scalar_ref());
        assert_eq!(ts, ts.to_owned_scalar());
        let ts = TimestampMicrosecond::new(123);
        assert_eq!(ts, ts.as_scalar_ref());
        assert_eq!(ts, ts.to_owned_scalar());
        let ts = TimestampNanosecond::new(123);
        assert_eq!(ts, ts.as_scalar_ref());
        assert_eq!(ts, ts.to_owned_scalar());
    }
}
