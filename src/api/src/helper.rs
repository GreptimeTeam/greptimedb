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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use common_decimal::Decimal128;
use common_decimal::decimal128::{DECIMAL128_DEFAULT_SCALE, DECIMAL128_MAX_PRECISION};
use common_time::time::Time;
use common_time::timestamp::TimeUnit;
use common_time::{Date, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, Timestamp};
use datatypes::json::value::{JsonNumber, JsonValue, JsonValueRef, JsonVariant};
use datatypes::prelude::{ConcreteDataType, ValueRef};
use datatypes::types::{
    IntervalType, JsonFormat, JsonType, StructField, StructType, TimeType, TimestampType,
};
use datatypes::value::{ListValueRef, OrderedF32, OrderedF64, StructValueRef, Value};
use datatypes::vectors::VectorRef;
use greptime_proto::v1::column_data_type_extension::TypeExt;
use greptime_proto::v1::ddl_request::Expr;
use greptime_proto::v1::greptime_request::Request;
use greptime_proto::v1::query_request::Query;
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{
    self, ColumnDataTypeExtension, DdlRequest, DecimalTypeExtension, DictionaryTypeExtension,
    JsonList, JsonNativeTypeExtension, JsonObject, JsonTypeExtension, ListTypeExtension,
    QueryRequest, Row, SemanticType, StructTypeExtension, VectorTypeExtension, json_value,
};
use paste::paste;
use snafu::prelude::*;

use crate::error::{self, InconsistentTimeUnitSnafu, InvalidTimeUnitSnafu, Result};
use crate::v1::column::Values;
use crate::v1::{ColumnDataType, Value as GrpcValue};

/// ColumnDataTypeWrapper is a wrapper of ColumnDataType and ColumnDataTypeExtension.
/// It could be used to convert with ConcreteDataType.
#[derive(Debug, PartialEq)]
pub struct ColumnDataTypeWrapper {
    datatype: ColumnDataType,
    datatype_ext: Option<ColumnDataTypeExtension>,
}

impl ColumnDataTypeWrapper {
    /// Try to create a ColumnDataTypeWrapper from i32(ColumnDataType) and ColumnDataTypeExtension.
    pub fn try_new(datatype: i32, datatype_ext: Option<ColumnDataTypeExtension>) -> Result<Self> {
        let datatype = ColumnDataType::try_from(datatype)
            .context(error::UnknownColumnDataTypeSnafu { datatype })?;
        Ok(Self {
            datatype,
            datatype_ext,
        })
    }

    /// Create a ColumnDataTypeWrapper from ColumnDataType and ColumnDataTypeExtension.
    pub fn new(datatype: ColumnDataType, datatype_ext: Option<ColumnDataTypeExtension>) -> Self {
        Self {
            datatype,
            datatype_ext,
        }
    }

    /// Get the ColumnDataType.
    pub fn datatype(&self) -> ColumnDataType {
        self.datatype
    }

    /// Get a tuple of ColumnDataType and ColumnDataTypeExtension.
    pub fn to_parts(&self) -> (ColumnDataType, Option<ColumnDataTypeExtension>) {
        (self.datatype, self.datatype_ext.clone())
    }

    pub fn into_parts(self) -> (ColumnDataType, Option<ColumnDataTypeExtension>) {
        (self.datatype, self.datatype_ext)
    }
}

impl From<ColumnDataTypeWrapper> for ConcreteDataType {
    fn from(datatype_wrapper: ColumnDataTypeWrapper) -> Self {
        match datatype_wrapper.datatype {
            ColumnDataType::Boolean => ConcreteDataType::boolean_datatype(),
            ColumnDataType::Int8 => ConcreteDataType::int8_datatype(),
            ColumnDataType::Int16 => ConcreteDataType::int16_datatype(),
            ColumnDataType::Int32 => ConcreteDataType::int32_datatype(),
            ColumnDataType::Int64 => ConcreteDataType::int64_datatype(),
            ColumnDataType::Uint8 => ConcreteDataType::uint8_datatype(),
            ColumnDataType::Uint16 => ConcreteDataType::uint16_datatype(),
            ColumnDataType::Uint32 => ConcreteDataType::uint32_datatype(),
            ColumnDataType::Uint64 => ConcreteDataType::uint64_datatype(),
            ColumnDataType::Float32 => ConcreteDataType::float32_datatype(),
            ColumnDataType::Float64 => ConcreteDataType::float64_datatype(),
            ColumnDataType::Binary => {
                if let Some(TypeExt::JsonType(_)) = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref())
                {
                    ConcreteDataType::json_datatype()
                } else {
                    ConcreteDataType::binary_datatype()
                }
            }
            ColumnDataType::Json => {
                let type_ext = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref());
                match type_ext {
                    Some(TypeExt::JsonType(_)) => {
                        // legacy json type
                        ConcreteDataType::json_datatype()
                    }
                    Some(TypeExt::JsonNativeType(type_ext)) => {
                        // native json type
                        let inner_type = ColumnDataTypeWrapper {
                            datatype: type_ext.datatype(),
                            datatype_ext: type_ext.datatype_extension.clone().map(|d| *d),
                        };
                        ConcreteDataType::json_native_datatype(inner_type.into())
                    }
                    None => ConcreteDataType::Json(JsonType::null()),
                    _ => {
                        // invalid state, type extension is missing or invalid
                        ConcreteDataType::null_datatype()
                    }
                }
            }
            ColumnDataType::String => ConcreteDataType::string_datatype(),
            ColumnDataType::Date => ConcreteDataType::date_datatype(),
            ColumnDataType::Datetime => ConcreteDataType::timestamp_microsecond_datatype(),
            ColumnDataType::TimestampSecond => ConcreteDataType::timestamp_second_datatype(),
            ColumnDataType::TimestampMillisecond => {
                ConcreteDataType::timestamp_millisecond_datatype()
            }
            ColumnDataType::TimestampMicrosecond => {
                ConcreteDataType::timestamp_microsecond_datatype()
            }
            ColumnDataType::TimestampNanosecond => {
                ConcreteDataType::timestamp_nanosecond_datatype()
            }
            ColumnDataType::TimeSecond => ConcreteDataType::time_second_datatype(),
            ColumnDataType::TimeMillisecond => ConcreteDataType::time_millisecond_datatype(),
            ColumnDataType::TimeMicrosecond => ConcreteDataType::time_microsecond_datatype(),
            ColumnDataType::TimeNanosecond => ConcreteDataType::time_nanosecond_datatype(),
            ColumnDataType::IntervalYearMonth => ConcreteDataType::interval_year_month_datatype(),
            ColumnDataType::IntervalDayTime => ConcreteDataType::interval_day_time_datatype(),
            ColumnDataType::IntervalMonthDayNano => {
                ConcreteDataType::interval_month_day_nano_datatype()
            }
            ColumnDataType::Decimal128 => {
                if let Some(TypeExt::DecimalType(d)) = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref())
                {
                    ConcreteDataType::decimal128_datatype(d.precision as u8, d.scale as i8)
                } else {
                    ConcreteDataType::decimal128_default_datatype()
                }
            }
            ColumnDataType::Vector => {
                if let Some(TypeExt::VectorType(d)) = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref())
                {
                    ConcreteDataType::vector_datatype(d.dim)
                } else {
                    ConcreteDataType::vector_default_datatype()
                }
            }
            ColumnDataType::List => {
                if let Some(TypeExt::ListType(d)) = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref())
                {
                    let item_type = ColumnDataTypeWrapper {
                        datatype: d.datatype(),
                        datatype_ext: d.datatype_extension.clone().map(|d| *d),
                    };
                    ConcreteDataType::list_datatype(Arc::new(item_type.into()))
                } else {
                    // invalid state: type extension not found
                    ConcreteDataType::null_datatype()
                }
            }
            ColumnDataType::Struct => {
                if let Some(TypeExt::StructType(d)) = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref())
                {
                    let fields = d
                        .fields
                        .iter()
                        .map(|f| {
                            let field_type = ColumnDataTypeWrapper {
                                datatype: f.datatype(),
                                datatype_ext: f.datatype_extension.clone(),
                            };
                            StructField::new(f.name.clone(), field_type.into(), true)
                        })
                        .collect::<Vec<_>>();
                    ConcreteDataType::struct_datatype(StructType::new(Arc::new(fields)))
                } else {
                    // invalid state: type extension not found
                    ConcreteDataType::null_datatype()
                }
            }
            ColumnDataType::Dictionary => {
                if let Some(TypeExt::DictionaryType(d)) = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref())
                {
                    let key_type = ColumnDataTypeWrapper {
                        datatype: d.key_datatype(),
                        datatype_ext: d.key_datatype_extension.clone().map(|ext| *ext),
                    };
                    let value_type = ColumnDataTypeWrapper {
                        datatype: d.value_datatype(),
                        datatype_ext: d.value_datatype_extension.clone().map(|ext| *ext),
                    };
                    ConcreteDataType::dictionary_datatype(key_type.into(), value_type.into())
                } else {
                    // invalid state: type extension not found
                    ConcreteDataType::null_datatype()
                }
            }
        }
    }
}

/// This macro is used to generate datatype functions
/// with lower style for ColumnDataTypeWrapper.
///
///
/// For example: we can use `ColumnDataTypeWrapper::int8_datatype()`,
/// to get a ColumnDataTypeWrapper with datatype `ColumnDataType::Int8`.
macro_rules! impl_column_type_functions {
    ($($Type: ident), +) => {
        paste! {
            impl ColumnDataTypeWrapper {
                $(
                    pub fn [<$Type:lower _datatype>]() -> ColumnDataTypeWrapper {
                        ColumnDataTypeWrapper {
                            datatype: ColumnDataType::$Type,
                            datatype_ext: None,
                        }
                    }
                )+
            }
        }
    }
}

/// This macro is used to generate datatype functions
/// with snake style for ColumnDataTypeWrapper.
///
///
/// For example: we can use `ColumnDataTypeWrapper::duration_second_datatype()`,
/// to get a ColumnDataTypeWrapper with datatype `ColumnDataType::DurationSecond`.
macro_rules! impl_column_type_functions_with_snake {
    ($($TypeName: ident), +) => {
        paste!{
            impl ColumnDataTypeWrapper {
                $(
                    pub fn [<$TypeName:snake _datatype>]() -> ColumnDataTypeWrapper {
                        ColumnDataTypeWrapper {
                            datatype: ColumnDataType::$TypeName,
                            datatype_ext: None,
                        }
                    }
                )+
            }
        }
    };
}

impl_column_type_functions!(
    Boolean, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float32, Float64, Binary,
    Date, Datetime, String
);

impl_column_type_functions_with_snake!(
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    TimeSecond,
    TimeMillisecond,
    TimeMicrosecond,
    TimeNanosecond,
    IntervalYearMonth,
    IntervalDayTime,
    IntervalMonthDayNano
);

impl ColumnDataTypeWrapper {
    pub fn decimal128_datatype(precision: i32, scale: i32) -> Self {
        ColumnDataTypeWrapper {
            datatype: ColumnDataType::Decimal128,
            datatype_ext: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                    precision,
                    scale,
                })),
            }),
        }
    }

    pub fn vector_datatype(dim: u32) -> Self {
        ColumnDataTypeWrapper {
            datatype: ColumnDataType::Vector,
            datatype_ext: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::VectorType(VectorTypeExtension { dim })),
            }),
        }
    }

    /// Create a list datatype with the given item type.
    pub fn list_datatype(item_type: ColumnDataTypeWrapper) -> Self {
        ColumnDataTypeWrapper {
            datatype: ColumnDataType::List,
            datatype_ext: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::ListType(Box::new(ListTypeExtension {
                    datatype: item_type.datatype() as i32,
                    datatype_extension: item_type.datatype_ext.map(Box::new),
                }))),
            }),
        }
    }

    /// Create a struct datatype with the given field tuples (name, datatype).
    pub fn struct_datatype(fields: Vec<(String, ColumnDataTypeWrapper)>) -> Self {
        let struct_fields = fields
            .into_iter()
            .map(|(name, datatype)| greptime_proto::v1::StructField {
                name,
                datatype: datatype.datatype() as i32,
                datatype_extension: datatype.datatype_ext,
            })
            .collect();
        ColumnDataTypeWrapper {
            datatype: ColumnDataType::Struct,
            datatype_ext: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::StructType(StructTypeExtension {
                    fields: struct_fields,
                })),
            }),
        }
    }

    pub fn dictionary_datatype(
        key_type: ColumnDataTypeWrapper,
        value_type: ColumnDataTypeWrapper,
    ) -> Self {
        ColumnDataTypeWrapper {
            datatype: ColumnDataType::Dictionary,
            datatype_ext: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::DictionaryType(Box::new(DictionaryTypeExtension {
                    key_datatype: key_type.datatype().into(),
                    key_datatype_extension: key_type.datatype_ext.map(Box::new),
                    value_datatype: value_type.datatype().into(),
                    value_datatype_extension: value_type.datatype_ext.map(Box::new),
                }))),
            }),
        }
    }
}

impl TryFrom<ConcreteDataType> for ColumnDataTypeWrapper {
    type Error = error::Error;

    fn try_from(datatype: ConcreteDataType) -> Result<Self> {
        let column_datatype = match &datatype {
            ConcreteDataType::Boolean(_) => ColumnDataType::Boolean,
            ConcreteDataType::Int8(_) => ColumnDataType::Int8,
            ConcreteDataType::Int16(_) => ColumnDataType::Int16,
            ConcreteDataType::Int32(_) => ColumnDataType::Int32,
            ConcreteDataType::Int64(_) => ColumnDataType::Int64,
            ConcreteDataType::UInt8(_) => ColumnDataType::Uint8,
            ConcreteDataType::UInt16(_) => ColumnDataType::Uint16,
            ConcreteDataType::UInt32(_) => ColumnDataType::Uint32,
            ConcreteDataType::UInt64(_) => ColumnDataType::Uint64,
            ConcreteDataType::Float32(_) => ColumnDataType::Float32,
            ConcreteDataType::Float64(_) => ColumnDataType::Float64,
            ConcreteDataType::Binary(_) => ColumnDataType::Binary,
            ConcreteDataType::String(_) => ColumnDataType::String,
            ConcreteDataType::Date(_) => ColumnDataType::Date,
            ConcreteDataType::Timestamp(t) => match t {
                TimestampType::Second(_) => ColumnDataType::TimestampSecond,
                TimestampType::Millisecond(_) => ColumnDataType::TimestampMillisecond,
                TimestampType::Microsecond(_) => ColumnDataType::TimestampMicrosecond,
                TimestampType::Nanosecond(_) => ColumnDataType::TimestampNanosecond,
            },
            ConcreteDataType::Time(t) => match t {
                TimeType::Second(_) => ColumnDataType::TimeSecond,
                TimeType::Millisecond(_) => ColumnDataType::TimeMillisecond,
                TimeType::Microsecond(_) => ColumnDataType::TimeMicrosecond,
                TimeType::Nanosecond(_) => ColumnDataType::TimeNanosecond,
            },
            ConcreteDataType::Interval(i) => match i {
                IntervalType::YearMonth(_) => ColumnDataType::IntervalYearMonth,
                IntervalType::DayTime(_) => ColumnDataType::IntervalDayTime,
                IntervalType::MonthDayNano(_) => ColumnDataType::IntervalMonthDayNano,
            },
            ConcreteDataType::Decimal128(_) => ColumnDataType::Decimal128,
            ConcreteDataType::Json(_) => ColumnDataType::Json,
            ConcreteDataType::Vector(_) => ColumnDataType::Vector,
            ConcreteDataType::List(_) => ColumnDataType::List,
            ConcreteDataType::Struct(_) => ColumnDataType::Struct,
            ConcreteDataType::Dictionary(_) => ColumnDataType::Dictionary,
            ConcreteDataType::Null(_) | ConcreteDataType::Duration(_) => {
                return error::IntoColumnDataTypeSnafu { from: datatype }.fail();
            }
        };
        let datatype_extension = match column_datatype {
            ColumnDataType::Decimal128 => {
                datatype
                    .as_decimal128()
                    .map(|decimal_type| ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                            precision: decimal_type.precision() as i32,
                            scale: decimal_type.scale() as i32,
                        })),
                    })
            }
            ColumnDataType::Json => {
                if let Some(json_type) = datatype.as_json() {
                    match &json_type.format {
                        JsonFormat::Jsonb => Some(ColumnDataTypeExtension {
                            type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
                        }),
                        JsonFormat::Native(native_type) => {
                            if native_type.is_null() {
                                None
                            } else {
                                let native_type = ConcreteDataType::from(native_type.as_ref());
                                let (datatype, datatype_extension) =
                                    ColumnDataTypeWrapper::try_from(native_type)?.into_parts();
                                Some(ColumnDataTypeExtension {
                                    type_ext: Some(TypeExt::JsonNativeType(Box::new(
                                        JsonNativeTypeExtension {
                                            datatype: datatype as i32,
                                            datatype_extension: datatype_extension.map(Box::new),
                                        },
                                    ))),
                                })
                            }
                        }
                    }
                } else {
                    None
                }
            }
            ColumnDataType::Vector => {
                datatype
                    .as_vector()
                    .map(|vector_type| ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::VectorType(VectorTypeExtension {
                            dim: vector_type.dim as _,
                        })),
                    })
            }
            ColumnDataType::List => {
                if let Some(list_type) = datatype.as_list() {
                    let list_item_type =
                        ColumnDataTypeWrapper::try_from(list_type.item_type().clone())?;
                    Some(ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::ListType(Box::new(ListTypeExtension {
                            datatype: list_item_type.datatype.into(),
                            datatype_extension: list_item_type.datatype_ext.map(Box::new),
                        }))),
                    })
                } else {
                    None
                }
            }
            ColumnDataType::Struct => {
                if let Some(struct_type) = datatype.as_struct() {
                    let mut fields = Vec::with_capacity(struct_type.fields().len());
                    for field in struct_type.fields().iter() {
                        let field_type =
                            ColumnDataTypeWrapper::try_from(field.data_type().clone())?;
                        let proto_field = crate::v1::StructField {
                            name: field.name().to_string(),
                            datatype: field_type.datatype.into(),
                            datatype_extension: field_type.datatype_ext,
                        };
                        fields.push(proto_field);
                    }
                    Some(ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::StructType(StructTypeExtension { fields })),
                    })
                } else {
                    None
                }
            }
            ColumnDataType::Dictionary => {
                if let ConcreteDataType::Dictionary(dict_type) = &datatype {
                    let key_type = ColumnDataTypeWrapper::try_from(dict_type.key_type().clone())?;
                    let value_type =
                        ColumnDataTypeWrapper::try_from(dict_type.value_type().clone())?;
                    Some(ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::DictionaryType(Box::new(
                            DictionaryTypeExtension {
                                key_datatype: key_type.datatype.into(),
                                key_datatype_extension: key_type.datatype_ext.map(Box::new),
                                value_datatype: value_type.datatype.into(),
                                value_datatype_extension: value_type.datatype_ext.map(Box::new),
                            },
                        ))),
                    })
                } else {
                    None
                }
            }
            _ => None,
        };
        Ok(Self {
            datatype: column_datatype,
            datatype_ext: datatype_extension,
        })
    }
}

pub fn values_with_capacity(datatype: ColumnDataType, capacity: usize) -> Values {
    match datatype {
        ColumnDataType::Boolean => Values {
            bool_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int8 => Values {
            i8_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int16 => Values {
            i16_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int32 => Values {
            i32_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int64 => Values {
            i64_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint8 => Values {
            u8_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint16 => Values {
            u16_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint32 => Values {
            u32_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint64 => Values {
            u64_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Float32 => Values {
            f32_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Float64 => Values {
            f64_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Binary => Values {
            binary_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::String => Values {
            string_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Date => Values {
            date_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Datetime => Values {
            datetime_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampSecond => Values {
            timestamp_second_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampMillisecond => Values {
            timestamp_millisecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampMicrosecond => Values {
            timestamp_microsecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampNanosecond => Values {
            timestamp_nanosecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeSecond => Values {
            time_second_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeMillisecond => Values {
            time_millisecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeMicrosecond => Values {
            time_microsecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeNanosecond => Values {
            time_nanosecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::IntervalDayTime => Values {
            interval_day_time_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::IntervalYearMonth => Values {
            interval_year_month_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::IntervalMonthDayNano => Values {
            interval_month_day_nano_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Decimal128 => Values {
            decimal128_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Json => Values {
            // TODO(sunng87): remove this when we finally sunset legacy jsonb
            string_values: Vec::with_capacity(capacity),
            // for native json
            json_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Vector => Values {
            binary_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::List => Values {
            list_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Struct => Values {
            struct_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Dictionary => Values {
            ..Default::default()
        },
    }
}

/// Returns the type name of the [Request].
pub fn request_type(request: &Request) -> &'static str {
    match request {
        Request::Inserts(_) => "inserts",
        Request::Query(query_req) => query_request_type(query_req),
        Request::Ddl(ddl_req) => ddl_request_type(ddl_req),
        Request::Deletes(_) => "deletes",
        Request::RowInserts(_) => "row_inserts",
        Request::RowDeletes(_) => "row_deletes",
    }
}

/// Returns the type name of the [QueryRequest].
fn query_request_type(request: &QueryRequest) -> &'static str {
    match request.query {
        Some(Query::Sql(_)) => "query.sql",
        Some(Query::LogicalPlan(_)) => "query.logical_plan",
        Some(Query::PromRangeQuery(_)) => "query.prom_range",
        Some(Query::InsertIntoPlan(_)) => "query.insert_into_plan",
        None => "query.empty",
    }
}

/// Returns the type name of the [DdlRequest].
fn ddl_request_type(request: &DdlRequest) -> &'static str {
    match request.expr {
        Some(Expr::CreateDatabase(_)) => "ddl.create_database",
        Some(Expr::CreateTable(_)) => "ddl.create_table",
        Some(Expr::AlterTable(_)) => "ddl.alter_table",
        Some(Expr::DropTable(_)) => "ddl.drop_table",
        Some(Expr::TruncateTable(_)) => "ddl.truncate_table",
        Some(Expr::CreateFlow(_)) => "ddl.create_flow",
        Some(Expr::DropFlow(_)) => "ddl.drop_flow",
        Some(Expr::CreateView(_)) => "ddl.create_view",
        Some(Expr::DropView(_)) => "ddl.drop_view",
        Some(Expr::AlterDatabase(_)) => "ddl.alter_database",
        Some(Expr::CommentOn(_)) => "ddl.comment_on",
        None => "ddl.empty",
    }
}

/// Converts an interval to google protobuf type [IntervalMonthDayNano].
pub fn convert_month_day_nano_to_pb(v: IntervalMonthDayNano) -> v1::IntervalMonthDayNano {
    v1::IntervalMonthDayNano {
        months: v.months,
        days: v.days,
        nanoseconds: v.nanoseconds,
    }
}

/// Convert common decimal128 to grpc decimal128 without precision and scale.
pub fn convert_to_pb_decimal128(v: Decimal128) -> v1::Decimal128 {
    let (hi, lo) = v.split_value();
    v1::Decimal128 { hi, lo }
}

pub fn pb_value_to_value_ref<'a>(
    value: &'a v1::Value,
    datatype_ext: Option<&'a ColumnDataTypeExtension>,
) -> ValueRef<'a> {
    let Some(value) = &value.value_data else {
        return ValueRef::Null;
    };

    match value {
        ValueData::I8Value(v) => ValueRef::Int8(*v as i8),
        ValueData::I16Value(v) => ValueRef::Int16(*v as i16),
        ValueData::I32Value(v) => ValueRef::Int32(*v),
        ValueData::I64Value(v) => ValueRef::Int64(*v),
        ValueData::U8Value(v) => ValueRef::UInt8(*v as u8),
        ValueData::U16Value(v) => ValueRef::UInt16(*v as u16),
        ValueData::U32Value(v) => ValueRef::UInt32(*v),
        ValueData::U64Value(v) => ValueRef::UInt64(*v),
        ValueData::F32Value(f) => ValueRef::Float32(OrderedF32::from(*f)),
        ValueData::F64Value(f) => ValueRef::Float64(OrderedF64::from(*f)),
        ValueData::BoolValue(b) => ValueRef::Boolean(*b),
        ValueData::BinaryValue(bytes) => ValueRef::Binary(bytes.as_slice()),
        ValueData::StringValue(string) => ValueRef::String(string.as_str()),
        ValueData::DateValue(d) => ValueRef::Date(Date::from(*d)),
        ValueData::TimestampSecondValue(t) => ValueRef::Timestamp(Timestamp::new_second(*t)),
        ValueData::TimestampMillisecondValue(t) => {
            ValueRef::Timestamp(Timestamp::new_millisecond(*t))
        }
        ValueData::DatetimeValue(t) | ValueData::TimestampMicrosecondValue(t) => {
            ValueRef::Timestamp(Timestamp::new_microsecond(*t))
        }
        ValueData::TimestampNanosecondValue(t) => {
            ValueRef::Timestamp(Timestamp::new_nanosecond(*t))
        }
        ValueData::TimeSecondValue(t) => ValueRef::Time(Time::new_second(*t)),
        ValueData::TimeMillisecondValue(t) => ValueRef::Time(Time::new_millisecond(*t)),
        ValueData::TimeMicrosecondValue(t) => ValueRef::Time(Time::new_microsecond(*t)),
        ValueData::TimeNanosecondValue(t) => ValueRef::Time(Time::new_nanosecond(*t)),
        ValueData::IntervalYearMonthValue(v) => {
            ValueRef::IntervalYearMonth(IntervalYearMonth::from_i32(*v))
        }
        ValueData::IntervalDayTimeValue(v) => {
            ValueRef::IntervalDayTime(IntervalDayTime::from_i64(*v))
        }
        ValueData::IntervalMonthDayNanoValue(v) => {
            let interval = IntervalMonthDayNano::new(v.months, v.days, v.nanoseconds);
            ValueRef::IntervalMonthDayNano(interval)
        }
        ValueData::Decimal128Value(v) => {
            // get precision and scale from datatype_extension
            if let Some(TypeExt::DecimalType(d)) = datatype_ext
                .as_ref()
                .and_then(|column_ext| column_ext.type_ext.as_ref())
            {
                ValueRef::Decimal128(Decimal128::from_value_precision_scale(
                    v.hi,
                    v.lo,
                    d.precision as u8,
                    d.scale as i8,
                ))
            } else {
                // If the precision and scale are not set, use the default value.
                ValueRef::Decimal128(Decimal128::from_value_precision_scale(
                    v.hi,
                    v.lo,
                    DECIMAL128_MAX_PRECISION,
                    DECIMAL128_DEFAULT_SCALE,
                ))
            }
        }
        ValueData::ListValue(list) => {
            let list_datatype_ext = datatype_ext
                .as_ref()
                .and_then(|ext| {
                    if let Some(TypeExt::ListType(l)) = &ext.type_ext {
                        Some(l)
                    } else {
                        None
                    }
                })
                .expect("list must contain datatype ext");
            let item_type = ConcreteDataType::from(ColumnDataTypeWrapper::new(
                list_datatype_ext.datatype(),
                list_datatype_ext
                    .datatype_extension
                    .as_ref()
                    .map(|ext| *ext.clone()),
            ));
            let items = list
                .items
                .iter()
                .map(|item| {
                    pb_value_to_value_ref(item, list_datatype_ext.datatype_extension.as_deref())
                })
                .collect::<Vec<_>>();

            let list_value = ListValueRef::RefList {
                val: items,
                item_datatype: Arc::new(item_type.clone()),
            };
            ValueRef::List(list_value)
        }

        ValueData::StructValue(struct_value) => {
            let struct_datatype_ext = datatype_ext
                .as_ref()
                .and_then(|ext| {
                    if let Some(TypeExt::StructType(s)) = &ext.type_ext {
                        Some(s)
                    } else {
                        None
                    }
                })
                .expect("struct must contain datatype ext");

            let struct_fields = struct_datatype_ext
                .fields
                .iter()
                .map(|field| {
                    let field_type = ConcreteDataType::from(ColumnDataTypeWrapper::new(
                        field.datatype(),
                        field.datatype_extension.clone(),
                    ));
                    let field_name = field.name.clone();
                    StructField::new(field_name, field_type, true)
                })
                .collect::<Vec<_>>();

            let items = struct_value
                .items
                .iter()
                .zip(struct_datatype_ext.fields.iter())
                .map(|(item, field)| pb_value_to_value_ref(item, field.datatype_extension.as_ref()))
                .collect::<Vec<ValueRef>>();

            let struct_value_ref = StructValueRef::RefList {
                val: items,
                fields: StructType::new(Arc::new(struct_fields)),
            };
            ValueRef::Struct(struct_value_ref)
        }

        ValueData::JsonValue(inner_value) => {
            let value = decode_json_value(inner_value);
            ValueRef::Json(Box::new(value))
        }
    }
}

/// Returns true if the pb semantic type is valid.
pub fn is_semantic_type_eq(type_value: i32, semantic_type: SemanticType) -> bool {
    type_value == semantic_type as i32
}

/// Returns true if the pb type value is valid.
pub fn is_column_type_value_eq(
    type_value: i32,
    type_extension: Option<ColumnDataTypeExtension>,
    expect_type: &ConcreteDataType,
) -> bool {
    ColumnDataTypeWrapper::try_new(type_value, type_extension)
        .map(|wrapper| {
            let datatype = ConcreteDataType::from(wrapper);
            expect_type == &datatype
        })
        .unwrap_or(false)
}

pub fn encode_json_value(value: JsonValue) -> v1::JsonValue {
    fn helper(json: JsonVariant) -> v1::JsonValue {
        let value = match json {
            JsonVariant::Null => None,
            JsonVariant::Bool(x) => Some(json_value::Value::Boolean(x)),
            JsonVariant::Number(x) => Some(match x {
                JsonNumber::PosInt(i) => json_value::Value::Uint(i),
                JsonNumber::NegInt(i) => json_value::Value::Int(i),
                JsonNumber::Float(f) => json_value::Value::Float(f.0),
            }),
            JsonVariant::String(x) => Some(json_value::Value::Str(x)),
            JsonVariant::Array(x) => Some(json_value::Value::Array(JsonList {
                items: x.into_iter().map(helper).collect::<Vec<_>>(),
            })),
            JsonVariant::Object(x) => {
                let entries = x
                    .into_iter()
                    .map(|(key, v)| v1::json_object::Entry {
                        key,
                        value: Some(helper(v)),
                    })
                    .collect::<Vec<_>>();
                Some(json_value::Value::Object(JsonObject { entries }))
            }
        };
        v1::JsonValue { value }
    }
    helper(value.into_variant())
}

fn decode_json_value(value: &v1::JsonValue) -> JsonValueRef<'_> {
    let Some(value) = &value.value else {
        return JsonValueRef::null();
    };
    match value {
        json_value::Value::Boolean(x) => (*x).into(),
        json_value::Value::Int(x) => (*x).into(),
        json_value::Value::Uint(x) => (*x).into(),
        json_value::Value::Float(x) => (*x).into(),
        json_value::Value::Str(x) => (x.as_str()).into(),
        json_value::Value::Array(array) => array
            .items
            .iter()
            .map(|x| decode_json_value(x).into_variant())
            .collect::<Vec<_>>()
            .into(),
        json_value::Value::Object(x) => x
            .entries
            .iter()
            .filter_map(|entry| {
                entry
                    .value
                    .as_ref()
                    .map(|v| (entry.key.as_str(), decode_json_value(v).into_variant()))
            })
            .collect::<BTreeMap<_, _>>()
            .into(),
    }
}

/// Returns the [ColumnDataTypeWrapper] of the value.
///
/// If value is null, returns `None`.
pub fn proto_value_type(value: &v1::Value) -> Option<ColumnDataType> {
    let value_type = match value.value_data.as_ref()? {
        ValueData::I8Value(_) => ColumnDataType::Int8,
        ValueData::I16Value(_) => ColumnDataType::Int16,
        ValueData::I32Value(_) => ColumnDataType::Int32,
        ValueData::I64Value(_) => ColumnDataType::Int64,
        ValueData::U8Value(_) => ColumnDataType::Uint8,
        ValueData::U16Value(_) => ColumnDataType::Uint16,
        ValueData::U32Value(_) => ColumnDataType::Uint32,
        ValueData::U64Value(_) => ColumnDataType::Uint64,
        ValueData::F32Value(_) => ColumnDataType::Float32,
        ValueData::F64Value(_) => ColumnDataType::Float64,
        ValueData::BoolValue(_) => ColumnDataType::Boolean,
        ValueData::BinaryValue(_) => ColumnDataType::Binary,
        ValueData::StringValue(_) => ColumnDataType::String,
        ValueData::DateValue(_) => ColumnDataType::Date,
        ValueData::DatetimeValue(_) => ColumnDataType::Datetime,
        ValueData::TimestampSecondValue(_) => ColumnDataType::TimestampSecond,
        ValueData::TimestampMillisecondValue(_) => ColumnDataType::TimestampMillisecond,
        ValueData::TimestampMicrosecondValue(_) => ColumnDataType::TimestampMicrosecond,
        ValueData::TimestampNanosecondValue(_) => ColumnDataType::TimestampNanosecond,
        ValueData::TimeSecondValue(_) => ColumnDataType::TimeSecond,
        ValueData::TimeMillisecondValue(_) => ColumnDataType::TimeMillisecond,
        ValueData::TimeMicrosecondValue(_) => ColumnDataType::TimeMicrosecond,
        ValueData::TimeNanosecondValue(_) => ColumnDataType::TimeNanosecond,
        ValueData::IntervalYearMonthValue(_) => ColumnDataType::IntervalYearMonth,
        ValueData::IntervalDayTimeValue(_) => ColumnDataType::IntervalDayTime,
        ValueData::IntervalMonthDayNanoValue(_) => ColumnDataType::IntervalMonthDayNano,
        ValueData::Decimal128Value(_) => ColumnDataType::Decimal128,
        ValueData::ListValue(_) => ColumnDataType::List,
        ValueData::StructValue(_) => ColumnDataType::Struct,
        ValueData::JsonValue(_) => ColumnDataType::Json,
    };
    Some(value_type)
}

pub fn vectors_to_rows<'a>(
    columns: impl Iterator<Item = &'a VectorRef>,
    row_count: usize,
) -> Vec<Row> {
    let mut rows = vec![Row { values: vec![] }; row_count];
    for column in columns {
        for (row_index, row) in rows.iter_mut().enumerate() {
            row.values.push(to_grpc_value(column.get(row_index)))
        }
    }

    rows
}

pub fn to_grpc_value(value: Value) -> GrpcValue {
    GrpcValue {
        value_data: match value {
            Value::Null => None,
            Value::Boolean(v) => Some(ValueData::BoolValue(v)),
            Value::UInt8(v) => Some(ValueData::U8Value(v as _)),
            Value::UInt16(v) => Some(ValueData::U16Value(v as _)),
            Value::UInt32(v) => Some(ValueData::U32Value(v)),
            Value::UInt64(v) => Some(ValueData::U64Value(v)),
            Value::Int8(v) => Some(ValueData::I8Value(v as _)),
            Value::Int16(v) => Some(ValueData::I16Value(v as _)),
            Value::Int32(v) => Some(ValueData::I32Value(v)),
            Value::Int64(v) => Some(ValueData::I64Value(v)),
            Value::Float32(v) => Some(ValueData::F32Value(*v)),
            Value::Float64(v) => Some(ValueData::F64Value(*v)),
            Value::String(v) => Some(ValueData::StringValue(v.into_string())),
            Value::Binary(v) => Some(ValueData::BinaryValue(v.to_vec())),
            Value::Date(v) => Some(ValueData::DateValue(v.val())),
            Value::Timestamp(v) => Some(match v.unit() {
                TimeUnit::Second => ValueData::TimestampSecondValue(v.value()),
                TimeUnit::Millisecond => ValueData::TimestampMillisecondValue(v.value()),
                TimeUnit::Microsecond => ValueData::TimestampMicrosecondValue(v.value()),
                TimeUnit::Nanosecond => ValueData::TimestampNanosecondValue(v.value()),
            }),
            Value::Time(v) => Some(match v.unit() {
                TimeUnit::Second => ValueData::TimeSecondValue(v.value()),
                TimeUnit::Millisecond => ValueData::TimeMillisecondValue(v.value()),
                TimeUnit::Microsecond => ValueData::TimeMicrosecondValue(v.value()),
                TimeUnit::Nanosecond => ValueData::TimeNanosecondValue(v.value()),
            }),
            Value::IntervalYearMonth(v) => Some(ValueData::IntervalYearMonthValue(v.to_i32())),
            Value::IntervalDayTime(v) => Some(ValueData::IntervalDayTimeValue(v.to_i64())),
            Value::IntervalMonthDayNano(v) => Some(ValueData::IntervalMonthDayNanoValue(
                convert_month_day_nano_to_pb(v),
            )),
            Value::Decimal128(v) => Some(ValueData::Decimal128Value(convert_to_pb_decimal128(v))),
            Value::List(list_value) => {
                let items = list_value
                    .take_items()
                    .into_iter()
                    .map(to_grpc_value)
                    .collect();
                Some(ValueData::ListValue(v1::ListValue { items }))
            }
            Value::Struct(struct_value) => {
                let items = struct_value
                    .take_items()
                    .into_iter()
                    .map(to_grpc_value)
                    .collect();
                Some(ValueData::StructValue(v1::StructValue { items }))
            }
            Value::Json(v) => Some(ValueData::JsonValue(encode_json_value(*v))),
            Value::Duration(_) => unreachable!(),
        },
    }
}

pub fn from_pb_time_unit(unit: v1::TimeUnit) -> TimeUnit {
    match unit {
        v1::TimeUnit::Second => TimeUnit::Second,
        v1::TimeUnit::Millisecond => TimeUnit::Millisecond,
        v1::TimeUnit::Microsecond => TimeUnit::Microsecond,
        v1::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

pub fn to_pb_time_unit(unit: TimeUnit) -> v1::TimeUnit {
    match unit {
        TimeUnit::Second => v1::TimeUnit::Second,
        TimeUnit::Millisecond => v1::TimeUnit::Millisecond,
        TimeUnit::Microsecond => v1::TimeUnit::Microsecond,
        TimeUnit::Nanosecond => v1::TimeUnit::Nanosecond,
    }
}

pub fn from_pb_time_ranges(time_ranges: v1::TimeRanges) -> Result<Vec<(Timestamp, Timestamp)>> {
    if time_ranges.time_ranges.is_empty() {
        return Ok(vec![]);
    }
    let proto_time_unit = v1::TimeUnit::try_from(time_ranges.time_unit).map_err(|_| {
        InvalidTimeUnitSnafu {
            time_unit: time_ranges.time_unit,
        }
        .build()
    })?;
    let time_unit = from_pb_time_unit(proto_time_unit);
    Ok(time_ranges
        .time_ranges
        .into_iter()
        .map(|r| {
            (
                Timestamp::new(r.start, time_unit),
                Timestamp::new(r.end, time_unit),
            )
        })
        .collect())
}

/// All time_ranges must be of the same time unit.
///
/// if input `time_ranges` is empty, it will return a default `TimeRanges` with `Millisecond` as the time unit.
pub fn to_pb_time_ranges(time_ranges: &[(Timestamp, Timestamp)]) -> Result<v1::TimeRanges> {
    let is_same_time_unit = time_ranges.windows(2).all(|x| {
        x[0].0.unit() == x[1].0.unit()
            && x[0].1.unit() == x[1].1.unit()
            && x[0].0.unit() == x[0].1.unit()
    });

    if !is_same_time_unit {
        let all_time_units: Vec<_> = time_ranges
            .iter()
            .map(|(s, e)| [s.unit(), e.unit()])
            .clone()
            .flatten()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        InconsistentTimeUnitSnafu {
            units: all_time_units,
        }
        .fail()?
    }

    let mut pb_time_ranges = v1::TimeRanges {
        // default time unit is Millisecond
        time_unit: v1::TimeUnit::Millisecond as i32,
        time_ranges: Vec::with_capacity(time_ranges.len()),
    };
    if let Some((start, _end)) = time_ranges.first() {
        pb_time_ranges.time_unit = to_pb_time_unit(start.unit()) as i32;
    }
    for (start, end) in time_ranges {
        pb_time_ranges.time_ranges.push(v1::TimeRange {
            start: start.value(),
            end: end.value(),
        });
    }
    Ok(pb_time_ranges)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::interval::IntervalUnit;
    use datatypes::scalars::ScalarVector;
    use datatypes::types::{Int8Type, Int32Type, UInt8Type, UInt32Type};
    use datatypes::value::{ListValue, StructValue};
    use datatypes::vectors::{
        BooleanVector, DateVector, Float32Vector, PrimitiveVector, StringVector,
    };

    use super::*;
    use crate::v1::Column;

    #[test]
    fn test_values_with_capacity() {
        let values = values_with_capacity(ColumnDataType::Int8, 2);
        let values = values.i8_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Int32, 2);
        let values = values.i32_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Int64, 2);
        let values = values.i64_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Uint8, 2);
        let values = values.u8_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Uint32, 2);
        let values = values.u32_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Uint64, 2);
        let values = values.u64_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Float32, 2);
        let values = values.f32_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Float64, 2);
        let values = values.f64_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Binary, 2);
        let values = values.binary_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Boolean, 2);
        let values = values.bool_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::String, 2);
        let values = values.string_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Date, 2);
        let values = values.date_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Datetime, 2);
        let values = values.datetime_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::TimestampMillisecond, 2);
        let values = values.timestamp_millisecond_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::TimeMillisecond, 2);
        let values = values.time_millisecond_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::IntervalDayTime, 2);
        let values = values.interval_day_time_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::IntervalMonthDayNano, 2);
        let values = values.interval_month_day_nano_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Decimal128, 2);
        let values = values.decimal128_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Vector, 2);
        let values = values.binary_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::List, 2);
        let values = values.list_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Struct, 2);
        let values = values.struct_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Json, 2);
        assert_eq!(2, values.json_values.capacity());
        assert_eq!(2, values.string_values.capacity());

        let values = values_with_capacity(ColumnDataType::Dictionary, 2);
        assert!(values.bool_values.is_empty());
    }

    #[test]
    fn test_concrete_datatype_from_column_datatype() {
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            ColumnDataTypeWrapper::boolean_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int8_datatype(),
            ColumnDataTypeWrapper::int8_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int16_datatype(),
            ColumnDataTypeWrapper::int16_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int32_datatype(),
            ColumnDataTypeWrapper::int32_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            ColumnDataTypeWrapper::int64_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint8_datatype(),
            ColumnDataTypeWrapper::uint8_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint16_datatype(),
            ColumnDataTypeWrapper::uint16_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint32_datatype(),
            ColumnDataTypeWrapper::uint32_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            ColumnDataTypeWrapper::uint64_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::float32_datatype(),
            ColumnDataTypeWrapper::float32_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ColumnDataTypeWrapper::float64_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::binary_datatype(),
            ColumnDataTypeWrapper::binary_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::string_datatype(),
            ColumnDataTypeWrapper::string_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::date_datatype(),
            ColumnDataTypeWrapper::date_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::timestamp_microsecond_datatype(),
            ColumnDataTypeWrapper::datetime_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            ColumnDataTypeWrapper::timestamp_millisecond_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::time_datatype(TimeUnit::Millisecond),
            ColumnDataTypeWrapper::time_millisecond_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::DayTime),
            ColumnDataTypeWrapper::interval_day_time_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::YearMonth),
            ColumnDataTypeWrapper::interval_year_month_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ColumnDataTypeWrapper::interval_month_day_nano_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::decimal128_datatype(10, 2),
            ColumnDataTypeWrapper::decimal128_datatype(10, 2).into()
        );
        assert_eq!(
            ConcreteDataType::vector_datatype(3),
            ColumnDataTypeWrapper::vector_datatype(3).into()
        );
        assert_eq!(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::string_datatype())),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::string_datatype()).into()
        );
        assert_eq!(
            ConcreteDataType::dictionary_datatype(
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::string_datatype()
            ),
            ColumnDataTypeWrapper::dictionary_datatype(
                ColumnDataTypeWrapper::int32_datatype(),
                ColumnDataTypeWrapper::string_datatype()
            )
            .into()
        );
        let struct_type = StructType::new(Arc::new(vec![
            StructField::new("id".to_string(), ConcreteDataType::int64_datatype(), true),
            StructField::new(
                "name".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            StructField::new("age".to_string(), ConcreteDataType::int32_datatype(), true),
            StructField::new(
                "address".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
        ]));
        assert_eq!(
            ConcreteDataType::struct_datatype(struct_type.clone()),
            ColumnDataTypeWrapper::struct_datatype(vec![
                ("id".to_string(), ColumnDataTypeWrapper::int64_datatype()),
                ("name".to_string(), ColumnDataTypeWrapper::string_datatype()),
                ("age".to_string(), ColumnDataTypeWrapper::int32_datatype()),
                (
                    "address".to_string(),
                    ColumnDataTypeWrapper::string_datatype()
                )
            ])
            .into()
        );
        assert_eq!(
            ConcreteDataType::json_native_datatype(ConcreteDataType::struct_datatype(
                struct_type.clone()
            )),
            ColumnDataTypeWrapper::new(
                ColumnDataType::Json,
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::JsonNativeType(Box::new(JsonNativeTypeExtension {
                        datatype: ColumnDataType::Struct.into(),
                        datatype_extension: Some(Box::new(ColumnDataTypeExtension {
                            type_ext: Some(TypeExt::StructType(StructTypeExtension {
                                fields: vec![
                                    v1::StructField {
                                        name: "id".to_string(),
                                        datatype: ColumnDataTypeWrapper::int64_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    },
                                    v1::StructField {
                                        name: "name".to_string(),
                                        datatype: ColumnDataTypeWrapper::string_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    },
                                    v1::StructField {
                                        name: "age".to_string(),
                                        datatype: ColumnDataTypeWrapper::int32_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    },
                                    v1::StructField {
                                        name: "address".to_string(),
                                        datatype: ColumnDataTypeWrapper::string_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    }
                                ]
                            }))
                        }))
                    })))
                })
            )
            .into()
        )
    }

    #[test]
    fn test_column_datatype_from_concrete_datatype() {
        assert_eq!(
            ColumnDataTypeWrapper::boolean_datatype(),
            ConcreteDataType::boolean_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int8_datatype(),
            ConcreteDataType::int8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int16_datatype(),
            ConcreteDataType::int16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int32_datatype(),
            ConcreteDataType::int32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int64_datatype(),
            ConcreteDataType::int64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint8_datatype(),
            ConcreteDataType::uint8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint16_datatype(),
            ConcreteDataType::uint16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint32_datatype(),
            ConcreteDataType::uint32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint64_datatype(),
            ConcreteDataType::uint64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::float32_datatype(),
            ConcreteDataType::float32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::float64_datatype(),
            ConcreteDataType::float64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::binary_datatype(),
            ConcreteDataType::binary_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::string_datatype(),
            ConcreteDataType::string_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::date_datatype(),
            ConcreteDataType::date_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::timestamp_millisecond_datatype(),
            ConcreteDataType::timestamp_millisecond_datatype()
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::interval_year_month_datatype(),
            ConcreteDataType::interval_datatype(IntervalUnit::YearMonth)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::interval_day_time_datatype(),
            ConcreteDataType::interval_datatype(IntervalUnit::DayTime)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::interval_month_day_nano_datatype(),
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano)
                .try_into()
                .unwrap()
        );

        assert_eq!(
            ColumnDataTypeWrapper::decimal128_datatype(10, 2),
            ConcreteDataType::decimal128_datatype(10, 2)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::vector_datatype(3),
            ConcreteDataType::vector_datatype(3).try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::dictionary_datatype(
                ColumnDataTypeWrapper::int32_datatype(),
                ColumnDataTypeWrapper::string_datatype()
            ),
            ConcreteDataType::dictionary_datatype(
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::string_datatype()
            )
            .try_into()
            .unwrap()
        );

        let result: Result<ColumnDataTypeWrapper> = ConcreteDataType::null_datatype().try_into();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Failed to create column datatype from Null(NullType)"
        );

        assert_eq!(
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::int16_datatype()),
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int16_datatype()))
                .try_into()
                .expect("Failed to create column datatype from List(ListType { item_type: Int16(Int16Type) })")
        );

        assert_eq!(
            ColumnDataTypeWrapper::struct_datatype(vec![
                ("a".to_string(), ColumnDataTypeWrapper::int64_datatype()),
                (
                    "a.a".to_string(),
                    ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::string_datatype())
                )
            ]),
            ConcreteDataType::struct_datatype(StructType::new(Arc::new(vec![
                StructField::new("a".to_string(), ConcreteDataType::int64_datatype(), true),
                StructField::new(
                    "a.a".to_string(),
                    ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::string_datatype())), true
                )
            ]))).try_into().expect("Failed to create column datatype from Struct(StructType { fields: [StructField { name: \"a\", data_type: Int64(Int64Type) }, StructField { name: \"a.a\", data_type: List(ListType { item_type: String(StringType) }) }] })")
        );

        let struct_type = StructType::new(Arc::new(vec![
            StructField::new("id".to_string(), ConcreteDataType::int64_datatype(), true),
            StructField::new(
                "name".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            StructField::new("age".to_string(), ConcreteDataType::int32_datatype(), true),
            StructField::new(
                "address".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
        ]));
        assert_eq!(
            ColumnDataTypeWrapper::new(
                ColumnDataType::Json,
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::JsonNativeType(Box::new(JsonNativeTypeExtension {
                        datatype: ColumnDataType::Struct.into(),
                        datatype_extension: Some(Box::new(ColumnDataTypeExtension {
                            type_ext: Some(TypeExt::StructType(StructTypeExtension {
                                fields: vec![
                                    v1::StructField {
                                        name: "address".to_string(),
                                        datatype: ColumnDataTypeWrapper::string_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    },
                                    v1::StructField {
                                        name: "age".to_string(),
                                        datatype: ColumnDataTypeWrapper::int64_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    },
                                    v1::StructField {
                                        name: "id".to_string(),
                                        datatype: ColumnDataTypeWrapper::int64_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    },
                                    v1::StructField {
                                        name: "name".to_string(),
                                        datatype: ColumnDataTypeWrapper::string_datatype()
                                            .datatype()
                                            .into(),
                                        datatype_extension: None
                                    },
                                ]
                            }))
                        }))
                    })))
                })
            ),
            ConcreteDataType::json_native_datatype(ConcreteDataType::struct_datatype(
                struct_type.clone()
            ))
            .try_into()
            .expect("failed to convert json type")
        );
    }

    #[test]
    fn test_convert_i128_to_interval() {
        let i128_val = 3;
        let interval = convert_month_day_nano_to_pb(IntervalMonthDayNano::from_i128(i128_val));
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.nanoseconds, 3);
    }

    #[test]
    fn test_vectors_to_rows_for_different_types() {
        let boolean_vec = BooleanVector::from_vec(vec![true, false, true]);
        let int8_vec = PrimitiveVector::<Int8Type>::from_iter_values(vec![1, 2, 3]);
        let int32_vec = PrimitiveVector::<Int32Type>::from_iter_values(vec![100, 200, 300]);
        let uint8_vec = PrimitiveVector::<UInt8Type>::from_iter_values(vec![10, 20, 30]);
        let uint32_vec = PrimitiveVector::<UInt32Type>::from_iter_values(vec![1000, 2000, 3000]);
        let float32_vec = Float32Vector::from_vec(vec![1.1, 2.2, 3.3]);
        let date_vec = DateVector::from_vec(vec![10, 20, 30]);
        let string_vec = StringVector::from_vec(vec!["a", "b", "c"]);

        let vector_refs: Vec<VectorRef> = vec![
            Arc::new(boolean_vec),
            Arc::new(int8_vec),
            Arc::new(int32_vec),
            Arc::new(uint8_vec),
            Arc::new(uint32_vec),
            Arc::new(float32_vec),
            Arc::new(date_vec),
            Arc::new(string_vec),
        ];

        let result = vectors_to_rows(vector_refs.iter(), 3);

        assert_eq!(result.len(), 3);

        assert_eq!(result[0].values.len(), 8);
        let values = result[0]
            .values
            .iter()
            .map(|v| v.value_data.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values[0], ValueData::BoolValue(true));
        assert_eq!(values[1], ValueData::I8Value(1));
        assert_eq!(values[2], ValueData::I32Value(100));
        assert_eq!(values[3], ValueData::U8Value(10));
        assert_eq!(values[4], ValueData::U32Value(1000));
        assert_eq!(values[5], ValueData::F32Value(1.1));
        assert_eq!(values[6], ValueData::DateValue(10));
        assert_eq!(values[7], ValueData::StringValue("a".to_string()));

        assert_eq!(result[1].values.len(), 8);
        let values = result[1]
            .values
            .iter()
            .map(|v| v.value_data.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values[0], ValueData::BoolValue(false));
        assert_eq!(values[1], ValueData::I8Value(2));
        assert_eq!(values[2], ValueData::I32Value(200));
        assert_eq!(values[3], ValueData::U8Value(20));
        assert_eq!(values[4], ValueData::U32Value(2000));
        assert_eq!(values[5], ValueData::F32Value(2.2));
        assert_eq!(values[6], ValueData::DateValue(20));
        assert_eq!(values[7], ValueData::StringValue("b".to_string()));

        assert_eq!(result[2].values.len(), 8);
        let values = result[2]
            .values
            .iter()
            .map(|v| v.value_data.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values[0], ValueData::BoolValue(true));
        assert_eq!(values[1], ValueData::I8Value(3));
        assert_eq!(values[2], ValueData::I32Value(300));
        assert_eq!(values[3], ValueData::U8Value(30));
        assert_eq!(values[4], ValueData::U32Value(3000));
        assert_eq!(values[5], ValueData::F32Value(3.3));
        assert_eq!(values[6], ValueData::DateValue(30));
        assert_eq!(values[7], ValueData::StringValue("c".to_string()));
    }

    #[test]
    fn test_is_column_type_value_eq() {
        // test column type eq
        let column1 = Column {
            column_name: "test".to_string(),
            semantic_type: 0,
            values: Some(Values {
                bool_values: vec![false, true, true],
                ..Default::default()
            }),
            null_mask: vec![2],
            datatype: ColumnDataType::Boolean as i32,
            datatype_extension: None,
            options: None,
        };
        assert!(is_column_type_value_eq(
            column1.datatype,
            column1.datatype_extension,
            &ConcreteDataType::boolean_datatype(),
        ));
    }

    #[test]
    fn test_convert_to_pb_decimal128() {
        let decimal = Decimal128::new(123, 3, 1);
        let pb_decimal = convert_to_pb_decimal128(decimal);
        assert_eq!(pb_decimal.lo, 123);
        assert_eq!(pb_decimal.hi, 0);
    }

    #[test]
    fn test_list_to_pb_value() {
        let value = Value::List(ListValue::new(
            vec![Value::Boolean(true)],
            Arc::new(ConcreteDataType::boolean_datatype()),
        ));

        let pb_value = to_grpc_value(value);

        match pb_value.value_data.unwrap() {
            ValueData::ListValue(pb_list_value) => {
                assert_eq!(pb_list_value.items.len(), 1);
            }
            _ => panic!("Unexpected value type"),
        }
    }

    #[test]
    fn test_struct_to_pb_value() {
        let items = vec![Value::Boolean(true), Value::String("tom".into())];

        let value = Value::Struct(
            StructValue::try_new(
                items,
                StructType::new(Arc::new(vec![
                    StructField::new(
                        "a.a".to_string(),
                        ConcreteDataType::boolean_datatype(),
                        true,
                    ),
                    StructField::new("a.b".to_string(), ConcreteDataType::string_datatype(), true),
                ])),
            )
            .unwrap(),
        );

        let pb_value = to_grpc_value(value);

        match pb_value.value_data.unwrap() {
            ValueData::StructValue(pb_struct_value) => {
                assert_eq!(pb_struct_value.items.len(), 2);
            }
            _ => panic!("Unexpected value type"),
        }
    }

    #[test]
    fn test_encode_decode_json_value() {
        let json = JsonValue::null();
        let proto = encode_json_value(json.clone());
        assert!(proto.value.is_none());
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = true.into();
        let proto = encode_json_value(json.clone());
        assert_eq!(proto.value, Some(json_value::Value::Boolean(true)));
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = (-1i64).into();
        let proto = encode_json_value(json.clone());
        assert_eq!(proto.value, Some(json_value::Value::Int(-1)));
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = 1u64.into();
        let proto = encode_json_value(json.clone());
        assert_eq!(proto.value, Some(json_value::Value::Uint(1)));
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = 1.0f64.into();
        let proto = encode_json_value(json.clone());
        assert_eq!(proto.value, Some(json_value::Value::Float(1.0)));
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = "s".into();
        let proto = encode_json_value(json.clone());
        assert_eq!(proto.value, Some(json_value::Value::Str("s".to_string())));
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = [1i64, 2, 3].into();
        let proto = encode_json_value(json.clone());
        assert_eq!(
            proto.value,
            Some(json_value::Value::Array(JsonList {
                items: vec![
                    v1::JsonValue {
                        value: Some(json_value::Value::Int(1))
                    },
                    v1::JsonValue {
                        value: Some(json_value::Value::Int(2))
                    },
                    v1::JsonValue {
                        value: Some(json_value::Value::Int(3))
                    }
                ]
            }))
        );
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = [(); 0].into();
        let proto = encode_json_value(json.clone());
        assert_eq!(
            proto.value,
            Some(json_value::Value::Array(JsonList { items: vec![] }))
        );
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = [("k3", 3i64), ("k2", 2i64), ("k1", 1i64)].into();
        let proto = encode_json_value(json.clone());
        assert_eq!(
            proto.value,
            Some(json_value::Value::Object(JsonObject {
                entries: vec![
                    v1::json_object::Entry {
                        key: "k1".to_string(),
                        value: Some(v1::JsonValue {
                            value: Some(json_value::Value::Int(1))
                        }),
                    },
                    v1::json_object::Entry {
                        key: "k2".to_string(),
                        value: Some(v1::JsonValue {
                            value: Some(json_value::Value::Int(2))
                        }),
                    },
                    v1::json_object::Entry {
                        key: "k3".to_string(),
                        value: Some(v1::JsonValue {
                            value: Some(json_value::Value::Int(3))
                        }),
                    },
                ]
            }))
        );
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = [("null", ()); 0].into();
        let proto = encode_json_value(json.clone());
        assert_eq!(
            proto.value,
            Some(json_value::Value::Object(JsonObject { entries: vec![] }))
        );
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);

        let json: JsonValue = [
            ("null", JsonVariant::from(())),
            ("bool", false.into()),
            ("list", ["hello", "world"].into()),
            (
                "object",
                [
                    ("positive_i", JsonVariant::from(42u64)),
                    ("negative_i", (-42i64).into()),
                    ("nested", [("what", "blah")].into()),
                ]
                .into(),
            ),
        ]
        .into();
        let proto = encode_json_value(json.clone());
        assert_eq!(
            proto.value,
            Some(json_value::Value::Object(JsonObject {
                entries: vec![
                    v1::json_object::Entry {
                        key: "bool".to_string(),
                        value: Some(v1::JsonValue {
                            value: Some(json_value::Value::Boolean(false))
                        }),
                    },
                    v1::json_object::Entry {
                        key: "list".to_string(),
                        value: Some(v1::JsonValue {
                            value: Some(json_value::Value::Array(JsonList {
                                items: vec![
                                    v1::JsonValue {
                                        value: Some(json_value::Value::Str("hello".to_string()))
                                    },
                                    v1::JsonValue {
                                        value: Some(json_value::Value::Str("world".to_string()))
                                    },
                                ]
                            }))
                        }),
                    },
                    v1::json_object::Entry {
                        key: "null".to_string(),
                        value: Some(v1::JsonValue { value: None }),
                    },
                    v1::json_object::Entry {
                        key: "object".to_string(),
                        value: Some(v1::JsonValue {
                            value: Some(json_value::Value::Object(JsonObject {
                                entries: vec![
                                    v1::json_object::Entry {
                                        key: "negative_i".to_string(),
                                        value: Some(v1::JsonValue {
                                            value: Some(json_value::Value::Int(-42))
                                        }),
                                    },
                                    v1::json_object::Entry {
                                        key: "nested".to_string(),
                                        value: Some(v1::JsonValue {
                                            value: Some(json_value::Value::Object(JsonObject {
                                                entries: vec![v1::json_object::Entry {
                                                    key: "what".to_string(),
                                                    value: Some(v1::JsonValue {
                                                        value: Some(json_value::Value::Str(
                                                            "blah".to_string()
                                                        ))
                                                    }),
                                                },]
                                            }))
                                        }),
                                    },
                                    v1::json_object::Entry {
                                        key: "positive_i".to_string(),
                                        value: Some(v1::JsonValue {
                                            value: Some(json_value::Value::Uint(42))
                                        }),
                                    },
                                ]
                            }))
                        }),
                    },
                ]
            }))
        );
        let value = decode_json_value(&proto);
        assert_eq!(json.as_ref(), value);
    }
}
