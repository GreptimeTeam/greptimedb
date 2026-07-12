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

use std::collections::HashMap;

use greptime_proto::v1::column_data_type_extension::TypeExt;
use greptime_proto::v1::{ColumnDataType, ColumnDataTypeExtension, JsonTypeExtension};
use once_cell::sync::Lazy;
use quote::format_ident;
use syn::{
    AngleBracketedGenericArguments, Data, DataStruct, Fields, FieldsNamed, GenericArgument, Ident,
    Path, PathArguments, PathSegment, Result, Type, TypePath, TypeReference,
};

use crate::row::attribute::{ColumnAttribute, find_column_attribute, parse_column_attribute};

static SEMANTIC_TYPES: Lazy<HashMap<&'static str, SemanticType>> = Lazy::new(|| {
    HashMap::from([
        ("field", SemanticType::Field),
        ("tag", SemanticType::Tag),
        ("timestamp", SemanticType::Timestamp),
    ])
});

static DATATYPE_TO_COLUMN_DATA_TYPE: Lazy<HashMap<&'static str, ColumnDataTypeWithExtension>> =
    Lazy::new(|| {
        HashMap::from([
            // Timestamp
            ("timestampsecond", ColumnDataType::TimestampSecond.into()),
            (
                "timestampmillisecond",
                ColumnDataType::TimestampMillisecond.into(),
            ),
            (
                "timestampmicrosecond",
                ColumnDataType::TimestampMicrosecond.into(),
            ),
            (
                "timestampnanosecond",
                ColumnDataType::TimestampNanosecond.into(),
            ),
            // Date
            ("date", ColumnDataType::Date.into()),
            ("datetime", ColumnDataType::Datetime.into()),
            // Time
            ("timesecond", ColumnDataType::TimeSecond.into()),
            ("timemillisecond", ColumnDataType::TimeMillisecond.into()),
            ("timemicrosecond", ColumnDataType::TimeMicrosecond.into()),
            ("timenanosecond", ColumnDataType::TimeNanosecond.into()),
            // Others
            ("string", ColumnDataType::String.into()),
            ("json", ColumnDataTypeWithExtension::json()),
            // TODO(weny): support vector and decimal128.
        ])
    });

static PRIMITIVE_TYPE_TO_COLUMN_DATA_TYPE: Lazy<HashMap<&'static str, ColumnDataType>> =
    Lazy::new(|| {
        HashMap::from([
            ("i8", ColumnDataType::Int8),
            ("i16", ColumnDataType::Int16),
            ("i32", ColumnDataType::Int32),
            ("i64", ColumnDataType::Int64),
            ("u8", ColumnDataType::Uint8),
            ("u16", ColumnDataType::Uint16),
            ("u32", ColumnDataType::Uint32),
            ("u64", ColumnDataType::Uint64),
            ("f32", ColumnDataType::Float32),
            ("f64", ColumnDataType::Float64),
            ("bool", ColumnDataType::Boolean),
        ])
    });

/// Extract the fields of a struct.
pub(crate) fn extract_struct_fields(data: &Data) -> Option<&FieldsNamed> {
    let Data::Struct(DataStruct {
        fields: Fields::Named(named),
        ..
    }) = &data
    else {
        return None;
    };

    Some(named)
}

/// Convert an identifier to a semantic type.
pub(crate) fn semantic_type_from_str(ident: &str) -> Option<SemanticType> {
    // Ignores the case of the identifier.
    let lowercase = ident.to_lowercase();
    let lowercase_str = lowercase.as_str();
    SEMANTIC_TYPES.get(lowercase_str).cloned()
}

/// Convert a field type to a column data type.
pub(crate) fn column_data_type_from_str(ident: &str) -> Option<ColumnDataTypeWithExtension> {
    // Ignores the case of the identifier.
    let lowercase = ident.to_lowercase();
    let lowercase_str = lowercase.as_str();
    DATATYPE_TO_COLUMN_DATA_TYPE.get(lowercase_str).cloned()
}

#[derive(Default, Clone, Copy)]
pub(crate) enum SemanticType {
    #[default]
    Field,
    Tag,
    Timestamp,
}

pub(crate) enum FieldType<'a> {
    Required(&'a Type),
    Optional(&'a Type),
}

impl FieldType<'_> {
    pub(crate) fn is_optional(&self) -> bool {
        matches!(self, FieldType::Optional(_))
    }

    pub(crate) fn extract_ident(&self) -> Option<&Ident> {
        match self {
            FieldType::Required(ty) => extract_ident_from_type(ty),
            FieldType::Optional(ty) => extract_ident_from_type(ty),
        }
    }
}

fn field_type(ty: &Type) -> FieldType<'_> {
    if let Type::Reference(TypeReference { elem, .. }) = ty {
        return field_type(elem);
    }

    if let Type::Path(TypePath {
        qself: _,
        path: Path {
            leading_colon,
            segments,
        },
    }) = ty
        && leading_colon.is_none()
        && segments.len() == 1
        && let Some(PathSegment {
            ident,
            arguments: PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }),
        }) = segments.first()
        && let (1, Some(GenericArgument::Type(t))) = (args.len(), args.first())
        && ident == "Option"
    {
        return FieldType::Optional(t);
    }

    FieldType::Required(ty)
}

fn extract_ident_from_type(ty: &Type) -> Option<&Ident> {
    match ty {
        Type::Path(TypePath { qself: None, path }) => path.get_ident(),
        Type::Reference(type_ref) => extract_ident_from_type(&type_ref.elem),
        Type::Group(type_group) => extract_ident_from_type(&type_group.elem),
        _ => None,
    }
}

/// Convert a semantic type to a proto semantic type.
pub(crate) fn convert_semantic_type_to_proto_semantic_type(
    semantic_type: SemanticType,
) -> greptime_proto::v1::SemanticType {
    match semantic_type {
        SemanticType::Field => greptime_proto::v1::SemanticType::Field,
        SemanticType::Tag => greptime_proto::v1::SemanticType::Tag,
        SemanticType::Timestamp => greptime_proto::v1::SemanticType::Timestamp,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnDataTypeWithExtension {
    pub(crate) data_type: ColumnDataType,
    pub(crate) extension: Option<ColumnDataTypeExtension>,
}

impl ColumnDataTypeWithExtension {
    pub(crate) fn json() -> Self {
        Self {
            data_type: ColumnDataType::Json,
            extension: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
        }
    }
}

impl From<ColumnDataType> for ColumnDataTypeWithExtension {
    fn from(data_type: ColumnDataType) -> Self {
        Self {
            data_type,
            extension: None,
        }
    }
}

pub(crate) struct ParsedField<'a> {
    pub(crate) ident: &'a Ident,
    pub(crate) field_type: FieldType<'a>,
    pub(crate) column_data_type: Option<ColumnDataTypeWithExtension>,
    pub(crate) column_attribute: ColumnAttribute,
}

/// Parse fields from fields named.
pub(crate) fn parse_fields_from_fields_named(named: &FieldsNamed) -> Result<Vec<ParsedField<'_>>> {
    Ok(named
        .named
        .iter()
        .map(|field| {
            let ident = field.ident.as_ref().expect("field must have an ident");
            let field_type = field_type(&field.ty);
            let column_data_type = field_type
                .extract_ident()
                .and_then(convert_primitive_type_to_column_data_type);
            let column_attribute = find_column_attribute(&field.attrs)
                .map(parse_column_attribute)
                .transpose()?
                .unwrap_or_default();

            Ok(ParsedField {
                ident,
                field_type,
                column_data_type,
                column_attribute,
            })
        })
        .collect::<Result<Vec<ParsedField<'_>>>>()?
        .into_iter()
        .filter(|field| !field.column_attribute.skip)
        .collect::<Vec<_>>())
}

fn convert_primitive_type_to_column_data_type(
    ident: &Ident,
) -> Option<ColumnDataTypeWithExtension> {
    PRIMITIVE_TYPE_TO_COLUMN_DATA_TYPE
        .get(ident.to_string().as_str())
        .cloned()
        .map(ColumnDataTypeWithExtension::from)
}

/// Get the column data type from the attribute or the inferred column data type.
pub(crate) fn get_column_data_type(
    infer_column_data_type: &Option<ColumnDataTypeWithExtension>,
    attribute: &ColumnAttribute,
) -> Option<ColumnDataTypeWithExtension> {
    attribute
        .datatype
        .clone()
        .or_else(|| infer_column_data_type.clone())
}

/// Convert a column data type to a value data ident.
pub(crate) fn convert_column_data_type_to_value_data_ident(
    column_data_type: &ColumnDataType,
) -> Ident {
    match column_data_type {
        ColumnDataType::Boolean => format_ident!("BoolValue"),
        ColumnDataType::Int8 => format_ident!("I8Value"),
        ColumnDataType::Int16 => format_ident!("I16Value"),
        ColumnDataType::Int32 => format_ident!("I32Value"),
        ColumnDataType::Int64 => format_ident!("I64Value"),
        ColumnDataType::Uint8 => format_ident!("U8Value"),
        ColumnDataType::Uint16 => format_ident!("U16Value"),
        ColumnDataType::Uint32 => format_ident!("U32Value"),
        ColumnDataType::Uint64 => format_ident!("U64Value"),
        ColumnDataType::Float32 => format_ident!("F32Value"),
        ColumnDataType::Float64 => format_ident!("F64Value"),
        ColumnDataType::Binary => format_ident!("BinaryValue"),
        ColumnDataType::String => format_ident!("StringValue"),
        ColumnDataType::Date => format_ident!("DateValue"),
        ColumnDataType::Datetime => format_ident!("DatetimeValue"),
        ColumnDataType::TimestampSecond => format_ident!("TimestampSecondValue"),
        ColumnDataType::TimestampMillisecond => {
            format_ident!("TimestampMillisecondValue")
        }
        ColumnDataType::TimestampMicrosecond => {
            format_ident!("TimestampMicrosecondValue")
        }
        ColumnDataType::TimestampNanosecond => format_ident!("TimestampNanosecondValue"),
        ColumnDataType::TimeSecond => format_ident!("TimeSecondValue"),
        ColumnDataType::TimeMillisecond => format_ident!("TimeMillisecondValue"),
        ColumnDataType::TimeMicrosecond => format_ident!("TimeMicrosecondValue"),
        ColumnDataType::TimeNanosecond => format_ident!("TimeNanosecondValue"),
        ColumnDataType::IntervalYearMonth => format_ident!("IntervalYearMonthValue"),
        ColumnDataType::IntervalDayTime => format_ident!("IntervalDayTimeValue"),
        ColumnDataType::IntervalMonthDayNano => {
            format_ident!("IntervalMonthDayNanoValue")
        }
        ColumnDataType::Decimal128 => format_ident!("Decimal128Value"),
        // Json is a special case, it is actually a string column.
        ColumnDataType::Json => format_ident!("StringValue"),
        ColumnDataType::Vector => format_ident!("VectorValue"),
        ColumnDataType::List => format_ident!("ListValue"),
        ColumnDataType::Struct => format_ident!("StructValue"),
        ColumnDataType::Dictionary => {
            panic!("Dictionary data type is not supported in row macros yet")
        }
    }
}
