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

use greptime_proto::v1::ColumnDataType;
use once_cell::sync::Lazy;
use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{
    AngleBracketedGenericArguments, Attribute, Data, DataStruct, DeriveInput, Fields, FieldsNamed,
    GenericArgument, LitStr, Meta, Path, PathArguments, PathSegment, Result, Type, TypePath,
    TypeReference,
};

const META_KEY_COL: &str = "col";
const META_KEY_NAME: &str = "name";
const META_KEY_DATATYPE: &str = "datatype";
const META_KEY_SEMANTIC: &str = "semantic";

pub(crate) fn derive_to_row_impl(input: DeriveInput) -> Result<TokenStream2> {
    let Data::Struct(DataStruct {
        fields: Fields::Named(FieldsNamed { named, .. }),
        ..
    }) = &input.data
    else {
        return Err(syn::Error::new(
            input.span(),
            "ToRow can only be derived for structs",
        ));
    };

    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = named.iter().map(|field| {
        (
            field.ident.as_ref().expect("field must have an ident"),
            &field.ty,
        )
    });

    // Ident of fields.
    let idents = fields.clone().map(|(ident, _)| ident).collect::<Vec<_>>();
    // Infer column data types for each field.
    let infer_column_data_types = fields
        .clone()
        .map(|(_, ty)| field_type(ty))
        .flat_map(|ty| {
            ty.extract_ident()
                .map(convert_primitive_type_to_column_data_type)
        })
        .collect::<Vec<_>>();

    // Find attributes for each field.
    let column_attributes = named
        .iter()
        .map(|field| {
            let attrs = &field.attrs;
            let attr = find_column_attribute(attrs);
            match attr {
                Some(attr) => parse_attribute(attr),
                None => Ok(ColumnAttribute::default()),
            }
        })
        .collect::<Result<Vec<ColumnAttribute>>>()?;
    let field_types = fields
        .clone()
        .map(|(_, ty)| field_type(ty))
        .collect::<Vec<_>>();

    // Implement `to_row` method.
    let impl_to_row_method = impl_to_row_method_combined(
        &idents,
        &field_types,
        &infer_column_data_types,
        &column_attributes,
    )?;

    // Implement `schema` method.
    let impl_schema_method =
        impl_schema_method(&idents, &infer_column_data_types, &column_attributes)?;

    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #impl_to_row_method

            #impl_schema_method
        }
    })
}

fn impl_schema_method(
    idents: &[&Ident],
    infer_column_data_types: &[Option<ColumnDataType>],
    column_attributes: &[ColumnAttribute],
) -> Result<TokenStream2> {
    let schemas: Vec<TokenStream2> = idents
        .iter()
        .zip(infer_column_data_types.iter())
        .zip(column_attributes.iter())
        .map(|((ident, column_data_type), column_attribute)| {
            let Some(column_data_type) = get_column_data_type(column_data_type, column_attribute)
            else {
                return Err(syn::Error::new(
                    ident.span(),
                    format!(
                        "expected to set data type explicitly via [({META_KEY_COL}({META_KEY_DATATYPE} = \"...\"))]"
                    ),
                ));
            };

            // Uses user explicit name or field name as column name.
            let name = column_attribute
                .name
                .clone()
                .unwrap_or_else(|| ident.to_string());
            let name = syn::LitStr::new(&name, ident.span());
            let column_data_type =
                syn::LitInt::new(&(column_data_type as i32).to_string(), ident.span());
            let semantic_type_val = match column_attribute.semantic_type {
                SemanticType::Field => greptime_proto::v1::SemanticType::Field,
                SemanticType::Tag => greptime_proto::v1::SemanticType::Tag,
                SemanticType::Timestamp => greptime_proto::v1::SemanticType::Timestamp,
            } as i32;
            let semantic_type = syn::LitInt::new(&semantic_type_val.to_string(), ident.span());

            Ok(quote! {
                greptime_proto::v1::ColumnSchema {
                    column_name: #name.to_string(),
                    datatype: #column_data_type,
                    datatype_extension: None,
                    options: None,
                    semantic_type: #semantic_type,
                }
            })
        })
        .collect::<Result<_>>()?;

    Ok(quote! {
        pub fn schema(&self) -> Vec<greptime_proto::v1::ColumnSchema> {
            vec![ #(#schemas),* ]
        }
    })
}

fn impl_to_row_method_combined(
    idents: &[&Ident],
    field_types: &[FieldType<'_>],
    infer_column_data_types: &[Option<ColumnDataType>],
    column_attributes: &[ColumnAttribute],
) -> Result<TokenStream2> {
    let value_exprs = idents
        .iter()
        .zip(field_types.iter())
        .zip(infer_column_data_types.iter())
        .zip(column_attributes.iter())
        .map(|(((ident, field_type), column_data_type), column_attribute)| {
            let Some(column_data_type) = get_column_data_type(column_data_type, column_attribute)
            else {
                return Err(syn::Error::new(
                    ident.span(),
                    format!(
                        "expected to set data type explicitly via [({META_KEY_COL}({META_KEY_DATATYPE} = \"...\"))]"
                    ),
                ));
            };
            let value_data = convert_column_data_type_to_value_data_ident(&column_data_type);
            let expr = if field_type.is_optional() {
                quote! {
                    match &self.#ident {
                        Some(v) => greptime_proto::v1::Value {
                            value_data: Some(greptime_proto::v1::value::ValueData::#value_data(v.clone().into())),
                        },
                        None => greptime_proto::v1::Value { value_data: None },
                    }
                }
            } else {
                quote! {
                    greptime_proto::v1::Value {
                        value_data: Some(greptime_proto::v1::value::ValueData::#value_data(self.#ident.clone().into())),
                    }
                }
            };
            Ok(expr)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        pub fn to_row(&self) -> greptime_proto::v1::Row {
            greptime_proto::v1::Row {
                values: vec![ #( #value_exprs ),* ]
            }
        }
    })
}

fn get_column_data_type(
    infer_column_data_type: &Option<ColumnDataType>,
    attribute: &ColumnAttribute,
) -> Option<ColumnDataType> {
    attribute.column_data_type.or(*infer_column_data_type)
}

fn convert_column_data_type_to_value_data_ident(column_data_type: &ColumnDataType) -> Ident {
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
        ColumnDataType::Json => format_ident!("JsonValue"),
        ColumnDataType::Vector => format_ident!("VectorValue"),
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
    {
        if leading_colon.is_none() && segments.len() == 1 {
            if let Some(PathSegment {
                ident,
                arguments:
                    PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }),
            }) = segments.first()
            {
                if let (1, Some(GenericArgument::Type(t))) = (args.len(), args.first()) {
                    if ident == "Option" {
                        return FieldType::Optional(t);
                    }
                }
            }
        }
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

static DATATYPE_TO_COLUMN_DATA_TYPE: Lazy<HashMap<&'static str, ColumnDataType>> =
    Lazy::new(|| {
        HashMap::from([
            // Timestamp
            ("timestampsecond", ColumnDataType::TimestampSecond),
            ("timestampmillisecond", ColumnDataType::TimestampMillisecond),
            (
                "timestamptimemicrosecond",
                ColumnDataType::TimestampMicrosecond,
            ),
            (
                "timestamptimenanosecond",
                ColumnDataType::TimestampNanosecond,
            ),
            // Date
            ("date", ColumnDataType::Date),
            ("datetime", ColumnDataType::Datetime),
            // Time
            ("timesecond", ColumnDataType::TimeSecond),
            ("timemillisecond", ColumnDataType::TimeMillisecond),
            ("timemicrosecond", ColumnDataType::TimeMicrosecond),
            ("timenanosecond", ColumnDataType::TimeNanosecond),
            // Others
            ("string", ColumnDataType::String),
            ("json", ColumnDataType::Json),
            ("decimal128", ColumnDataType::Decimal128),
            ("vector", ColumnDataType::Vector),
        ])
    });

static SEMANTIC_TYPES: Lazy<HashMap<&'static str, SemanticType>> = Lazy::new(|| {
    HashMap::from([
        ("field", SemanticType::Field),
        ("tag", SemanticType::Tag),
        ("timestamp", SemanticType::Timestamp),
    ])
});

fn convert_primitive_type_to_column_data_type(ident: &Ident) -> Option<ColumnDataType> {
    PRIMITIVE_TYPE_TO_COLUMN_DATA_TYPE
        .get(ident.to_string().as_str())
        .cloned()
}

fn semantic_type_from_ident(ident: &str) -> Option<SemanticType> {
    // Ignores the case of the identifier.
    let lowercase = ident.to_lowercase();
    let lowercase_str = lowercase.as_str();
    SEMANTIC_TYPES.get(lowercase_str).cloned()
}

fn convert_field_type_to_column_data_type(ident: &str) -> Option<ColumnDataType> {
    // Ignores the case of the identifier.
    let lowercase = ident.to_lowercase();
    let lowercase_str = lowercase.as_str();
    DATATYPE_TO_COLUMN_DATA_TYPE.get(lowercase_str).cloned()
}

#[derive(Default, Clone, Copy)]
enum SemanticType {
    #[default]
    Field,
    Tag,
    Timestamp,
}

enum FieldType<'a> {
    Required(&'a Type),
    Optional(&'a Type),
}

impl FieldType<'_> {
    fn is_optional(&self) -> bool {
        matches!(self, FieldType::Optional(_))
    }

    fn extract_ident(&self) -> Option<&Ident> {
        match self {
            FieldType::Required(ty) => extract_ident_from_type(ty),
            FieldType::Optional(ty) => extract_ident_from_type(ty),
        }
    }
}

#[derive(Default)]
struct ColumnAttribute {
    name: Option<String>,
    column_data_type: Option<ColumnDataType>,
    semantic_type: SemanticType,
}

fn find_column_attribute(attrs: &[Attribute]) -> Option<&Attribute> {
    attrs
        .iter()
        .find(|attr| matches!(&attr.meta, Meta::List(list) if list.path.is_ident(META_KEY_COL)))
}

fn parse_attribute(attr: &Attribute) -> Result<ColumnAttribute> {
    match &attr.meta {
        Meta::List(list) if list.path.is_ident(META_KEY_COL) => {
            let mut attribute = ColumnAttribute::default();
            list.parse_nested_meta(|meta| {
                parse_column_attribute(&meta, &mut attribute)
            })?;
            Ok(attribute)
        }
        _ => Err(syn::Error::new(
            attr.span(),
            format!(
                "expected `{META_KEY_COL}({META_KEY_NAME} = \"...\", {META_KEY_DATATYPE} = \"...\", {META_KEY_SEMANTIC} = \"...\")`"
            ),
        )),
    }
}

fn parse_column_attribute(
    meta: &ParseNestedMeta<'_>,
    attribute: &mut ColumnAttribute,
) -> Result<()> {
    let Some(ident) = meta.path.get_ident() else {
        return Err(meta.error(format!("expected `{META_KEY_COL}({META_KEY_NAME} = \"...\", {META_KEY_DATATYPE} = \"...\", {META_KEY_SEMANTIC} = \"...\")`")));
    };

    match ident.to_string().as_str() {
        META_KEY_NAME => {
            let value = meta.value()?;
            let s: LitStr = value.parse()?;
            attribute.name = Some(s.value());
        }
        META_KEY_DATATYPE => {
            let value = meta.value()?;
            let s: LitStr = value.parse()?;
            let ident = s.value();
            let Some(value) = convert_field_type_to_column_data_type(&ident) else {
                return Err(meta.error(format!("unexpected {META_KEY_DATATYPE}: {ident}")));
            };
            attribute.column_data_type = Some(value);
        }
        META_KEY_SEMANTIC => {
            let value = meta.value()?;
            let s: LitStr = value.parse()?;

            let ident = s.value();
            let Some(value) = semantic_type_from_ident(&ident) else {
                return Err(meta.error(format!("unexpected {META_KEY_SEMANTIC}: {ident}")));
            };
            attribute.semantic_type = value;
        }
        attr => return Err(meta.error(format!("unexpected attribute: {attr}"))),
    }

    Ok(())
}
