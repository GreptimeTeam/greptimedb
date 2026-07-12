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

use greptime_proto::v1::ColumnDataTypeExtension;
use greptime_proto::v1::column_data_type_extension::TypeExt;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::spanned::Spanned;
use syn::{DeriveInput, Result};

use crate::row::utils::{
    ColumnDataTypeWithExtension, ParsedField, convert_semantic_type_to_proto_semantic_type,
    extract_struct_fields, get_column_data_type, parse_fields_from_fields_named,
};
use crate::row::{META_KEY_COL, META_KEY_DATATYPE};

pub(crate) fn derive_schema_impl(input: DeriveInput) -> Result<TokenStream2> {
    let Some(fields) = extract_struct_fields(&input.data) else {
        return Err(syn::Error::new(
            input.span(),
            "Schema can only be derived for structs",
        ));
    };
    let ident = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let fields = parse_fields_from_fields_named(fields)?;

    // Implement `schema` method.
    let impl_schema_method = impl_schema_method(&fields)?;
    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #impl_schema_method
        }
    })
}

fn impl_schema_method(fields: &[ParsedField<'_>]) -> Result<TokenStream2> {
    let schemas: Vec<TokenStream2> = fields
        .iter()
        .map(|field| {
            let ParsedField{ ident, column_data_type, column_attribute, ..} = field;
            let Some(ColumnDataTypeWithExtension{data_type, extension}) = get_column_data_type(column_data_type, column_attribute)
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
                syn::LitInt::new(&(data_type as i32).to_string(), ident.span());
            let semantic_type_val = convert_semantic_type_to_proto_semantic_type(column_attribute.semantic_type) as i32;
            let semantic_type = syn::LitInt::new(&semantic_type_val.to_string(), ident.span());
            let extension = match extension {
                Some(ext) => column_data_type_extension_to_tokens(&ext, ident.span()),
                None => quote! { None },
            };

            Ok(quote! {
                ColumnSchema {
                    column_name: #name.to_string(),
                    datatype: #column_data_type,
                    datatype_extension: #extension,
                    options: None,
                    semantic_type: #semantic_type,
                }
            })
        })
        .collect::<Result<_>>()?;

    Ok(quote! {
        pub fn schema() -> Vec<ColumnSchema> {
            vec![ #(#schemas),* ]
        }
    })
}

fn column_data_type_extension_to_tokens(
    extension: &ColumnDataTypeExtension,
    span: Span,
) -> TokenStream2 {
    match extension.type_ext.as_ref() {
        Some(TypeExt::DecimalType(ext)) => {
            let precision = syn::LitInt::new(&ext.precision.to_string(), span);
            let scale = syn::LitInt::new(&ext.scale.to_string(), span);
            quote! {
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                        precision: #precision,
                        scale: #scale,
                    })),
                })
            }
        }
        Some(TypeExt::JsonType(ext)) => {
            let json_type = syn::LitInt::new(&ext.to_string(), span);
            quote! {
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::JsonType(#json_type)),
                })
            }
        }
        Some(TypeExt::VectorType(ext)) => {
            let dim = syn::LitInt::new(&ext.dim.to_string(), span);
            quote! {
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::VectorType(VectorTypeExtension { dim: #dim })),
                })
            }
        }
        Some(TypeExt::ListType(ext)) => {
            let datatype = syn::LitInt::new(&ext.datatype.to_string(), span);
            let datatype_extension = ext
                .datatype_extension
                .as_deref()
                .map(|ext| column_data_type_extension_to_tokens(ext, span))
                .unwrap_or_else(|| quote! { None });
            quote! {
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::ListType(Box::new(ListTypeExtension {
                        datatype: #datatype,
                        datatype_extension: #datatype_extension,
                    }))),
                })
            }
        }
        Some(TypeExt::StructType(ext)) => {
            let fields = ext.fields.iter().map(|field| {
                let field_name = &field.name;
                let datatype = syn::LitInt::new(&field.datatype.to_string(), span);
                let datatype_extension = field
                    .datatype_extension
                    .as_ref()
                    .map(|ext| column_data_type_extension_to_tokens(ext, span))
                    .unwrap_or_else(|| quote! { None });
                quote! {
                    greptime_proto::v1::StructField {
                        name: #field_name.to_string(),
                        datatype: #datatype,
                        datatype_extension: #datatype_extension,
                    }
                }
            });
            quote! {
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::StructType(StructTypeExtension {
                        fields: vec![#(#fields),*],
                    })),
                })
            }
        }
        Some(TypeExt::JsonNativeType(ext)) => {
            let inner = syn::LitInt::new(&ext.datatype.to_string(), span);
            let datatype_extension = ext
                .datatype_extension
                .as_deref()
                .map(|ext| column_data_type_extension_to_tokens(ext, span))
                .unwrap_or_else(|| quote! { None });
            quote! {
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::JsonNativeType(Box::new(
                        JsonNativeTypeExtension {
                            datatype: #inner,
                            datatype_extension: #datatype_extension,
                        },
                    ))),
                })
            }
        }
        Some(TypeExt::DictionaryType(ext)) => {
            let key_datatype = syn::LitInt::new(&ext.key_datatype.to_string(), span);
            let value_datatype = syn::LitInt::new(&ext.value_datatype.to_string(), span);
            let key_datatype_extension = ext
                .key_datatype_extension
                .as_deref()
                .map(|ext| column_data_type_extension_to_tokens(ext, span))
                .unwrap_or_else(|| quote! { None });
            let value_datatype_extension = ext
                .value_datatype_extension
                .as_deref()
                .map(|ext| column_data_type_extension_to_tokens(ext, span))
                .unwrap_or_else(|| quote! { None });
            quote! {
                Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::DictionaryType(Box::new(
                        DictionaryTypeExtension {
                            key_datatype: #key_datatype,
                            key_datatype_extension: #key_datatype_extension,
                            value_datatype: #value_datatype,
                            value_datatype_extension: #value_datatype_extension,
                        },
                    ))),
                })
            }
        }
        None => quote! { None },
    }
}
