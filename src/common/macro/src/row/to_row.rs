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

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DeriveInput, Result};

use crate::row::utils::{
    convert_column_data_type_to_value_data_ident, extract_struct_fields, get_column_data_type,
    parse_fields_from_fields_named, ParsedField,
};
use crate::row::{META_KEY_COL, META_KEY_DATATYPE};

pub(crate) fn derive_to_row_impl(input: DeriveInput) -> Result<TokenStream2> {
    let Some(fields) = extract_struct_fields(&input.data) else {
        return Err(syn::Error::new(
            input.span(),
            "ToRow can only be derived for structs",
        ));
    };
    let ident = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let fields = parse_fields_from_fields_named(fields)?;

    // Implement `to_row` method.
    let impl_to_row_method = impl_to_row_method_combined(&fields)?;
    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #impl_to_row_method
        }
    })
}

fn impl_to_row_method_combined(fields: &[ParsedField<'_>]) -> Result<TokenStream2> {
    let value_exprs = fields
        .iter()
        .map(|field| {
            let ParsedField {ident, field_type, column_data_type, column_attribute} = field;
            let Some(column_data_type) = get_column_data_type(column_data_type, column_attribute)
            else {
                return Err(syn::Error::new(
                    ident.span(),
                    format!(
                        "expected to set data type explicitly via [({META_KEY_COL}({META_KEY_DATATYPE} = \"...\"))]"
                    ),
                ));
            };
            let value_data = convert_column_data_type_to_value_data_ident(&column_data_type.data_type);
            let expr = if field_type.is_optional() {
                quote! {
                    match &self.#ident {
                        Some(v) => Value {
                            value_data: Some(ValueData::#value_data(v.clone().into())),
                        },
                        None => Value { value_data: None },
                    }
                }
            } else {
                quote! {
                    Value {
                        value_data: Some(ValueData::#value_data(self.#ident.clone().into())),
                    }
                }
            };
            Ok(expr)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        pub fn to_row(&self) -> Row {
            Row {
                values: vec![ #( #value_exprs ),* ]
            }
        }
    })
}
