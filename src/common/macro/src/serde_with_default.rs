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

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Error, Ident, LitStr, Result, Token};

struct DeserializeInput {
    enum_name: Ident,
    variants: Vec<(Ident, LitStr)>,
}

impl Parse for DeserializeInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let enum_name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;

        let mut variants = Vec::new();
        while !input.is_empty() {
            let variant: Ident = input.parse()?;
            input.parse::<Token![=>]>()?;
            let string_literal: LitStr = input.parse()?;
            variants.push((variant, string_literal));

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            } else {
                break;
            }
        }

        if variants.is_empty() {
            return Err(Error::new(
                Span::call_site(),
                "At least one variant must be specified",
            ));
        }

        Ok(DeserializeInput {
            enum_name,
            variants,
        })
    }
}

pub fn impl_deserialize_with_empty_default(input: TokenStream) -> TokenStream {
    let DeserializeInput {
        enum_name,
        variants,
    } = parse_macro_input!(input as DeserializeInput);

    let variant_matches = variants.iter().map(|(variant, string_literal)| {
        quote! {
            #string_literal => Ok(#enum_name::#variant),
        }
    });

    let variant_strings: Vec<_> = variants.iter().map(|(_, s)| s).collect();

    let expanded = quote! {
        impl<'de> serde::Deserialize<'de> for #enum_name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                match s.as_str() {
                    "" => Ok(#enum_name::default()),
                    #(#variant_matches)*
                    _ => Err(serde::de::Error::unknown_variant(
                        &s,
                        &[#(#variant_strings),*],
                    )),
                }
            }
        }
    };

    TokenStream::from(expanded)
}
