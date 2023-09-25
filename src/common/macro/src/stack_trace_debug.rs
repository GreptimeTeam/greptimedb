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

//! implement `::common_error::ext::StackError`

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn2::spanned::Spanned;
use syn2::{parenthesized, Ident, ItemEnum, Variant};

pub fn stack_trace_style_impl(args: TokenStream2, input: TokenStream2) -> TokenStream2 {
    let input_cloned: TokenStream2 = input.clone();

    let error_enum_definition: ItemEnum = syn2::parse2(input_cloned).unwrap();
    let enum_name = error_enum_definition.ident;

    let mut variants = vec![];

    for error_variant in error_enum_definition.variants {
        let variant = ErrorVariant::from_enum_variant(error_variant);
        variants.push(variant);
    }

    let debug_fmt_fn = build_debug_fmt_impl(enum_name.clone(), variants.clone());
    let next_fn = build_next_impl(enum_name.clone(), variants);
    let debug_impl = build_debug_impl(enum_name.clone());

    quote! {
        #args
        #input

        impl ::common_error::ext::StackError for #enum_name {
            #debug_fmt_fn
            #next_fn
        }

        #debug_impl
    }
    .into()
}

/// Generate `debug_fmt` fn.
///
/// The generated fn will be like:
/// ```rust, ignore
/// fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>);
/// ```
fn build_debug_fmt_impl(enum_name: Ident, variants: Vec<ErrorVariant>) -> TokenStream2 {
    let match_arms = variants
        .iter()
        .map(|v| v.to_debug_match_arm())
        .collect::<Vec<_>>();

    quote! {
        fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
            use #enum_name::*;
            match self {
                #(#match_arms)*
            }
        }
    }
}

/// Generate `next` fn.
///
/// The generated fn will be like:
/// ```rust, ignore
/// fn next(&self) -> Option<&dyn ::common_error::ext::StackError>;
/// ```
fn build_next_impl(enum_name: Ident, variants: Vec<ErrorVariant>) -> TokenStream2 {
    let match_arms = variants
        .iter()
        .map(|v| v.to_next_match_arm())
        .collect::<Vec<_>>();

    quote! {
        fn next(&self) -> Option<&dyn ::common_error::ext::StackError> {
            use #enum_name::*;
            match self {
                #(#match_arms)*
            }
        }
    }
}

/// Implement [std::fmt::Debug] via `debug_fmt`
fn build_debug_impl(enum_name: Ident) -> TokenStream2 {
    quote! {
        impl std::fmt::Debug for #enum_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                use ::common_error::ext::StackError;
                let mut buf = vec![];
                self.debug_fmt(0, &mut buf);
                write!(f, "{}", buf.join("\n"))
            }
        }
    }
}

#[derive(Clone, Debug)]
struct ErrorVariant {
    name: Ident,
    fields: Vec<Ident>,
    has_location: bool,
    has_source: bool,
    display: TokenStream2,
}

impl ErrorVariant {
    /// Construct self from [Variant]
    fn from_enum_variant(variant: Variant) -> Self {
        let mut has_location = false;
        let mut has_source = false;

        for field in &variant.fields {
            if let Some(ident) = &field.ident {
                if ident == "location" {
                    has_location = true;
                } else if ident == "source" {
                    has_source = true;
                }
            }
        }

        let mut display = None;
        for attr in variant.attrs {
            eprintln!("attribute: {:?}", attr);
            if attr.path().is_ident("snafu") {
                // display = Some(attr.parse_args::<Punctuated<Expr, Comma>>().unwrap());
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("display") {
                        let content;
                        parenthesized!(content in meta.input);
                        let display_ts: TokenStream2 = content.parse()?;
                        display = Some(display_ts);
                        Ok(())
                    } else {
                        Err(meta.error("unrecognized repr"))
                    }
                })
                .unwrap(); // TODO: report error?
            }
            // if attr.path().is_ident("snafu") {
            //     if let Meta::List(meta_list) = attr.meta {
            //         if meta_list.path.is_ident("display") {
            //             // let tokens = meta_list.tokens.into();
            //             // display = Some(parse_macro_input!(tokens as Punctuated<Expr, Comma>));
            //             display = Some(meta_list.tokens);
            //         }
            //     }
            // }
        }

        let field_ident = variant
            .fields
            .iter()
            .map(|f| f.ident.clone().unwrap_or_else(|| Ident::new("_", f.span())))
            .collect();

        Self {
            name: variant.ident,
            fields: field_ident,
            has_location,
            has_source,
            display: display.unwrap(),
        }
    }

    /// Convert self into an match arm that will be used in [build_debug_impl].
    ///
    /// The generated match arm will be like:
    /// ```rust, ignore
    ///     ErrorKindWithSource { source, .. } => {
    ///         debug_fmt(source, layer + 1, buf);
    ///     },
    ///     ErrorKindWithoutSource { .. } => {
    ///        buf.push(format!("{layer}: {}, at {}", format!(#display), location)));
    ///     }
    /// ```
    ///
    /// The generated code assumes fn `debug_fmt`, var `layer`, var `buf` are in scope.
    fn to_debug_match_arm(&self) -> TokenStream2 {
        let name = &self.name;
        let fields = &self.fields;
        let display = &self.display;

        match (self.has_location, self.has_source) {
            (true, true) => quote! {
                #name { #(#fields),*, } => {
                    buf.push(format!("{layer}: {}, at {}", format!(#display), location));
                    source.debug_fmt(layer + 1, buf);
                },
            },
            (true, false) => quote! {
                #name { #(#fields),* } => {
                    buf.push(format!("{layer}: {}, at {}", format!(#display), location));
                },
            },
            (false, true) => quote! {
                #name { #(#fields),* } => {
                    buf.push(format!("{layer}: {}", format!(#display)));
                    source.debug_fmt(layer + 1, buf);
                },
            },
            (false, false) => quote! {
                #name { #(#fields),* } => {
                    buf.push(format!("{layer}: {}", format!(#display)));
                },
            },
        }
    }

    fn to_next_match_arm(&self) -> TokenStream2 {
        let name = &self.name;
        let fields = &self.fields;

        if self.has_source {
            quote! {
                #name { #(#fields),* } => {
                    Some(source)
                },
            }
        } else {
            quote! {
                #name { #(#fields),* } =>{
                    None
                }
            }
        }
    }
}
