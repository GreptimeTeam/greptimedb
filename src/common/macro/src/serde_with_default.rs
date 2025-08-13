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
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{
    parse_macro_input, Attribute, Data, DeriveInput, Error, Expr, ExprLit, Lit, Meta,
    MetaNameValue, Token, Variant,
};

/// Implementation for the derive macro that automatically generates the deserialize implementation
pub fn impl_deserialize_with_empty_default_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let enum_name = &input.ident;

    let Data::Enum(data_enum) = &input.data else {
        return Error::new_spanned(
            enum_name,
            "DeserializeWithEmptyDefault can only be used on enums",
        )
        .to_compile_error()
        .into();
    };

    // Extract container-level serde rename_all attribute
    let rename_all = extract_rename_all(&input.attrs);

    let mut variant_data = Vec::new();
    for variant in &data_enum.variants {
        let variant_ident = &variant.ident;
        let variant_str = match get_variant_string(variant, &rename_all) {
            Ok(s) => s,
            Err(e) => {
                return Error::new_spanned(variant, e).to_compile_error().into();
            }
        };
        variant_data.push((variant_ident, variant_str));
    }

    // Generate variant matches
    let variant_matches = variant_data.iter().map(|(variant_ident, variant_str)| {
        quote! {
            #variant_str => Ok(#enum_name::#variant_ident),
        }
    });

    // Generate variant strings for error message
    let variant_strings = variant_data.iter().map(|(_, variant_str)| {
        quote! { #variant_str }
    });

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

/// Extract a string value from serde attributes for a given key
fn extract_serde_string_value(attrs: &[Attribute], key: &str) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("serde") {
            if let Ok(meta_list) =
                attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            {
                for meta in meta_list {
                    if let Meta::NameValue(MetaNameValue { path, value, .. }) = meta {
                        if path.is_ident(key) {
                            if let Expr::Lit(ExprLit {
                                lit: Lit::Str(lit_str),
                                ..
                            }) = value
                            {
                                return Some(lit_str.value());
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

/// Extract the rename_all attribute from container attributes
fn extract_rename_all(attrs: &[Attribute]) -> Option<String> {
    extract_serde_string_value(attrs, "rename_all")
}

/// Get the string representation of a variant, considering serde rename attributes
fn get_variant_string(variant: &Variant, rename_all: &Option<String>) -> Result<String, String> {
    // Check for field-level rename attribute first
    if let Some(renamed) = extract_serde_string_value(&variant.attrs, "rename") {
        return Ok(renamed);
    }

    // Apply container-level rename_all if no field-level rename
    let variant_name = variant.ident.to_string();
    if let Some(rename_style) = rename_all {
        apply_rename_style(&variant_name, rename_style)
    } else {
        Ok(variant_name)
    }
}

/// Apply the rename style to a variant name
fn apply_rename_style(name: &str, style: &str) -> Result<String, String> {
    match style {
        "snake_case" => Ok(to_snake_case(name)),
        "kebab-case" => Ok(to_kebab_case(name)),
        "camelCase" => Ok(to_camel_case(name)),
        "PascalCase" => Ok(name.to_string()), // Already in PascalCase
        "SCREAMING_SNAKE_CASE" => Ok(to_screaming_snake_case(name)),
        "lowercase" => Ok(name.to_lowercase()),
        "UPPERCASE" => Ok(name.to_uppercase()),
        unknown => Err(format!(
            "Unknown serde rename style: '{}'. Supported styles: snake_case, kebab-case, camelCase, PascalCase, SCREAMING_SNAKE_CASE, lowercase, UPPERCASE",
            unknown
        )),
    }
}

/// Convert PascalCase to snake_case
fn to_snake_case(name: &str) -> String {
    let mut result = String::new();
    let mut chars = name.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch.is_uppercase() && !result.is_empty() {
            // Look ahead to see if next char is lowercase (camelCase pattern)
            if let Some(&next_ch) = chars.peek() {
                if next_ch.is_lowercase() || result.chars().last().is_some_and(|c| c.is_lowercase())
                {
                    result.push('_');
                }
            }
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }

    result
}

/// Convert PascalCase to kebab-case
fn to_kebab_case(name: &str) -> String {
    to_snake_case(name).replace('_', "-")
}

/// Convert PascalCase to camelCase
fn to_camel_case(name: &str) -> String {
    let mut chars = name.chars();
    if let Some(first) = chars.next() {
        first.to_lowercase().collect::<String>() + &chars.collect::<String>()
    } else {
        String::new()
    }
}

/// Convert PascalCase to SCREAMING_SNAKE_CASE
fn to_screaming_snake_case(name: &str) -> String {
    to_snake_case(name).to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("Json"), "json");
        assert_eq!(to_snake_case("Text"), "text");
        assert_eq!(to_snake_case("LogFormat"), "log_format");
        assert_eq!(to_snake_case("HTTPResponse"), "h_t_t_p_response");
        assert_eq!(to_snake_case("XMLParser"), "x_m_l_parser");
        assert_eq!(to_snake_case("SimpleCase"), "simple_case");
        assert_eq!(to_snake_case("IOHandler"), "i_o_handler");
        assert_eq!(to_snake_case("APIKey"), "a_p_i_key");
        assert_eq!(to_snake_case("HTMLElement"), "h_t_m_l_element");
    }

    #[test]
    fn test_to_kebab_case() {
        assert_eq!(to_kebab_case("Json"), "json");
        assert_eq!(to_kebab_case("LogFormat"), "log-format");
        assert_eq!(to_kebab_case("SimpleCase"), "simple-case");
    }

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("Json"), "json");
        assert_eq!(to_camel_case("LogFormat"), "logFormat");
        assert_eq!(to_camel_case("SimpleCase"), "simpleCase");
    }

    #[test]
    fn test_to_screaming_snake_case() {
        assert_eq!(to_screaming_snake_case("Json"), "JSON");
        assert_eq!(to_screaming_snake_case("LogFormat"), "LOG_FORMAT");
        assert_eq!(to_screaming_snake_case("SimpleCase"), "SIMPLE_CASE");
    }

    #[test]
    fn test_apply_rename_style() {
        assert_eq!(
            apply_rename_style("Json", "snake_case"),
            Ok("json".to_string())
        );
        assert_eq!(
            apply_rename_style("LogFormat", "snake_case"),
            Ok("log_format".to_string())
        );
        assert_eq!(
            apply_rename_style("Json", "kebab-case"),
            Ok("json".to_string())
        );
        assert_eq!(
            apply_rename_style("LogFormat", "kebab-case"),
            Ok("log-format".to_string())
        );
        assert_eq!(
            apply_rename_style("Json", "camelCase"),
            Ok("json".to_string())
        );
        assert_eq!(
            apply_rename_style("LogFormat", "camelCase"),
            Ok("logFormat".to_string())
        );
        assert_eq!(
            apply_rename_style("Json", "PascalCase"),
            Ok("Json".to_string())
        );
        assert_eq!(
            apply_rename_style("LogFormat", "PascalCase"),
            Ok("LogFormat".to_string())
        );
        assert_eq!(
            apply_rename_style("Json", "SCREAMING_SNAKE_CASE"),
            Ok("JSON".to_string())
        );
        assert_eq!(
            apply_rename_style("LogFormat", "SCREAMING_SNAKE_CASE"),
            Ok("LOG_FORMAT".to_string())
        );
        assert_eq!(
            apply_rename_style("Json", "lowercase"),
            Ok("json".to_string())
        );
        assert_eq!(
            apply_rename_style("LogFormat", "lowercase"),
            Ok("logformat".to_string())
        );
        assert_eq!(
            apply_rename_style("Json", "UPPERCASE"),
            Ok("JSON".to_string())
        );
        assert_eq!(
            apply_rename_style("LogFormat", "UPPERCASE"),
            Ok("LOGFORMAT".to_string())
        );

        // Test error case for unknown style
        assert!(apply_rename_style("Json", "unknown").is_err());
        let error = apply_rename_style("Json", "unknown").unwrap_err();
        assert!(error.contains("Unknown serde rename style: 'unknown'"));
        assert!(error.contains("snake_case"));
    }
}
