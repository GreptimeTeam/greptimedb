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
use quote::{quote, ToTokens};
use syn::{parse_macro_input, AttributeArgs, ItemFn, Lit, Meta, NestedMeta};

pub(crate) fn process_print_caller(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut depth = 1;

    let args = parse_macro_input!(args as AttributeArgs);
    for meta in args.iter() {
        if let NestedMeta::Meta(Meta::NameValue(name_value)) = meta {
            let ident = name_value
                .path
                .get_ident()
                .expect("Expected an ident!")
                .to_string();
            if ident == "depth" {
                let Lit::Int(i) = &name_value.lit else {
                    panic!("Expected 'depth' to be a valid int!")
                };
                depth = i.base10_parse::<usize>().expect("Invalid 'depth' value");
                break;
            }
        }
    }

    let tokens: TokenStream = quote! {
        {
            let curr_file = file!();

            let bt = backtrace::Backtrace::new();
            let call_stack = bt
                .frames()
                .iter()
                .skip_while(|f| {
                    !f.symbols().iter().any(|s| {
                        s.filename()
                            .map(|p| p.ends_with(curr_file))
                            .unwrap_or(false)
                    })
                })
                .skip(1)
                .take(#depth);

            let call_stack = call_stack
                .map(|f| {
                    f.symbols()
                        .iter()
                        .map(|s| {
                            let filename = s
                                .filename()
                                .map(|p| format!("{:?}", p))
                                .unwrap_or_else(|| "unknown".to_string());

                            let lineno = s
                                .lineno()
                                .map(|l| format!("{}", l))
                                .unwrap_or_else(|| "unknown".to_string());

                            format!("filename: {}, lineno: {}", filename, lineno)
                        })
                        .collect::<Vec<String>>()
                        .join(", ")
                })
                .collect::<Vec<_>>();

            match call_stack.len() {
                0 => common_telemetry::info!("unable to find call stack"),
                1 => common_telemetry::info!("caller: {}", call_stack[0]),
                _ => {
                    let mut s = String::new();
                    s.push_str("[\n");
                    for e in call_stack {
                        s.push_str("\t");
                        s.push_str(&e);
                        s.push_str("\n");
                    }
                    s.push_str("]");
                    common_telemetry::info!("call stack: {}", s)
                }
            }
        }
    }
    .into();

    let stmt = match syn::parse(tokens) {
        Ok(stmt) => stmt,
        Err(e) => return e.into_compile_error().into(),
    };

    let mut item = parse_macro_input!(input as ItemFn);
    item.block.stmts.insert(0, stmt);

    item.into_token_stream().into()
}
