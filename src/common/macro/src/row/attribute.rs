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

use greptime_proto::v1::ColumnDataType;
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{Attribute, LitStr, Meta, Result};

use crate::row::utils::{column_data_type_from_str, semantic_type_from_str, SemanticType};
use crate::row::{
    META_KEY_COL, META_KEY_DATATYPE, META_KEY_NAME, META_KEY_SEMANTIC, META_KEY_SKIP,
};

/// Column attribute.
#[derive(Default)]
pub(crate) struct ColumnAttribute {
    /// User-defined name of the column.,
    pub(crate) name: Option<String>,
    /// Data type of the column.
    pub(crate) column_data_type: Option<ColumnDataType>,
    /// Semantic type of the column.
    pub(crate) semantic_type: SemanticType,
    /// Whether to skip the column.
    pub(crate) skip: bool,
}

/// Find the column attribute in the attributes.
pub(crate) fn find_column_attribute(attrs: &[Attribute]) -> Option<&Attribute> {
    attrs
        .iter()
        .find(|attr| matches!(&attr.meta, Meta::List(list) if list.path.is_ident(META_KEY_COL)))
}

/// Parse the column attribute.
pub(crate) fn parse_attribute(attr: &Attribute) -> Result<ColumnAttribute> {
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
            let Some(value) = column_data_type_from_str(&ident) else {
                return Err(meta.error(format!("unexpected {META_KEY_DATATYPE}: {ident}")));
            };
            attribute.column_data_type = Some(value);
        }
        META_KEY_SEMANTIC => {
            let value = meta.value()?;
            let s: LitStr = value.parse()?;

            let ident = s.value();
            let Some(value) = semantic_type_from_str(&ident) else {
                return Err(meta.error(format!("unexpected {META_KEY_SEMANTIC}: {ident}")));
            };
            attribute.semantic_type = value;
        }
        META_KEY_SKIP => attribute.skip = true,
        attr => return Err(meta.error(format!("unexpected attribute: {attr}"))),
    }

    Ok(())
}
