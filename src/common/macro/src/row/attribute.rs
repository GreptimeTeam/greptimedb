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
pub(crate) fn parse_column_attribute(attr: &Attribute) -> Result<ColumnAttribute> {
    match &attr.meta {
        Meta::List(list) if list.path.is_ident(META_KEY_COL) => {
            let mut attribute = ColumnAttribute::default();
            list.parse_nested_meta(|meta| {
                parse_column_attribute_field(&meta, &mut attribute)
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

type ParseColumnAttributeField = fn(&ParseNestedMeta, &mut ColumnAttribute) -> Result<()>;

static PARSE_COLUMN_ATTRIBUTE_FIELDS: Lazy<HashMap<&str, ParseColumnAttributeField>> =
    Lazy::new(|| {
        HashMap::from([
            (META_KEY_NAME, parse_name_field as _),
            (META_KEY_DATATYPE, parse_datatype_field as _),
            (META_KEY_SEMANTIC, parse_semantic_field as _),
            (META_KEY_SKIP, parse_skip_field as _),
        ])
    });

fn parse_name_field(meta: &ParseNestedMeta<'_>, attribute: &mut ColumnAttribute) -> Result<()> {
    let value = meta.value()?;
    let s: LitStr = value.parse()?;
    attribute.name = Some(s.value());
    Ok(())
}

fn parse_datatype_field(meta: &ParseNestedMeta<'_>, attribute: &mut ColumnAttribute) -> Result<()> {
    let value = meta.value()?;
    let s: LitStr = value.parse()?;
    let ident = s.value();
    let Some(value) = column_data_type_from_str(&ident) else {
        return Err(meta.error(format!("unexpected {META_KEY_DATATYPE}: {ident}")));
    };
    attribute.column_data_type = Some(value);
    Ok(())
}

fn parse_semantic_field(meta: &ParseNestedMeta<'_>, attribute: &mut ColumnAttribute) -> Result<()> {
    let value = meta.value()?;
    let s: LitStr = value.parse()?;
    let ident = s.value();
    let Some(value) = semantic_type_from_str(&ident) else {
        return Err(meta.error(format!("unexpected {META_KEY_SEMANTIC}: {ident}")));
    };
    attribute.semantic_type = value;
    Ok(())
}

fn parse_skip_field(_: &ParseNestedMeta<'_>, attribute: &mut ColumnAttribute) -> Result<()> {
    attribute.skip = true;
    Ok(())
}

fn parse_column_attribute_field(
    meta: &ParseNestedMeta<'_>,
    attribute: &mut ColumnAttribute,
) -> Result<()> {
    let Some(ident) = meta.path.get_ident() else {
        return Err(meta.error(format!("expected `{META_KEY_COL}({META_KEY_NAME} = \"...\", {META_KEY_DATATYPE} = \"...\", {META_KEY_SEMANTIC} = \"...\")`")));
    };
    let Some(parse_column_attribute) =
        PARSE_COLUMN_ATTRIBUTE_FIELDS.get(ident.to_string().as_str())
    else {
        return Err(meta.error(format!("unexpected attribute: {ident}")));
    };

    parse_column_attribute(meta, attribute)
}
