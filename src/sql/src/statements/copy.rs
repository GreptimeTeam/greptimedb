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

use std::fmt::Display;

use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

use crate::statements::OptionMap;

macro_rules! format_sorted_hashmap {
    ($hashmap:expr) => {{
        let hashmap = $hashmap;
        let mut sorted_keys: Vec<&String> = hashmap.keys().collect();
        sorted_keys.sort();
        let mut result = String::new();
        for key in sorted_keys {
            if let Some(val) = hashmap.get(key) {
                result.push_str(&format!("{} = {}, ", key, val));
            }
        }
        result
    }};
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum Copy {
    CopyTable(CopyTable),
    CopyDatabase(CopyDatabase),
}

impl Display for Copy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Copy::CopyTable(s) => s.fmt(f),
            Copy::CopyDatabase(s) => s.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum CopyTable {
    To(CopyTableArgument),
    From(CopyTableArgument),
}

impl Display for CopyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyTable::To(s) => {
                let table_name = &s.table_name;
                let with = s.format_with();
                let connection = s.format_connection();
                let location = &s.location;
                write!(f, r#"COPY {table_name} TO {location} {with} {connection}"#)
            }
            CopyTable::From(s) => {
                let table_name = &s.table_name;
                let with = s.format_with();
                let connection = s.format_connection();
                let location = &s.location;
                write!(
                    f,
                    r#"COPY {table_name} FROM {location} {with} {connection}"#
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum CopyDatabase {
    To(CopyDatabaseArgument),
    From(CopyDatabaseArgument),
}

impl Display for CopyDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyDatabase::To(s) => {
                let database_name = &s.database_name;
                let with = s.format_with();
                let connection = s.format_connection();
                let location = &s.location;
                write!(
                    f,
                    r#"COPY DATABASE {database_name} TO {location} {with} {connection}"#
                )
            }
            CopyDatabase::From(s) => {
                let database_name = &s.database_name;
                let with = s.format_with();
                let connection = s.format_connection();
                let location = &s.location;
                write!(
                    f,
                    r#"COPY DATABASE {database_name} FROM {location} {with} {connection}"#
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct CopyDatabaseArgument {
    pub database_name: ObjectName,
    pub with: OptionMap,
    pub connection: OptionMap,
    pub location: String,
}

impl CopyDatabaseArgument {
    #[inline]
    fn format_with(&self) -> String {
        if self.with.map.is_empty() {
            String::default()
        } else {
            let options = format_sorted_hashmap!(&self.with.map);
            format!(
                r#"WITH(
{options}
)"#
            )
        }
    }

    #[inline]
    fn format_connection(&self) -> String {
        if self.connection.map.is_empty() {
            String::default()
        } else {
            let options = format_sorted_hashmap!(&self.connection.map);
            format!(
                r#"CONNECTION(
{options}
)"#
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct CopyTableArgument {
    pub table_name: ObjectName,
    pub with: OptionMap,
    pub connection: OptionMap,
    /// Copy tbl [To|From] 'location'.
    pub location: String,
}

impl CopyTableArgument {
    #[inline]
    fn format_with(&self) -> String {
        if self.with.map.is_empty() {
            String::default()
        } else {
            let options = format_sorted_hashmap!(&self.with.map);
            format!(
                r#"WITH(
{options}
)"#
            )
        }
    }

    #[inline]
    fn format_connection(&self) -> String {
        if self.connection.map.is_empty() {
            String::default()
        } else {
            let options = format_sorted_hashmap!(&self.connection.map);
            format!(
                r#"CONNECTION(
{options}
)"#
            )
        }
    }
}

#[cfg(test)]
impl CopyTableArgument {
    pub fn format(&self) -> Option<String> {
        self.with
            .get(common_datasource::file_format::FORMAT_TYPE)
            .cloned()
            .or_else(|| Some("PARQUET".to_string()))
    }

    pub fn pattern(&self) -> Option<String> {
        self.with
            .get(common_datasource::file_format::FILE_PATTERN)
            .cloned()
    }
}
