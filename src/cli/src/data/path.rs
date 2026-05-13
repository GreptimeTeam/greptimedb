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

//! Shared path helpers for export/import data files.

use crate::data::export_v2::schema::{DDL_DIR, SCHEMA_DIR};

pub(crate) fn ddl_path_for_schema(schema: &str) -> String {
    format!(
        "{}/{}/{}.sql",
        SCHEMA_DIR,
        DDL_DIR,
        encode_path_segment(schema)
    )
}

pub(crate) fn data_dir_for_schema_chunk(schema: &str, chunk_id: u32) -> String {
    format!("data/{}/{}/", encode_path_segment(schema), chunk_id)
}

pub(crate) fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push(hex_char(byte >> 4));
                encoded.push(hex_char(byte & 0x0F));
            }
        }
    }
    encoded
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + (nibble - 10)) as char,
        _ => unreachable!("nibble must be in 0..=15"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_path_segment_preserves_safe_ascii() {
        assert_eq!(encode_path_segment("test_db"), "test_db");
    }

    #[test]
    fn test_encode_path_segment_escapes_path_traversal_chars() {
        assert_eq!(encode_path_segment("../evil"), "%2E%2E%2Fevil");
        assert_eq!(encode_path_segment(r"..\\evil"), "%2E%2E%5C%5Cevil");
    }

    #[test]
    fn test_ddl_path_for_schema_encodes_schema_segment() {
        assert_eq!(ddl_path_for_schema("public"), "schema/ddl/public.sql");
        assert_eq!(
            ddl_path_for_schema("../evil"),
            "schema/ddl/%2E%2E%2Fevil.sql"
        );
    }

    #[test]
    fn test_data_dir_for_schema_chunk_encodes_schema_segment() {
        assert_eq!(data_dir_for_schema_chunk("public", 1), "data/public/1/");
        assert_eq!(
            data_dir_for_schema_chunk("../evil", 7),
            "data/%2E%2E%2Fevil/7/"
        );
    }
}
