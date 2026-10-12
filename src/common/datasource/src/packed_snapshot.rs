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

//! Shared on-disk index for one packed snapshot schema chunk.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{InvalidPackedSnapshotSnafu, Result};

pub const PACK_INDEX_FILE: &str = "pack-index.json";
pub const PACKED_LAYOUT: &str = "metric-parquet-packs";

/// Versioned index. Object paths are generated direct children of the chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackIndex {
    pub version: u32,
    pub objects: Vec<PackObject>,
    pub tables: Vec<PackTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackObject {
    pub path: String,
    pub kind: ObjectKind,
    pub length: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObjectKind {
    Pack,
    Parquet,
}

/// One complete independent Parquet stream, including its footer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackTable {
    pub table_name: String,
    pub object: String,
    pub offset: u64,
    pub length: u64,
    pub row_count: u64,
}

impl PackIndex {
    /// Validates all references and complete, nonoverlapping object coverage.
    pub fn validate(&self) -> Result<()> {
        let invalid = |reason: &str| InvalidPackedSnapshotSnafu { reason }.build();
        if self.version != 1 {
            return Err(invalid("unsupported index version"));
        }
        let mut objects = HashMap::new();
        for object in &self.objects {
            let (prefix, suffix) = match object.kind {
                ObjectKind::Pack => ("pack-", ".bin"),
                ObjectKind::Parquet => ("table-", ".parquet"),
            };
            let generated = object
                .path
                .strip_prefix(prefix)
                .and_then(|s| s.strip_suffix(suffix))
                .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()));
            if !generated
                || object.length == 0
                || objects.insert(object.path.as_str(), object).is_some()
            {
                return Err(invalid("invalid or duplicate object path/length"));
            }
        }
        let mut names = HashSet::new();
        let mut ranges: HashMap<&str, Vec<&PackTable>> = HashMap::new();
        for table in &self.tables {
            if table.table_name.is_empty() || !names.insert(table.table_name.as_str()) {
                return Err(invalid("empty or duplicate table name"));
            }
            let object = objects
                .get(table.object.as_str())
                .ok_or_else(|| invalid("unknown object reference"))?;
            let end = table
                .offset
                .checked_add(table.length)
                .ok_or_else(|| invalid("table range overflow"))?;
            if table.length < 12 || end > object.length {
                return Err(invalid("invalid Parquet stream range"));
            }
            ranges.entry(&table.object).or_default().push(table);
        }
        for object in &self.objects {
            let entries = ranges
                .get_mut(object.path.as_str())
                .ok_or_else(|| invalid("unreferenced object"))?;
            entries.sort_unstable_by_key(|t| t.offset);
            if object.kind == ObjectKind::Parquet && entries.len() != 1 {
                return Err(invalid("standalone object must contain one table"));
            }
            let mut end = 0;
            for entry in entries {
                if entry.offset != end {
                    return Err(invalid("object ranges have a gap or overlap"));
                }
                end += entry.length; // Checked against object length above.
            }
            if end != object.length {
                return Err(invalid("object ranges do not cover the full object"));
            }
        }
        Ok(())
    }

    /// Requires exactly the data-bearing tables selected by snapshot DDL.
    pub fn validate_membership<'a>(&self, tables: impl IntoIterator<Item = &'a str>) -> Result<()> {
        self.validate()?;
        let expected: HashSet<_> = tables.into_iter().collect();
        let actual: HashSet<_> = self.tables.iter().map(|t| t.table_name.as_str()).collect();
        if expected != actual {
            return InvalidPackedSnapshotSnafu {
                reason: "index table membership differs from snapshot DDL",
            }
            .fail();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> PackIndex {
        PackIndex {
            version: 1,
            objects: vec![PackObject {
                path: "pack-000000.bin".into(),
                kind: ObjectKind::Pack,
                length: 24,
            }],
            tables: ["literal.name", "other"]
                .into_iter()
                .enumerate()
                .map(|(i, name)| PackTable {
                    table_name: name.into(),
                    object: "pack-000000.bin".into(),
                    offset: i as u64 * 12,
                    length: 12,
                    row_count: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn validates_complete_membership_and_ranges() {
        index()
            .validate_membership(["other", "literal.name"])
            .unwrap();
        assert!(index().validate_membership(["other"]).is_err());
        for mutate in [
            |i: &mut PackIndex| i.version = 2,
            |i: &mut PackIndex| i.objects[0].path = "../pack-0.bin".into(),
            |i: &mut PackIndex| i.objects.push(i.objects[0].clone()),
            |i: &mut PackIndex| i.tables[1].table_name = "literal.name".into(),
            |i: &mut PackIndex| i.tables[1].object = "missing".into(),
            |i: &mut PackIndex| i.tables[1].offset = 11,
            |i: &mut PackIndex| i.tables[1].length = u64::MAX,
            |i: &mut PackIndex| i.tables.pop().map(|_| ()).unwrap(),
        ] {
            let mut bad = index();
            mutate(&mut bad);
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }
}
