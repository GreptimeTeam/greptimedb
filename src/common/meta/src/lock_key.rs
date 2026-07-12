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

use common_catalog::{format_full_table_name, format_schema_name};
use common_procedure::StringKey;
use store_api::storage::{RegionId, TableId};

use crate::key::FlowId;

const CATALOG_LOCK_PREFIX: &str = "__catalog_lock";
const SCHEMA_LOCK_PREFIX: &str = "__schema_lock";
const TABLE_LOCK_PREFIX: &str = "__table_lock";
const TABLE_NAME_LOCK_PREFIX: &str = "__table_name_lock";
const FLOW_NAME_LOCK_PREFIX: &str = "__flow_name_lock";
const REGION_LOCK_PREFIX: &str = "__region_lock";
const FLOW_LOCK_PREFIX: &str = "__flow_lock";
const REMOTE_WAL_LOCK_PREFIX: &str = "__remote_wal_lock";

/// [CatalogLock] acquires the lock on the tenant level.
pub enum CatalogLock<'a> {
    Read(&'a str),
    Write(&'a str),
}

impl Display for CatalogLock<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match self {
            CatalogLock::Read(s) => s,
            CatalogLock::Write(s) => s,
        };
        write!(f, "{}/{}", CATALOG_LOCK_PREFIX, key)
    }
}

impl From<CatalogLock<'_>> for StringKey {
    fn from(value: CatalogLock) -> Self {
        match value {
            CatalogLock::Write(_) => StringKey::Exclusive(value.to_string()),
            CatalogLock::Read(_) => StringKey::Share(value.to_string()),
        }
    }
}

/// [SchemaLock] acquires the lock on the database level.
pub enum SchemaLock {
    Read(String),
    Write(String),
}

impl SchemaLock {
    pub fn read(catalog: &str, schema: &str) -> Self {
        Self::Read(format_schema_name(catalog, schema))
    }

    pub fn write(catalog: &str, schema: &str) -> Self {
        Self::Write(format_schema_name(catalog, schema))
    }
}

impl Display for SchemaLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match self {
            SchemaLock::Read(s) => s,
            SchemaLock::Write(s) => s,
        };
        write!(f, "{}/{}", SCHEMA_LOCK_PREFIX, key)
    }
}

impl From<SchemaLock> for StringKey {
    fn from(value: SchemaLock) -> Self {
        match value {
            SchemaLock::Write(_) => StringKey::Exclusive(value.to_string()),
            SchemaLock::Read(_) => StringKey::Share(value.to_string()),
        }
    }
}

/// [TableNameLock] prevents any procedures trying to create a table named it.
pub enum TableNameLock {
    Write(String),
}

impl TableNameLock {
    pub fn new(catalog: &str, schema: &str, table: &str) -> Self {
        Self::Write(format_full_table_name(catalog, schema, table))
    }
}

impl Display for TableNameLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let TableNameLock::Write(name) = self;
        write!(f, "{}/{}", TABLE_NAME_LOCK_PREFIX, name)
    }
}

impl From<TableNameLock> for StringKey {
    fn from(value: TableNameLock) -> Self {
        match value {
            TableNameLock::Write(_) => StringKey::Exclusive(value.to_string()),
        }
    }
}

/// [FlowNameLock] prevents any procedures trying to create a flow named it.
pub enum FlowNameLock {
    Write(String),
}

impl FlowNameLock {
    pub fn new(catalog: &str, flow_name: &str) -> Self {
        Self::Write(format!("{catalog}.{flow_name}"))
    }
}

impl Display for FlowNameLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let FlowNameLock::Write(name) = self;
        write!(f, "{}/{}", FLOW_NAME_LOCK_PREFIX, name)
    }
}

impl From<FlowNameLock> for StringKey {
    fn from(value: FlowNameLock) -> Self {
        match value {
            FlowNameLock::Write(_) => StringKey::Exclusive(value.to_string()),
        }
    }
}

/// [TableLock] acquires the lock on the table level.
///
/// Note: Allows to read/modify the corresponding table's [TableInfoValue](crate::key::table_info::TableInfoValue),
/// [TableRouteValue](crate::key::table_route::TableRouteValue), [TableDatanodeValue](crate::key::datanode_table::DatanodeTableValue).
pub enum TableLock {
    Read(TableId),
    Write(TableId),
}

impl Display for TableLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match self {
            TableLock::Read(s) => s,
            TableLock::Write(s) => s,
        };
        write!(f, "{}/{}", TABLE_LOCK_PREFIX, key)
    }
}

impl From<TableLock> for StringKey {
    fn from(value: TableLock) -> Self {
        match value {
            TableLock::Write(_) => StringKey::Exclusive(value.to_string()),
            TableLock::Read(_) => StringKey::Share(value.to_string()),
        }
    }
}

/// [RegionLock] acquires the lock on the region level.
///
/// Note:
/// - Allows modification the corresponding region's [TableRouteValue](crate::key::table_route::TableRouteValue),
///   [TableDatanodeValue](crate::key::datanode_table::DatanodeTableValue) even if
///   it acquires the [RegionLock::Write] only without acquiring the [TableLock::Write].
///
/// - Should acquire [TableLock] of the table at same procedure.
///
/// TODO(weny): we should consider separating TableRouteValue into finer keys.
pub enum RegionLock {
    Read(RegionId),
    Write(RegionId),
}

impl Display for RegionLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match self {
            RegionLock::Read(s) => s.as_u64(),
            RegionLock::Write(s) => s.as_u64(),
        };
        write!(f, "{}/{}", REGION_LOCK_PREFIX, key)
    }
}

impl From<RegionLock> for StringKey {
    fn from(value: RegionLock) -> Self {
        match value {
            RegionLock::Write(_) => StringKey::Exclusive(value.to_string()),
            RegionLock::Read(_) => StringKey::Share(value.to_string()),
        }
    }
}

/// [FlowLock] acquires the lock on the table level.
///
/// Note: Allows to read/modify the corresponding flow's [FlowInfoValue](crate::key::flow::flow_info::FlowInfoValue),
/// [FlowNameValue](crate::key::flow::flow_name::FlowNameValue),[FlownodeFlowKey](crate::key::flow::flownode_flow::FlownodeFlowKey),
/// [TableFlowKey](crate::key::flow::table_flow::TableFlowKey).
pub enum FlowLock {
    Read(FlowId),
    Write(FlowId),
}

impl Display for FlowLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match self {
            FlowLock::Read(s) => s,
            FlowLock::Write(s) => s,
        };
        write!(f, "{}/{}", FLOW_LOCK_PREFIX, key)
    }
}

impl From<FlowLock> for StringKey {
    fn from(value: FlowLock) -> Self {
        match value {
            FlowLock::Write(_) => StringKey::Exclusive(value.to_string()),
            FlowLock::Read(_) => StringKey::Share(value.to_string()),
        }
    }
}

/// [RemoteWalLock] acquires the lock on the remote wal topic level.
pub enum RemoteWalLock {
    Read(String),
    Write(String),
}

impl Display for RemoteWalLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match self {
            RemoteWalLock::Read(s) => s,
            RemoteWalLock::Write(s) => s,
        };
        write!(f, "{}/{}", REMOTE_WAL_LOCK_PREFIX, key)
    }
}

impl From<RemoteWalLock> for StringKey {
    fn from(value: RemoteWalLock) -> Self {
        match value {
            RemoteWalLock::Write(_) => StringKey::Exclusive(value.to_string()),
            RemoteWalLock::Read(_) => StringKey::Share(value.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_procedure::StringKey;

    use crate::lock_key::*;

    #[test]
    fn test_lock_key() {
        // The catalog lock
        let string_key: StringKey = CatalogLock::Read("foo").into();
        assert_eq!(
            string_key,
            StringKey::Share(format!("{}/{}", CATALOG_LOCK_PREFIX, "foo"))
        );
        let string_key: StringKey = CatalogLock::Write("foo").into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", CATALOG_LOCK_PREFIX, "foo"))
        );
        // The schema lock
        let string_key: StringKey = SchemaLock::read("foo", "bar").into();
        assert_eq!(
            string_key,
            StringKey::Share(format!("{}/{}", SCHEMA_LOCK_PREFIX, "foo.bar"))
        );
        let string_key: StringKey = SchemaLock::write("foo", "bar").into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", SCHEMA_LOCK_PREFIX, "foo.bar"))
        );
        // The table lock
        let string_key: StringKey = TableLock::Read(1024).into();
        assert_eq!(
            string_key,
            StringKey::Share(format!("{}/{}", TABLE_LOCK_PREFIX, 1024))
        );
        let string_key: StringKey = TableLock::Write(1024).into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", TABLE_LOCK_PREFIX, 1024))
        );
        // The table name lock
        let string_key: StringKey = TableNameLock::new("foo", "bar", "baz").into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", TABLE_NAME_LOCK_PREFIX, "foo.bar.baz"))
        );
        // The flow name lock
        let string_key: StringKey = FlowNameLock::new("foo", "baz").into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", FLOW_NAME_LOCK_PREFIX, "foo.baz"))
        );
        // The region lock
        let region_id = RegionId::new(1024, 1);
        let string_key: StringKey = RegionLock::Read(region_id).into();
        assert_eq!(
            string_key,
            StringKey::Share(format!("{}/{}", REGION_LOCK_PREFIX, region_id.as_u64()))
        );
        let string_key: StringKey = RegionLock::Write(region_id).into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", REGION_LOCK_PREFIX, region_id.as_u64()))
        );
        // The flow lock
        let flow_id = 1024;
        let string_key: StringKey = FlowLock::Read(flow_id).into();
        assert_eq!(
            string_key,
            StringKey::Share(format!("{}/{}", FLOW_LOCK_PREFIX, flow_id))
        );
        let string_key: StringKey = FlowLock::Write(flow_id).into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", FLOW_LOCK_PREFIX, flow_id))
        );
        // The remote wal lock
        let string_key: StringKey = RemoteWalLock::Read("foo".to_string()).into();
        assert_eq!(
            string_key,
            StringKey::Share(format!("{}/{}", REMOTE_WAL_LOCK_PREFIX, "foo"))
        );
        let string_key: StringKey = RemoteWalLock::Write("foo".to_string()).into();
        assert_eq!(
            string_key,
            StringKey::Exclusive(format!("{}/{}", REMOTE_WAL_LOCK_PREFIX, "foo"))
        );
    }
}
