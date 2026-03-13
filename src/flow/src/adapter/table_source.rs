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

//! How to query table information from database

use common_error::ext::BoxedError;
use common_meta::key::table_info::{TableInfoManager, TableInfoValue};
use common_meta::key::table_name::{TableNameKey, TableNameManager};
use datatypes::schema::ColumnDefaultConstraint;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;

use crate::adapter::TableName;
use crate::adapter::util::table_info_value_to_relation_desc;
use crate::error::{
    Error, ExternalSnafu, TableNotFoundMetaSnafu, TableNotFoundSnafu, UnexpectedSnafu,
};
use crate::repr::RelationDesc;

/// Table description, include relation desc and default values, which is the minimal information flow needed for table
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDesc {
    pub relation_desc: RelationDesc,
    pub default_values: Vec<Option<ColumnDefaultConstraint>>,
}

impl TableDesc {
    pub fn new(
        relation_desc: RelationDesc,
        default_values: Vec<Option<ColumnDefaultConstraint>>,
    ) -> Self {
        Self {
            relation_desc,
            default_values,
        }
    }

    pub fn new_no_default(relation_desc: RelationDesc) -> Self {
        Self {
            relation_desc,
            default_values: vec![],
        }
    }
}

/// Table source but for flow, provide table schema by table name/id
#[async_trait::async_trait]
pub trait FlowTableSource: Send + Sync + std::fmt::Debug {
    async fn table_name_from_id(&self, table_id: &TableId) -> Result<TableName, Error>;
    async fn table_id_from_name(&self, name: &TableName) -> Result<TableId, Error>;

    /// Get the table schema by table name
    async fn table(&self, name: &TableName) -> Result<TableDesc, Error> {
        let id = self.table_id_from_name(name).await?;
        self.table_from_id(&id).await
    }
    async fn table_from_id(&self, table_id: &TableId) -> Result<TableDesc, Error>;
}

/// managed table source information, query from table info manager and table name manager
#[derive(Clone)]
pub struct ManagedTableSource {
    /// for query `TableId -> TableName` mapping
    table_info_manager: TableInfoManager,
    table_name_manager: TableNameManager,
}

#[async_trait::async_trait]
impl FlowTableSource for ManagedTableSource {
    async fn table_from_id(&self, table_id: &TableId) -> Result<TableDesc, Error> {
        let table_info_value = self
            .get_table_info_value(table_id)
            .await?
            .with_context(|| TableNotFoundSnafu {
                name: format!("TableId = {:?}, Can't found table info", table_id),
            })?;
        let desc = table_info_value_to_relation_desc(table_info_value)?;

        Ok(desc)
    }
    async fn table_name_from_id(&self, table_id: &TableId) -> Result<TableName, Error> {
        self.get_table_name(table_id).await
    }
    async fn table_id_from_name(&self, name: &TableName) -> Result<TableId, Error> {
        self.get_opt_table_id_from_name(name)
            .await?
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })
    }
}

impl ManagedTableSource {
    pub fn new(table_info_manager: TableInfoManager, table_name_manager: TableNameManager) -> Self {
        ManagedTableSource {
            table_info_manager,
            table_name_manager,
        }
    }

    /// Get the time index column from table id
    pub async fn get_time_index_column_from_table_id(
        &self,
        table_id: TableId,
    ) -> Result<(usize, datatypes::schema::ColumnSchema), Error> {
        let info = self
            .table_info_manager
            .get(table_id)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
            .context(UnexpectedSnafu {
                reason: format!("Table id = {:?}, couldn't found table info", table_id),
            })?;
        let schema = &info.table_info.meta.schema;
        let Some(ts_index) = schema.timestamp_index() else {
            UnexpectedSnafu {
                reason: format!("Table id = {:?}, couldn't found timestamp index", table_id),
            }
            .fail()?
        };
        let col_schema = schema.column_schemas()[ts_index].clone();
        Ok((ts_index, col_schema))
    }

    pub async fn get_table_id_from_proto_name(
        &self,
        name: &greptime_proto::v1::TableName,
    ) -> Result<TableId, Error> {
        self.table_name_manager
            .get(TableNameKey::new(
                &name.catalog_name,
                &name.schema_name,
                &name.table_name,
            ))
            .await
            .with_context(|_| TableNotFoundMetaSnafu {
                msg: format!("Table name = {:?}, couldn't found table id", name),
            })?
            .with_context(|| UnexpectedSnafu {
                reason: format!("Table name = {:?}, couldn't found table id", name),
            })
            .map(|id| id.table_id())
    }

    /// If the table haven't been created in database, the tableId returned would be null
    pub async fn get_opt_table_id_from_name(
        &self,
        name: &TableName,
    ) -> Result<Option<TableId>, Error> {
        let ret = self
            .table_name_manager
            .get(TableNameKey::new(&name[0], &name[1], &name[2]))
            .await
            .with_context(|_| TableNotFoundMetaSnafu {
                msg: format!("Table name = {:?}, couldn't found table id", name),
            })?
            .map(|id| id.table_id());
        Ok(ret)
    }

    /// query metasrv about the table name and table id
    pub async fn get_table_name(&self, table_id: &TableId) -> Result<TableName, Error> {
        self.table_info_manager
            .get(*table_id)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
            .with_context(|| UnexpectedSnafu {
                reason: format!("Table id = {:?}, couldn't found table name", table_id),
            })
            .map(|name| name.table_name())
            .map(|name| [name.catalog_name, name.schema_name, name.table_name])
    }

    /// query metasrv about the `TableInfoValue` and table id
    pub async fn get_table_info_value(
        &self,
        table_id: &TableId,
    ) -> Result<Option<TableInfoValue>, Error> {
        Ok(self
            .table_info_manager
            .get(*table_id)
            .await
            .with_context(|_| TableNotFoundMetaSnafu {
                msg: format!("TableId = {:?}, couldn't found table name", table_id),
            })?
            .map(|v| v.into_inner()))
    }

    pub async fn get_table_name_schema(
        &self,
        table_id: &TableId,
    ) -> Result<(TableName, TableDesc), Error> {
        let table_info_value = self
            .get_table_info_value(table_id)
            .await?
            .with_context(|| TableNotFoundSnafu {
                name: format!("TableId = {:?}, Can't found table info", table_id),
            })?;

        let table_name = table_info_value.table_name();
        let table_name = [
            table_name.catalog_name,
            table_name.schema_name,
            table_name.table_name,
        ];

        let desc = table_info_value_to_relation_desc(table_info_value)?;
        Ok((table_name, desc))
    }

    pub async fn check_table_exist(&self, table_id: &TableId) -> Result<bool, Error> {
        self.table_info_manager
            .exists(*table_id)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}

impl std::fmt::Debug for ManagedTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvBackendTableSource").finish()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;

    use datatypes::data_type::ConcreteDataType as CDT;

    use super::*;
    use crate::repr::{ColumnType, RelationType};

    pub struct FlowDummyTableSource {
        pub id_names_to_desc: Vec<(TableId, TableName, TableDesc)>,
        id_to_idx: HashMap<TableId, usize>,
        name_to_idx: HashMap<TableName, usize>,
    }

    impl Default for FlowDummyTableSource {
        fn default() -> Self {
            let id_names_to_desc = vec![
                (
                    1024,
                    [
                        "greptime".to_string(),
                        "public".to_string(),
                        "numbers".to_string(),
                    ],
                    TableDesc::new_no_default(
                        RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)])
                            .into_named(vec![Some("number".to_string())]),
                    ),
                ),
                (
                    1025,
                    [
                        "greptime".to_string(),
                        "public".to_string(),
                        "numbers_with_ts".to_string(),
                    ],
                    TableDesc::new_no_default(
                        RelationType::new(vec![
                            ColumnType::new(CDT::uint32_datatype(), false),
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), false),
                        ])
                        .into_named(vec![Some("number".to_string()), Some("ts".to_string())]),
                    ),
                ),
            ];
            let id_to_idx = id_names_to_desc
                .iter()
                .enumerate()
                .map(|(idx, (id, _name, _desc))| (*id, idx))
                .collect();
            let name_to_idx = id_names_to_desc
                .iter()
                .enumerate()
                .map(|(idx, (_id, name, _desc))| (name.clone(), idx))
                .collect();
            Self {
                id_names_to_desc,
                id_to_idx,
                name_to_idx,
            }
        }
    }

    #[async_trait::async_trait]
    impl FlowTableSource for FlowDummyTableSource {
        async fn table_from_id(&self, table_id: &TableId) -> Result<TableDesc, Error> {
            let idx = self.id_to_idx.get(table_id).context(TableNotFoundSnafu {
                name: format!("Table id = {:?}, couldn't found table desc", table_id),
            })?;
            let desc = self
                .id_names_to_desc
                .get(*idx)
                .map(|x| x.2.clone())
                .context(TableNotFoundSnafu {
                    name: format!("Table id = {:?}, couldn't found table desc", table_id),
                })?;
            Ok(desc)
        }

        async fn table_name_from_id(&self, table_id: &TableId) -> Result<TableName, Error> {
            let idx = self.id_to_idx.get(table_id).context(TableNotFoundSnafu {
                name: format!("Table id = {:?}, couldn't found table desc", table_id),
            })?;
            self.id_names_to_desc
                .get(*idx)
                .map(|x| x.1.clone())
                .context(TableNotFoundSnafu {
                    name: format!("Table id = {:?}, couldn't found table desc", table_id),
                })
        }

        async fn table_id_from_name(&self, name: &TableName) -> Result<TableId, Error> {
            for (id, table_name, _desc) in &self.id_names_to_desc {
                if name == table_name {
                    return Ok(*id);
                }
            }
            TableNotFoundSnafu {
                name: format!("Table name = {:?}, couldn't found table id", name),
            }
            .fail()?
        }
    }

    impl std::fmt::Debug for FlowDummyTableSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("DummyTableSource").finish()
        }
    }
}
