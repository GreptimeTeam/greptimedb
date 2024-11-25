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
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;

use crate::adapter::util::table_info_value_to_relation_desc;
use crate::adapter::TableName;
use crate::error::{
    Error, ExternalSnafu, TableNotFoundMetaSnafu, TableNotFoundSnafu, UnexpectedSnafu,
};
use crate::repr::{self, ColumnType, RelationDesc, RelationType};

/// mapping of table name <-> table id should be query from tableinfo manager
pub struct TableSource {
    /// for query `TableId -> TableName` mapping
    table_info_manager: TableInfoManager,
    table_name_manager: TableNameManager,
}

impl TableSource {
    pub fn new(table_info_manager: TableInfoManager, table_name_manager: TableNameManager) -> Self {
        TableSource {
            table_info_manager,
            table_name_manager,
        }
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

    /// If the table havn't been created in database, the tableId returned would be null
    pub async fn get_table_id_from_name(&self, name: &TableName) -> Result<Option<TableId>, Error> {
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
    ) -> Result<(TableName, RelationDesc), Error> {
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
}
