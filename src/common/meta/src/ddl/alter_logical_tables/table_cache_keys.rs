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

use table::metadata::RawTableInfo;
use table::table_name::TableName;

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::instruction::CacheIdent;

impl AlterLogicalTablesProcedure {
    pub(crate) fn build_table_cache_keys_to_invalidate(&self) -> Vec<CacheIdent> {
        let mut cache_keys = self
            .data
            .table_info_values
            .iter()
            .flat_map(|table| {
                vec![
                    CacheIdent::TableId(table.table_info.ident.table_id),
                    CacheIdent::TableName(extract_table_name(&table.table_info)),
                ]
            })
            .collect::<Vec<_>>();
        cache_keys.push(CacheIdent::TableId(self.data.physical_table_id));
        // Safety: physical_table_info already filled in previous steps
        let physical_table_info = &self.data.physical_table_info.as_ref().unwrap().table_info;
        cache_keys.push(CacheIdent::TableName(extract_table_name(
            physical_table_info,
        )));

        cache_keys
    }
}

fn extract_table_name(table_info: &RawTableInfo) -> TableName {
    TableName::new(
        &table_info.catalog_name,
        &table_info.schema_name,
        &table_info.name,
    )
}
