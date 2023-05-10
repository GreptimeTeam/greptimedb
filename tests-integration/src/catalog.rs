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

#[cfg(test)]
mod tests {
    use catalog::RegisterSystemTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
    use script::table::{build_scripts_schema, SCRIPTS_TABLE_NAME};
    use table::requests::{CreateTableRequest, TableOptions};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_system_table() {
        let instance =
            crate::tests::create_distributed_instance("test_register_system_table").await;

        let catalog_name = DEFAULT_CATALOG_NAME;
        let schema_name = DEFAULT_SCHEMA_NAME;
        let table_name = SCRIPTS_TABLE_NAME;
        let request = CreateTableRequest {
            id: 1,
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            desc: Some("Scripts table".to_string()),
            schema: build_scripts_schema(),
            region_numbers: vec![0],
            primary_key_indices: vec![0, 1],
            create_if_not_exists: true,
            table_options: TableOptions::default(),
            engine: MITO_ENGINE.to_string(),
        };

        let result = instance
            .frontend()
            .catalog_manager()
            .register_system_table(RegisterSystemTableRequest {
                create_table_request: request,
                open_hook: None,
            })
            .await;
        assert!(result.is_ok());

        assert!(
            instance
                .frontend()
                .catalog_manager()
                .table(catalog_name, schema_name, table_name)
                .await
                .unwrap()
                .is_some(),
            "the registered system table cannot be found in catalog"
        );

        let mut actually_created_table_in_datanode = 0;
        for datanode in instance.datanodes().values() {
            if datanode
                .catalog_manager()
                .table(catalog_name, schema_name, table_name)
                .await
                .unwrap()
                .is_some()
            {
                actually_created_table_in_datanode += 1;
            }
        }
        assert_eq!(
            actually_created_table_in_datanode, 1,
            "system table should be actually created at one and only one datanode"
        )
    }
}
