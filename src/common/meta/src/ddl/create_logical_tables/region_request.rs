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

use api::v1::region::{region_request, CreateRequests, RegionRequest, RegionRequestHeader};
use common_telemetry::tracing_context::TracingContext;
use store_api::storage::RegionId;

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table_template::{build_template, CreateRequestBuilder};
use crate::ddl::utils::region_storage_path;
use crate::error::Result;
use crate::peer::Peer;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::{find_leader_regions, RegionRoute};

impl CreateLogicalTablesProcedure {
    pub(crate) fn make_request(
        &self,
        peer: &Peer,
        region_routes: &[RegionRoute],
    ) -> Result<Option<RegionRequest>> {
        let tasks = &self.data.tasks;
        let table_ids_already_exists = &self.data.table_ids_already_exists;
        let regions_on_this_peer = find_leader_regions(region_routes, peer);
        let mut requests = Vec::with_capacity(tasks.len() * regions_on_this_peer.len());
        for (task, table_id_already_exists) in tasks.iter().zip(table_ids_already_exists) {
            if table_id_already_exists.is_some() {
                continue;
            }
            let create_table_expr = &task.create_table;
            let catalog = &create_table_expr.catalog_name;
            let schema = &create_table_expr.schema_name;
            let logical_table_id = task.table_info.ident.table_id;
            let storage_path = region_storage_path(catalog, schema);
            let request_builder = self.create_region_request_builder(task)?;

            for region_number in &regions_on_this_peer {
                let region_id = RegionId::new(logical_table_id, *region_number);
                let one_region_request =
                    request_builder.build_one(region_id, storage_path.clone(), &HashMap::new())?;
                requests.push(one_region_request);
            }
        }

        if requests.is_empty() {
            return Ok(None);
        }

        Ok(Some(RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::Creates(CreateRequests { requests })),
        }))
    }

    fn create_region_request_builder(
        &self,
        task: &CreateTableTask,
    ) -> Result<CreateRequestBuilder> {
        let create_expr = &task.create_table;
        let template = build_template(create_expr)?;
        Ok(CreateRequestBuilder::new(
            template,
            Some(self.data.physical_table_id),
        ))
    }
}
