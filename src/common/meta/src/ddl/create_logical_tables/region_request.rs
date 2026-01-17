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

use api::v1::CreateTableExpr;
use api::v1::region::{CreateRequests, RegionRequest, RegionRequestHeader, region_request};
use common_telemetry::debug;
use common_telemetry::tracing_context::TracingContext;
use store_api::storage::{RegionId, TableId};
use table::metadata::RawTableInfo;

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table::template::{
    CreateRequestBuilder, build_template, build_template_from_raw_table_info,
};
use crate::ddl::utils::region_storage_path;
use crate::error::Result;
use crate::peer::Peer;
use crate::rpc::router::{RegionRoute, find_leader_regions};

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
        let partition_exprs = region_routes
            .iter()
            .map(|r| (r.region.id.region_number(), r.region.partition_expr()))
            .collect();
        for (task, table_id_already_exists) in tasks.iter().zip(table_ids_already_exists) {
            if table_id_already_exists.is_some() {
                continue;
            }
            let create_table_expr = &task.create_table;
            let catalog = &create_table_expr.catalog_name;
            let schema = &create_table_expr.schema_name;
            let logical_table_id = task.table_info.ident.table_id;
            let physical_table_id = self.data.physical_table_id;
            let storage_path = region_storage_path(catalog, schema);
            let request_builder = create_region_request_builder_from_raw_table_info(
                &task.table_info,
                physical_table_id,
            )?;

            for region_number in &regions_on_this_peer {
                let region_id = RegionId::new(logical_table_id, *region_number);
                let one_region_request = request_builder.build_one(
                    region_id,
                    storage_path.clone(),
                    &HashMap::new(),
                    &partition_exprs,
                );
                requests.push(one_region_request);
            }
        }

        if requests.is_empty() {
            debug!("no region request to send to datanodes");
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
}

/// Creates a region request builder
pub fn create_region_request_builder(
    create_table_expr: &CreateTableExpr,
    physical_table_id: TableId,
) -> Result<CreateRequestBuilder> {
    let template = build_template(create_table_expr)?;
    Ok(CreateRequestBuilder::new(template, Some(physical_table_id)))
}

/// Builds a [CreateRequestBuilder] from a [RawTableInfo].
///
/// Note: This function is primarily intended for creating logical tables or allocating placeholder regions.
pub fn create_region_request_builder_from_raw_table_info(
    raw_table_info: &RawTableInfo,
    physical_table_id: TableId,
) -> Result<CreateRequestBuilder> {
    let template = build_template_from_raw_table_info(raw_table_info, true)?;
    Ok(CreateRequestBuilder::new(template, Some(physical_table_id)))
}
