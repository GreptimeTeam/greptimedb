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

use std::sync::{Arc, Mutex};

use api::v1::flow::flow_request::Body as PbFlowRequest;
use api::v1::flow::{CreateRequest, FlowRequest, FlowResponse};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_procedure_test::execute_procedure_until_done;
use common_time::TimeToLive;
use table::table_name::TableName;

use crate::ddl::activate_flow::ActivatePendingFlowProcedure;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::tests::create_flow::create_test_flow;
use crate::key::table_route::TableRouteValue;
use crate::test_util::{MockFlownodeHandler, MockFlownodeManager, new_ddl_context};

#[derive(Clone, Default)]
struct RecordingFlownodeHandler {
    create_requests: Arc<Mutex<Vec<CreateRequest>>>,
}

#[async_trait::async_trait]
impl MockFlownodeHandler for RecordingFlownodeHandler {
    async fn handle(
        &self,
        _peer: &crate::peer::Peer,
        request: FlowRequest,
    ) -> crate::error::Result<FlowResponse> {
        if let Some(PbFlowRequest::Create(create_req)) = request.body {
            self.create_requests.lock().unwrap().push(create_req);
        }

        Ok(FlowResponse {
            affected_rows: 0,
            ..Default::default()
        })
    }

    async fn handle_inserts(
        &self,
        _peer: &crate::peer::Peer,
        _requests: api::v1::region::InsertRequests,
    ) -> crate::error::Result<FlowResponse> {
        unreachable!()
    }
}

#[tokio::test]
async fn test_activate_pending_flow() {
    let source_table_names = vec![TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "activate_source_table",
    )];
    let sink_table_name = TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "activate_sink_table",
    );
    let node_manager = Arc::new(MockFlownodeManager::new(
        crate::ddl::test_util::flownode_handler::NaiveFlownodeHandler,
    ));
    let ddl_context = new_ddl_context(node_manager);

    let flow_id = create_test_flow(
        &ddl_context,
        "activate_pending_flow",
        source_table_names.clone(),
        sink_table_name,
    )
    .await;

    let pending_flow = ddl_context
        .flow_metadata_manager
        .flow_info_manager()
        .get(flow_id)
        .await
        .unwrap()
        .unwrap();
    assert!(pending_flow.is_pending());

    let create_table_task = test_create_table_task("activate_source_table", 1024);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            create_table_task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            Default::default(),
        )
        .await
        .unwrap();

    let mut procedure = ActivatePendingFlowProcedure::new(
        flow_id,
        DEFAULT_CATALOG_NAME.to_string(),
        ddl_context.clone(),
    );
    let output = execute_procedure_until_done(&mut procedure).await.unwrap();
    let activated_flow_id = output.downcast_ref::<u32>().unwrap();
    assert_eq!(*activated_flow_id, flow_id);

    let activated_flow = ddl_context
        .flow_metadata_manager
        .flow_info_manager()
        .get(flow_id)
        .await
        .unwrap()
        .unwrap();
    assert!(activated_flow.is_active());
    assert_eq!(activated_flow.unresolved_source_table_names().len(), 0);
    assert_eq!(activated_flow.source_table_ids(), &[1024]);
    assert_eq!(activated_flow.last_activation_error(), &None);
    assert!(!activated_flow.flownode_ids().is_empty());
}

#[tokio::test]
async fn test_activate_pending_flow_require_streaming_keeps_pending() {
    let source_table_names = vec![TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "instant_ttl_source_table",
    )];
    let sink_table_name = TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "instant_ttl_sink_table",
    );
    let node_manager = Arc::new(MockFlownodeManager::new(
        crate::ddl::test_util::flownode_handler::NaiveFlownodeHandler,
    ));
    let ddl_context = new_ddl_context(node_manager);

    let flow_id = create_test_flow(
        &ddl_context,
        "instant_ttl_pending_flow",
        source_table_names.clone(),
        sink_table_name,
    )
    .await;

    let mut create_table_task = test_create_table_task("instant_ttl_source_table", 1025);
    create_table_task.table_info.meta.options.ttl = Some(TimeToLive::Instant);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            create_table_task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            Default::default(),
        )
        .await
        .unwrap();

    let mut procedure = ActivatePendingFlowProcedure::new(
        flow_id,
        DEFAULT_CATALOG_NAME.to_string(),
        ddl_context.clone(),
    );
    assert!(execute_procedure_until_done(&mut procedure).await.is_none());

    let pending_flow = ddl_context
        .flow_metadata_manager
        .flow_info_manager()
        .get(flow_id)
        .await
        .unwrap()
        .unwrap();
    assert!(pending_flow.is_pending());
    assert_eq!(pending_flow.unresolved_source_table_names().len(), 0);
    assert_eq!(pending_flow.source_table_ids(), &[1025]);
    assert!(pending_flow.flownode_ids().is_empty());
    assert!(
        pending_flow
            .last_activation_error()
            .as_ref()
            .unwrap()
            .contains("requires streaming activation")
    );
    let first_updated_time = *pending_flow.updated_time();

    let mut procedure = ActivatePendingFlowProcedure::new(
        flow_id,
        DEFAULT_CATALOG_NAME.to_string(),
        ddl_context.clone(),
    );
    assert!(execute_procedure_until_done(&mut procedure).await.is_none());

    let pending_flow = ddl_context
        .flow_metadata_manager
        .flow_info_manager()
        .get(flow_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending_flow.updated_time(), &first_updated_time);
}

#[tokio::test]
async fn test_activate_pending_flow_uses_replace_semantics() {
    let source_table_names = vec![TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "activate_replace_source_table",
    )];
    let sink_table_name = TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "activate_replace_sink_table",
    );

    let handler = RecordingFlownodeHandler::default();
    let node_manager = Arc::new(MockFlownodeManager::new(handler.clone()));
    let ddl_context = new_ddl_context(node_manager);

    let flow_id = create_test_flow(
        &ddl_context,
        "activate_pending_flow_replace_semantics",
        source_table_names,
        sink_table_name,
    )
    .await;

    let create_table_task = test_create_table_task("activate_replace_source_table", 1027);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            create_table_task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            Default::default(),
        )
        .await
        .unwrap();

    let mut procedure =
        ActivatePendingFlowProcedure::new(flow_id, DEFAULT_CATALOG_NAME.to_string(), ddl_context);
    let output = execute_procedure_until_done(&mut procedure).await.unwrap();
    let activated_flow_id = output.downcast_ref::<u32>().unwrap();
    assert_eq!(*activated_flow_id, flow_id);

    let create_requests = handler.create_requests.lock().unwrap();
    assert!(!create_requests.is_empty());
    for req in create_requests.iter() {
        assert!(!req.create_if_not_exists);
        assert!(req.or_replace);
    }
}
