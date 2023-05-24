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

use std::assert_matches::assert_matches;
use std::sync::Arc;

use api::v1::greptime_request::Request as GrpcRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::query_request::Query;
use api::v1::QueryRequest;
use catalog::CatalogManagerRef;
use common_meta::heartbeat::handler::{
    HandlerGroupExecutor, HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutor,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MessageMeta};
use common_meta::instruction::{Instruction, InstructionReply, RegionIdent, SimpleReply};
use common_query::Output;
use datatypes::prelude::ConcreteDataType;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContext;
use table::engine::manager::TableEngineManagerRef;
use test_util::MockInstance;
use tokio::sync::mpsc::{self, Receiver};

use crate::heartbeat::handler::close_region::CloseRegionHandler;
use crate::heartbeat::handler::open_region::OpenRegionHandler;
use crate::instance::Instance;

pub(crate) mod test_util;

struct HandlerTestGuard {
    instance: MockInstance,
    mailbox: Arc<HeartbeatMailbox>,
    rx: Receiver<(MessageMeta, InstructionReply)>,
    engine_manager_ref: TableEngineManagerRef,
    catalog_manager_ref: CatalogManagerRef,
}

#[tokio::test]
async fn test_close_region_handler() {
    let HandlerTestGuard {
        instance,
        mailbox,
        mut rx,
        engine_manager_ref,
        catalog_manager_ref,
        ..
    } = parepare_handler_test("test_close_region_handler").await;

    let executor = Arc::new(HandlerGroupExecutor::new(vec![Arc::new(
        CloseRegionHandler::new(catalog_manager_ref.clone(), engine_manager_ref.clone()),
    )]));

    parepare_table(instance.inner()).await;

    // Closes demo table
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        close_region_instruction(),
    );
    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::CloseRegion(SimpleReply { result: true, .. })
    );

    assert_test_table_not_found(instance.inner()).await;

    // Closes demo table again
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        close_region_instruction(),
    );
    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::CloseRegion(SimpleReply { result: true, .. })
    );

    // Closes non-exist table
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        Instruction::CloseRegion(RegionIdent {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            table: "non-exist".to_string(),
            table_id: 1025,
            engine: "mito".to_string(),
            region_number: 0,
            cluster_id: 1,
            datanode_id: 2,
        }),
    );
    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::CloseRegion(SimpleReply { result: true, .. })
    );
}

#[tokio::test]
async fn test_open_region_handler() {
    let HandlerTestGuard {
        instance,
        mailbox,
        mut rx,
        engine_manager_ref,
        catalog_manager_ref,
        ..
    } = parepare_handler_test("test_open_region_handler").await;

    let executor = Arc::new(HandlerGroupExecutor::new(vec![
        Arc::new(OpenRegionHandler::new(
            catalog_manager_ref.clone(),
            engine_manager_ref.clone(),
        )),
        Arc::new(CloseRegionHandler::new(
            catalog_manager_ref.clone(),
            engine_manager_ref.clone(),
        )),
    ]));

    parepare_table(instance.inner()).await;

    // Opens a opened table
    handle_instruction(executor.clone(), mailbox.clone(), open_region_instruction());
    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::OpenRegion(SimpleReply { result: true, .. })
    );

    // Opens a non-exist table
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        Instruction::OpenRegion(RegionIdent {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            table: "non-exist".to_string(),
            table_id: 2024,
            engine: "mito".to_string(),
            region_number: 0,
            cluster_id: 1,
            datanode_id: 2,
        }),
    );
    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::OpenRegion(SimpleReply { result: false, .. })
    );

    // Closes demo table
    handle_instruction(
        executor.clone(),
        mailbox.clone(),
        close_region_instruction(),
    );
    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::CloseRegion(SimpleReply { result: true, .. })
    );
    assert_test_table_not_found(instance.inner()).await;

    // Opens demo table
    handle_instruction(executor.clone(), mailbox.clone(), open_region_instruction());
    let (_, reply) = rx.recv().await.unwrap();
    assert_matches!(
        reply,
        InstructionReply::OpenRegion(SimpleReply { result: true, .. })
    );
    assert_test_table_found(instance.inner()).await;
}

async fn parepare_handler_test(name: &str) -> HandlerTestGuard {
    let mock_instance = MockInstance::new(name).await;
    let instance = mock_instance.inner();
    let engine_manager = instance.sql_handler().table_engine_manager().clone();
    let catalog_manager = instance.sql_handler().catalog_manager().clone();
    let (tx, rx) = mpsc::channel(8);
    let mailbox = Arc::new(HeartbeatMailbox::new(tx));

    HandlerTestGuard {
        instance: mock_instance,
        mailbox,
        rx,
        engine_manager_ref: engine_manager,
        catalog_manager_ref: catalog_manager,
    }
}

pub fn test_message_meta(id: u64, subject: &str, to: &str, from: &str) -> MessageMeta {
    MessageMeta {
        id,
        subject: subject.to_string(),
        to: to.to_string(),
        from: from.to_string(),
    }
}

fn handle_instruction(
    executor: Arc<dyn HeartbeatResponseHandlerExecutor>,
    mailbox: Arc<HeartbeatMailbox>,
    instruction: Instruction,
) {
    let response = HeartbeatResponse::default();
    let mut ctx: HeartbeatResponseHandlerContext =
        HeartbeatResponseHandlerContext::new(mailbox, response);
    ctx.incoming_message = Some((test_message_meta(1, "hi", "foo", "bar"), instruction));
    executor.handle(ctx).unwrap();
}

fn close_region_instruction() -> Instruction {
    Instruction::CloseRegion(RegionIdent {
        catalog: "greptime".to_string(),
        schema: "public".to_string(),
        table: "demo".to_string(),
        table_id: 1024,
        engine: "mito".to_string(),
        region_number: 0,
        cluster_id: 1,
        datanode_id: 2,
    })
}

fn open_region_instruction() -> Instruction {
    Instruction::OpenRegion(RegionIdent {
        catalog: "greptime".to_string(),
        schema: "public".to_string(),
        table: "demo".to_string(),
        table_id: 1024,
        engine: "mito".to_string(),
        region_number: 0,
        cluster_id: 1,
        datanode_id: 2,
    })
}

async fn parepare_table(instance: &Instance) {
    test_util::create_test_table(instance, ConcreteDataType::timestamp_millisecond_datatype())
        .await
        .unwrap();
}

async fn assert_test_table_not_found(instance: &Instance) {
    let query = GrpcRequest::Query(QueryRequest {
        query: Some(Query::Sql(
            "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                        ('host1', 66.6, 1024, 1672201025000),\
                        ('host2', 88.8, 333.3, 1672201026000)"
                .to_string(),
        )),
    });
    let output = instance
        .do_query(query, QueryContext::arc())
        .await
        .unwrap_err();

    assert_eq!(output.to_string(), "Failed to execute sql, source: Failure during query execution, source: Table not found: greptime.public.demo");
}

async fn assert_test_table_found(instance: &Instance) {
    let query = GrpcRequest::Query(QueryRequest {
        query: Some(Query::Sql(
            "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                        ('host1', 66.6, 1024, 1672201025000),\
                        ('host2', 88.8, 333.3, 1672201026000)"
                .to_string(),
        )),
    });
    let output = instance.do_query(query, QueryContext::arc()).await.unwrap();

    assert!(matches!(output, Output::AffectedRows(2)));
}
