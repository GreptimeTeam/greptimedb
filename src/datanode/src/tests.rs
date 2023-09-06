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

use std::any::Any;
use std::sync::Arc;

use api::v1::meta::HeartbeatResponse;
use async_trait::async_trait;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_function::scalars::FunctionRef;
use common_meta::heartbeat::handler::{
    HeartbeatResponseHandlerContext, HeartbeatResponseHandlerExecutor,
};
use common_meta::heartbeat::mailbox::{HeartbeatMailbox, MessageMeta};
use common_meta::ident::TableIdent;
use common_meta::instruction::{Instruction, InstructionReply, RegionIdent};
use common_query::prelude::ScalarUdf;
use common_query::Output;
use common_runtime::Runtime;
use query::dataframe::DataFrame;
use query::plan::LogicalPlan;
use query::planner::LogicalPlanner;
use query::query_engine::DescribeResult;
use query::QueryEngine;
use session::context::QueryContextRef;
use table::TableRef;
use tokio::sync::mpsc::{self, Receiver};

use crate::region_server::RegionServer;
use crate::Instance;

struct HandlerTestGuard {
    instance: Instance,
    mailbox: Arc<HeartbeatMailbox>,
    rx: Receiver<(MessageMeta, InstructionReply)>,
}

async fn prepare_handler_test(_name: &str) -> HandlerTestGuard {
    let instance = Instance;
    let (tx, rx) = mpsc::channel(8);
    let mailbox = Arc::new(HeartbeatMailbox::new(tx));

    HandlerTestGuard {
        instance,
        mailbox,
        rx,
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

async fn handle_instruction(
    executor: Arc<dyn HeartbeatResponseHandlerExecutor>,
    mailbox: Arc<HeartbeatMailbox>,
    instruction: Instruction,
) {
    let response = HeartbeatResponse::default();
    let mut ctx: HeartbeatResponseHandlerContext =
        HeartbeatResponseHandlerContext::new(mailbox, response);
    ctx.incoming_message = Some((test_message_meta(1, "hi", "foo", "bar"), instruction));
    executor.handle(ctx).await.unwrap();
}

fn close_region_instruction() -> Instruction {
    Instruction::CloseRegion(RegionIdent {
        table_ident: TableIdent {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            table: "demo".to_string(),
            table_id: 1024,
            engine: "mito".to_string(),
        },
        region_number: 0,
        cluster_id: 1,
        datanode_id: 2,
    })
}

fn open_region_instruction() -> Instruction {
    Instruction::OpenRegion(RegionIdent {
        table_ident: TableIdent {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            table: "demo".to_string(),
            table_id: 1024,
            engine: "mito".to_string(),
        },
        region_number: 0,
        cluster_id: 1,
        datanode_id: 2,
    })
}

pub struct MockQueryEngine;

#[async_trait]
impl QueryEngine for MockQueryEngine {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn planner(&self) -> Arc<dyn LogicalPlanner> {
        unimplemented!()
    }

    fn name(&self) -> &str {
        "MockQueryEngine"
    }

    async fn describe(&self, _plan: LogicalPlan) -> query::error::Result<DescribeResult> {
        unimplemented!()
    }

    async fn execute(
        &self,
        _plan: LogicalPlan,
        _query_ctx: QueryContextRef,
    ) -> query::error::Result<Output> {
        unimplemented!()
    }

    fn register_udf(&self, _udf: ScalarUdf) {}

    fn register_aggregate_function(&self, _func: AggregateFunctionMetaRef) {}

    fn register_function(&self, _func: FunctionRef) {}

    fn read_table(&self, _table: TableRef) -> query::error::Result<DataFrame> {
        unimplemented!()
    }
}

/// Create a region server without any engine
pub fn mock_region_server() -> RegionServer {
    RegionServer::new(
        Arc::new(MockQueryEngine),
        Arc::new(Runtime::builder().build().unwrap()),
    )
}
