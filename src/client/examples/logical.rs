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

use api::v1::{ColumnDataType, ColumnDef, CreateTableExpr, SemanticType, TableId};
use client::{Client, Database};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
use prost::Message;
use substrait_proto::proto::plan_rel::RelType as PlanRelType;
use substrait_proto::proto::read_rel::{NamedTable, ReadType};
use substrait_proto::proto::rel::RelType;
use substrait_proto::proto::{PlanRel, ReadRel, Rel};
use tracing::{event, Level};

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let client = Client::with_urls(vec!["127.0.0.1:3001"]);

    let create_table_expr = CreateTableExpr {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: "test_logical_dist_exec".to_string(),
        desc: "".to_string(),
        column_defs: vec![
            ColumnDef {
                name: "timestamp".to_string(),
                data_type: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "key".to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Tag as i32,
                comment: String::new(),
                ..Default::default()
            },
            ColumnDef {
                name: "value".to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                ..Default::default()
            },
        ],
        time_index: "timestamp".to_string(),
        primary_keys: vec!["key".to_string()],
        create_if_not_exists: false,
        table_options: Default::default(),
        table_id: Some(TableId { id: 1024 }),
        engine: MITO_ENGINE.to_string(),
    };

    let db = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
    let result = db.create(create_table_expr).await.unwrap();
    event!(Level::INFO, "create table result: {:#?}", result);

    let logical = mock_logical_plan();
    event!(Level::INFO, "plan size: {:#?}", logical.len());
    let result = db.logical_plan(logical).await.unwrap();

    event!(Level::INFO, "result: {:#?}", result);
}

fn mock_logical_plan() -> Vec<u8> {
    let catalog_name = "greptime".to_string();
    let schema_name = "public".to_string();
    let table_name = "test_logical_dist_exec".to_string();

    let named_table = NamedTable {
        names: vec![catalog_name, schema_name, table_name],
        advanced_extension: None,
    };
    let read_type = ReadType::NamedTable(named_table);

    let read_rel = ReadRel {
        read_type: Some(read_type),
        ..Default::default()
    };

    let mut buf = vec![];
    let rel = Rel {
        rel_type: Some(RelType::Read(Box::new(read_rel))),
    };
    let plan_rel = PlanRel {
        rel_type: Some(PlanRelType::Rel(rel)),
    };
    plan_rel.encode(&mut buf).unwrap();

    buf
}
