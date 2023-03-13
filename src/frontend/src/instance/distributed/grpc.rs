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

use alter_expr::Kind;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::{alter_expr, AddColumn, AddColumns, AlterExpr, DropColumn, DropColumns, RenameTable};
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::Output;
use datanode::instance::sql::table_idents_to_full_name;
use meta_client::rpc::TableName;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::statements::alter::{AlterTable, AlterTableOperation};
use sql::statements::sql_column_def_to_grpc_column_def;

use crate::error::{self, ExternalSnafu, Result};
use crate::instance::distributed::DistInstance;

#[async_trait]
impl GrpcQueryHandler for DistInstance {
    type Error = error::Error;

    async fn do_query(&self, request: Request, ctx: QueryContextRef) -> Result<Output> {
        match request {
            Request::Insert(request) => self.handle_dist_insert(request, ctx).await,
            Request::Query(_) => {
                unreachable!("Query should have been handled directly in Frontend Instance!")
            }
            Request::Ddl(request) => {
                let expr = request.expr.context(error::IncompleteGrpcResultSnafu {
                    err_msg: "Missing 'expr' in DDL request",
                })?;
                match expr {
                    DdlExpr::CreateDatabase(expr) => self.handle_create_database(expr, ctx).await,
                    DdlExpr::CreateTable(mut expr) => {
                        // TODO(LFC): Support creating distributed table through GRPC interface.
                        // Currently only SQL supports it; how to design the fields in CreateTableExpr?
                        self.create_table(&mut expr, None).await
                    }
                    DdlExpr::Alter(expr) => self.handle_alter_table(expr).await,
                    DdlExpr::DropTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.drop_table(table_name).await
                    }
                    DdlExpr::FlushTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.flush_table(table_name, expr.region_id).await
                    }
                }
            }
        }
    }
}

pub(crate) fn to_alter_expr(
    alter_table: AlterTable,
    query_ctx: QueryContextRef,
) -> Result<AlterExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(alter_table.table_name(), query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let kind = match alter_table.alter_operation() {
        AlterTableOperation::AddConstraint(_) => {
            return error::NotSupportedSnafu {
                feat: "ADD CONSTRAINT",
            }
            .fail();
        }
        AlterTableOperation::AddColumn { column_def } => Kind::AddColumns(AddColumns {
            add_columns: vec![AddColumn {
                column_def: Some(
                    sql_column_def_to_grpc_column_def(column_def)
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?,
                ),
                is_key: false,
            }],
        }),
        AlterTableOperation::DropColumn { name } => Kind::DropColumns(DropColumns {
            drop_columns: vec![DropColumn {
                name: name.value.to_string(),
            }],
        }),
        AlterTableOperation::RenameTable { new_table_name } => Kind::RenameTable(RenameTable {
            new_table_name: new_table_name.to_string(),
        }),
    };

    Ok(AlterExpr {
        catalog_name,
        schema_name,
        table_name,
        kind: Some(kind),
    })
}
