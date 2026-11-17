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

use std::collections::HashSet;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_query::Output;
use common_telemetry::{debug, warn};
use session::context::{Channel, QueryContextRef};
use session::table_name::table_idents_to_full_name;
use snafu::{IntoError, ResultExt};
use sql::ast::ObjectNamePartExt;
use sql::statements::copy::{Copy, CopyDatabase};
use sql::statements::statement::Statement;
use sql::util::extract_tables_from_statement_checked;

use crate::error::{
    Error, ExternalSnafu, InvalidSqlSnafu, PermissionSnafu, Result, TableOperationSnafu,
};
use crate::instance::Instance;

impl Instance {
    /// Filters unauthorized candidates before selecting an existing schema.
    pub(crate) async fn set_search_path(
        &self,
        schemas: Vec<String>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let mut authorized = Vec::with_capacity(schemas.len());
        let provider = self.plugins.get::<auth::UserProviderRef>();
        let user = query_ctx.current_user();
        for schema in schemas {
            let schema = if schema == "$user" {
                user.username().to_string()
            } else {
                schema
            };
            if schema.is_empty() {
                continue;
            }
            if let Some(provider) = &provider {
                match provider
                    .authorize(query_ctx.current_catalog(), &schema, &user)
                    .await
                {
                    Ok(()) => {}
                    Err(error)
                        if matches!(
                            error.status_code(),
                            StatusCode::PermissionDenied | StatusCode::AccessDenied
                        ) =>
                    {
                        debug!(user = user.username(), protocol = %query_ctx.channel(),
                            catalog = query_ctx.current_catalog(), schema = %schema,
                            process_id = query_ctx.process_id(), reason = %error.status_code(),
                            "Skipping unauthorized search_path candidate");
                        continue;
                    }
                    Err(error) => return Err(error).context(PermissionSnafu),
                }
            }
            authorized.push(schema);
        }
        self.statement_executor
            .set_search_path(authorized, query_ctx)
            .await
            .context(TableOperationSnafu)
    }

    /// Authorizes an explicit database access without changing the session.
    pub(crate) async fn authorize_database(
        &self,
        catalog: &str,
        schema: &str,
        query_ctx: &QueryContextRef,
    ) -> Result<()> {
        let Some(provider) = self.plugins.get::<auth::UserProviderRef>() else {
            return Ok(());
        };
        let user = query_ctx.current_user();
        if let Err(error) = provider.authorize(catalog, schema, &user).await {
            if matches!(
                error.status_code(),
                StatusCode::PermissionDenied | StatusCode::AccessDenied
            ) {
                warn!(user = user.username(), protocol = %query_ctx.channel(), catalog, schema,
                    previous_schema = %query_ctx.current_schema(), process_id = query_ctx.process_id(),
                    reason = %error.status_code(), "Database access denied");
            }
            return Err(error).context(PermissionSnafu);
        }
        Ok(())
    }

    /// Checks explicit targets before planning when no default scope is authorized.
    pub(crate) async fn check_unselected_mysql_database(
        &self,
        stmt: &Statement,
        query_ctx: &QueryContextRef,
    ) -> Result<()> {
        if query_ctx.channel() != Channel::Mysql || !query_ctx.current_schema().is_empty() {
            return Ok(());
        }
        for (catalog, schema) in resolve_mysql_database_scopes(stmt, query_ctx)? {
            self.authorize_database(&catalog, &schema, query_ctx)
                .await?;
        }
        Ok(())
    }
}

fn resolve_mysql_database_scopes(
    stmt: &Statement,
    query_ctx: &QueryContextRef,
) -> Result<HashSet<(String, String)>> {
    let mut stmt = stmt;
    while let Statement::Explain(explain) = stmt {
        stmt = &explain.statement;
    }
    let mut scopes = HashSet::new();
    let tables = extract_tables_from_statement_checked(stmt).ok_or_else(no_database_selected)?;
    for table in tables {
        let (catalog, schema, _) = table_idents_to_full_name(&table, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        if schema.is_empty() {
            return Err(no_database_selected());
        }
        scopes.insert((catalog, schema));
    }
    if let Some(scope) = resolve_statement_database_scope(stmt, query_ctx)? {
        scopes.insert(scope);
    }
    if scopes.is_empty()
        && !matches!(
            stmt,
            Statement::Query(_)
                | Statement::Use(_)
                | Statement::SetVariables(_)
                | Statement::ShowVariables(_)
                | Statement::ShowDatabases(_)
                | Statement::ShowCharset(_)
                | Statement::ShowCollation(_)
                | Statement::ShowStatus(_)
                | Statement::ShowProcesslist(_)
                | Statement::Kill(_)
        )
    {
        return Err(no_database_selected());
    }
    Ok(scopes)
}

fn resolve_statement_database_scope(
    stmt: &Statement,
    query_ctx: &QueryContextRef,
) -> Result<Option<(String, String)>> {
    let database = match stmt {
        Statement::ShowTables(show) => Some(show.database.as_deref()),
        Statement::ShowTableStatus(show) => Some(show.database.as_deref()),
        Statement::ShowViews(show) => Some(show.database.as_deref()),
        _ => None,
    };
    if let Some(database) = database {
        let schema = database
            .filter(|schema| !schema.is_empty())
            .ok_or_else(no_database_selected)?;
        return Ok(Some((
            query_ctx.current_catalog().to_string(),
            schema.to_string(),
        )));
    }
    let name = match stmt {
        Statement::CreateDatabase(create) => &create.name,
        Statement::AlterDatabase(alter) => alter.database_name(),
        Statement::DropDatabase(drop) => drop.name(),
        Statement::ShowCreateDatabase(show) => &show.database_name,
        Statement::Copy(Copy::CopyDatabase(copy)) => match copy {
            CopyDatabase::To(args) | CopyDatabase::From(args) => &args.database_name,
        },
        _ => return Ok(None),
    };
    let scope = match &name.0[..] {
        [schema] => (
            query_ctx.current_catalog().to_string(),
            schema.to_string_unquoted(),
        ),
        [catalog, schema] => (catalog.to_string_unquoted(), schema.to_string_unquoted()),
        _ => {
            return InvalidSqlSnafu {
                err_msg: format!("Invalid database name: {name}"),
            }
            .fail();
        }
    };
    if scope.1.is_empty() {
        return Err(no_database_selected());
    }
    Ok(Some(scope))
}

fn no_database_selected() -> Error {
    ExternalSnafu.into_error(BoxedError::new(
        session::SessionError::NoDatabaseSelected {},
    ))
}
