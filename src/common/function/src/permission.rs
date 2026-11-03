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

use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionResp};
use common_error::ext::BoxedError;
use common_query::error::PermissionDeniedSnafu;
use datafusion_common::DataFusionError;
use sql::ast::{FunctionArguments, Ident, ObjectName};
use sql::statements::admin::Admin;
use sql::statements::statement::Statement;

use crate::function::FunctionContext;

pub fn ensure_admin_privilege(
    func_ctx: &FunctionContext,
    fn_name: &str,
) -> datafusion_common::Result<()> {
    let admin_stmt = Statement::Admin(Admin::Func(sql::ast::Function {
        name: ObjectName::from(vec![Ident::new(fn_name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::None,
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }));

    let checker = func_ctx.state.plugins.get::<PermissionCheckerRef>();
    match checker.as_ref().check_permission_with_context(
        func_ctx.query_ctx.current_user(),
        PermissionReq::SqlStatement(&admin_stmt),
        Some(&func_ctx.query_ctx.current_schema()),
    ) {
        Ok(PermissionResp::Allow) => Ok(()),
        Ok(PermissionResp::Reject) | Err(_) => {
            let err = PermissionDeniedSnafu {
                err_msg: format!(
                    "Executing the admin function '{fn_name}' requires the privileges of an ADMIN statement"
                ),
            }
            .build();
            Err(DataFusionError::External(Box::new(BoxedError::new(err))))
        }
    }
}

#[cfg(test)]
pub(crate) fn test_function_context(
    plugins: common_base::Plugins,
    user: auth::UserInfoRef,
) -> FunctionContext {
    let query_ctx = session::context::QueryContext::arc();
    query_ctx.set_current_user(user);
    FunctionContext {
        query_ctx,
        state: std::sync::Arc::new(crate::state::FunctionState {
            plugins,
            ..Default::default()
        }),
    }
}

#[cfg(test)]
mod tests {
    use auth::{
        DefaultPermissionChecker, Identity, Password, PermissionCheckerRef, UserProvider,
        static_user_provider_from_option,
    };
    use common_base::Plugins;
    use common_error::status_code::StatusCode;

    use super::*;

    async fn user(username: &str, mode: &str) -> auth::UserInfoRef {
        let provider = static_user_provider_from_option(&format!(
            "static_user_provider:cmd:{username}:{mode}={username}_pwd"
        ))
        .unwrap();
        provider
            .authenticate(
                Identity::UserId(username, None),
                Password::PlainText(format!("{username}_pwd").into()),
            )
            .await
            .unwrap()
    }

    fn denied_status(error: &DataFusionError) -> StatusCode {
        common_query::error::datafusion_status_code::<common_query::error::Error>(error, None)
    }

    #[tokio::test]
    async fn test_admin_privilege_without_checker_allows() {
        let func_ctx = test_function_context(Plugins::default(), user("anyone", "ro").await);
        ensure_admin_privilege(&func_ctx, "flush_table").unwrap();
    }

    #[tokio::test]
    async fn test_admin_privilege_readonly_user_denied() {
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(DefaultPermissionChecker::arc());
        let func_ctx = test_function_context(plugins, user("ro_user", "ro").await);

        let error = ensure_admin_privilege(&func_ctx, "flush_table")
            .expect_err("read-only user must be denied");
        assert_eq!(StatusCode::PermissionDenied, denied_status(&error));
    }

    #[tokio::test]
    async fn test_admin_privilege_readwrite_user_allowed() {
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(DefaultPermissionChecker::arc());
        let func_ctx = test_function_context(plugins, user("rw_user", "rw").await);

        ensure_admin_privilege(&func_ctx, "flush_table").unwrap();
    }
}
