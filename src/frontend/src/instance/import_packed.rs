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

use auth::{PermissionReq, PermissionTableTarget, PermissionTableTargets};
use common_query::Output;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::copy::CopyDatabaseArgument;
use sql::statements::statement::Statement;
use tokio_util::sync::CancellationToken;

use crate::error::{PermissionSnafu, Result};
use crate::instance::Instance;

impl Instance {
    pub(crate) async fn copy_packed_database(
        &self,
        arg: CopyDatabaseArgument,
        stmt: &Statement,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let req = operator::statement::to_copy_database_request(arg, &ctx)?;
        let plan = self
            .statement_executor
            .prepare_packed_import(req, &ctx)
            .await?;
        let targets = PermissionTableTargets::resolved(
            plan.tables
                .iter()
                .map(|t| {
                    let info = t.table_info();
                    PermissionTableTarget::new(&info.catalog_name, &info.schema_name, &info.name)
                })
                .collect(),
        );
        self.check_table_permission(&ctx, PermissionReq::SqlStatement(stmt), targets)
            .context(PermissionSnafu)?;
        let executor = self.statement_executor.clone();
        run_packed_import(move |cancellation| async move {
            executor.import_packed(plan, &cancellation, ctx).await
        })
        .await
    }
}

async fn run_packed_import<F, Fut>(worker: F) -> Result<Output>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: std::future::Future<Output = operator::error::Result<Output>> + Send + 'static,
{
    let cancellation = CancellationToken::new();
    let _guard = cancellation.clone().drop_guard();
    // Transport timeout/drop signals cancellation; the worker retains ownership
    // until its started inserts finish, even after the response is gone.
    common_runtime::spawn_query(worker(cancellation))
        .await
        .context(operator::error::JoinTaskSnafu)?
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn packed_request_drop_keeps_worker_alive_until_cleanup() {
        let (started_tx, started_rx) = oneshot::channel();
        let (cancelled_tx, cancelled_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();
        let (finished_tx, mut finished_rx) = oneshot::channel();
        let mut request = Box::pin(run_packed_import(move |cancellation| async move {
            started_tx.send(()).unwrap();
            cancellation.cancelled().await;
            cancelled_tx.send(()).unwrap();
            // Model an already-started insert that must finish before worker exit.
            drain_rx.await.unwrap();
            finished_tx.send(()).unwrap();
            Ok(Output::new_with_affected_rows(1))
        }));
        tokio::select! {
            _ = started_rx => {}
            _ = &mut request => panic!("worker must wait for cancellation"),
        }
        drop(request);
        tokio::time::timeout(Duration::from_secs(5), cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            finished_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drain_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), finished_rx)
            .await
            .unwrap()
            .unwrap();
    }
}
