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

use std::sync::Arc;
use std::time::Duration;

use client::OutputData;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::lock_key::TableLock;
use common_procedure::{
    LockKey, Procedure, ProcedureId, ProcedureState, ProcedureWithId, Status, watcher,
};
use datatypes::arrow::array::AsArray;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::{execute_sql, try_execute_sql};
use tokio::sync::Notify;

/// Holds the table lock until explicitly released, keeping truncate pending.
struct HoldTruncateTableLock {
    table_id: u32,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl Procedure for HoldTruncateTableLock {
    fn type_name(&self) -> &str {
        "test::HoldTruncateTableLock"
    }

    async fn execute(&mut self, _: &common_procedure::Context) -> common_procedure::Result<Status> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(Status::done())
    }

    fn dump(&self) -> common_procedure::Result<String> {
        Ok(String::new())
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(vec![TableLock::Write(self.table_id).into()])
    }
}

#[rstest_reuse::apply(crate::both_deployment_cases)]
async fn test_truncate_wait_and_timeout(distributed: bool) {
    let cluster = if distributed {
        Some(
            GreptimeDbClusterBuilder::new("truncate_wait_and_timeout")
                .await
                .with_datanodes(1)
                .build(false)
                .await,
        )
    } else {
        None
    };
    let standalone = if distributed {
        None
    } else {
        Some(
            GreptimeDbStandaloneBuilder::new("truncate_wait_and_timeout")
                .build()
                .await,
        )
    };
    let (frontend, manager) = if let Some(cluster) = &cluster {
        (
            cluster.fe_instance().clone(),
            cluster.metasrv.procedure_manager().clone(),
        )
    } else {
        let standalone = standalone.as_ref().unwrap();
        (
            standalone.fe_instance().clone(),
            standalone.procedure_manager.clone(),
        )
    };

    execute_sql(
        &frontend,
        "CREATE TABLE truncate_wait_test (v INT, ts TIMESTAMP TIME INDEX)",
    )
    .await;
    execute_sql(&frontend, "INSERT INTO truncate_wait_test VALUES (1, 1)").await;
    let definition = execute_sql(&frontend, "SHOW CREATE TABLE truncate_wait_test")
        .await
        .data
        .pretty_print()
        .await;
    let table_id = frontend
        .catalog_manager()
        .table("greptime", "public", "truncate_wait_test", None)
        .await
        .unwrap()
        .unwrap()
        .table_info()
        .table_id();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut blocker = manager
        .submit(ProcedureWithId::with_random_id(Box::new(
            HoldTruncateTableLock {
                table_id,
                started: started.clone(),
                release: release.clone(),
            },
        )))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .unwrap();

    let output = execute_sql(
        &frontend,
        "TRUNCATE TABLE truncate_wait_test WITH (wait=false, timeout='20ms')",
    )
    .await;
    let OutputData::RecordBatches(batches) = output.data else {
        panic!("expected Procedure ID result");
    };
    let batch = batches.iter().next().unwrap();
    let id = batch
        .column_by_name("Procedure ID")
        .unwrap()
        .as_string::<i32>()
        .value(0);
    let id = ProcedureId::parse_str(id).unwrap();
    let state = execute_sql(&frontend, &format!("SELECT procedure_state('{id}')"))
        .await
        .data
        .pretty_print()
        .await;
    assert!(state.contains(r#""status":"Running""#), "{state}");

    // The same table lock holds a synchronous truncate past its wait deadline.
    // Standalone exercises the shared timer without any RPC; distributed mode
    // retains the existing rounding up to whole seconds on the wire.
    let err = try_execute_sql(
        &frontend,
        "TRUNCATE TABLE truncate_wait_test WITH (wait=true, timeout='20ms')",
    )
    .await
    .unwrap_err();
    if distributed {
        assert!(matches!(
            err.status_code(),
            StatusCode::DeadlineExceeded | StatusCode::Cancelled
        ));
    } else {
        assert_eq!(err.status_code(), StatusCode::Cancelled);
        assert!(err.output_msg().contains("DDL request timed out"), "{err}");
    }
    let pending = manager
        .list_procedures()
        .await
        .unwrap()
        .into_iter()
        .filter(|procedure| procedure.type_name == "metasrv-procedure::TruncateTable")
        .collect::<Vec<_>>();
    assert_eq!(pending.len(), 2);
    let mut watchers = pending
        .into_iter()
        .map(|procedure| {
            assert!(matches!(procedure.state, ProcedureState::Running));
            assert!(!err.output_msg().contains(&procedure.id.to_string()));
            manager.procedure_watcher(procedure.id).unwrap()
        })
        .collect::<Vec<_>>();
    let before_release = execute_sql(&frontend, "SELECT * FROM truncate_wait_test")
        .await
        .data
        .pretty_print()
        .await;
    assert!(before_release.contains("1970-01-01"), "{before_release}");

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), async {
        watcher::wait(&mut blocker).await.unwrap();
        for watcher in &mut watchers {
            watcher::wait(watcher).await.unwrap();
        }
    })
    .await
    .unwrap();
    let empty = execute_sql(&frontend, "SELECT * FROM truncate_wait_test")
        .await
        .data
        .pretty_print()
        .await;
    assert!(!empty.contains("1970-01-01"), "{empty}");
    assert_eq!(
        definition,
        execute_sql(&frontend, "SHOW CREATE TABLE truncate_wait_test")
            .await
            .data
            .pretty_print()
            .await
    );
}
