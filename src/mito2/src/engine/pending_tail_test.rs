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

use api::v1::region::StrictWindow;
use api::v1::region::compact_request::Options;
use api::v1::{Rows, SemanticType};
use common_error::ext::{BoxedError, ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use rstest::rstest;
use store_api::logstore::provider::RaftEngineProvider;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AddColumn, AlterKind, EnterStagingRequest, RegionAlterRequest, RegionCloseRequest,
    RegionCompactRequest, RegionCreateRequest, RegionDropRequest, RegionFlushReason,
    RegionFlushRequest, RegionRequest, RegionTruncateRequest, SetRegionOption,
    StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::error::Error;
use crate::manifest::action::{
    HandoffFloor, PendingTail, RegionEdit, RegionMetaAction, RegionMetaActionList, UptoKind,
    WalRecoveryEdit,
};
use crate::region::RegionLeaderState;
use crate::test_util::{
    CreateRequestBuilder, LogStoreImpl, TestEnv, build_rows, flush_region, put_rows, reopen_region,
    rows_schema,
};
use crate::wal::EntryId;

const NODE_ID: u64 = 1;

pub(crate) fn tail(node_id: u64, after: EntryId, upto: EntryId) -> PendingTail {
    PendingTail {
        node_id,
        after,
        upto,
        upto_kind: UptoKind::Reported,
    }
}

/// Creates a region and writes one row in each of WAL entries `1..=num_entries`.
async fn create_region_with_entries(
    engine: &MitoEngine,
    region_id: RegionId,
    num_entries: usize,
) -> RegionCreateRequest {
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    for i in 0..num_entries {
        let rows = Rows {
            schema: schema.clone(),
            rows: build_rows(i, i + 1),
        };
        put_rows(engine, region_id, rows).await;
    }
    request
}

pub(crate) fn pending_tails_edit(pending_tails: Vec<PendingTail>) -> RegionEdit {
    RegionEdit {
        files_to_add: vec![],
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
        wal_recovery: WalRecoveryEdit {
            pending_tails: Some(pending_tails),
            handoff_floor: None,
        },
    }
}

async fn commit_edit(engine: &MitoEngine, region_id: RegionId, edit: RegionEdit) {
    let region = engine.get_region(region_id).unwrap();
    region
        .manifest_ctx
        .update_manifest(
            RegionLeaderState::Writable,
            RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone())),
            false,
        )
        .await
        .unwrap();
    region
        .version_control
        .apply_edit(Some(edit), &[], region.file_purger.clone());
}

pub(crate) async fn set_pending_tails(
    engine: &MitoEngine,
    region_id: RegionId,
    pending_tails: Vec<PendingTail>,
) {
    commit_edit(engine, region_id, pending_tails_edit(pending_tails)).await;
}

async fn reopen(engine: &MitoEngine, region_id: RegionId, request: RegionCreateRequest) {
    reopen_region(engine, region_id, request.table_dir, true, request.options).await;
}

fn wal_span(env: &TestEnv, region_id: RegionId) -> (Option<EntryId>, Option<EntryId>) {
    let Some(LogStoreImpl::RaftEngine(log_store)) = env.get_log_store() else {
        unreachable!()
    };
    log_store.span(&RaftEngineProvider::new(region_id.into()))
}

#[rstest]
#[case::own_tail(vec![tail(NODE_ID, 0, 2)], None, 1)]
#[case::other_node_tail(vec![tail(NODE_ID + 1, 0, 2)], None, 3)]
#[case::own_floor(vec![], Some(NODE_ID), 1)]
#[case::other_node_floor(vec![], Some(NODE_ID + 1), 3)]
#[tokio::test]
async fn test_open_replays_wal_after_own_tails_and_floor(
    #[case] pending_tails: Vec<PendingTail>,
    #[case] floor_node_id: Option<u64>,
    #[case] replayed_rows: u64,
) {
    let mut env = TestEnv::new().await.with_node_id(NODE_ID);
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_region_with_entries(&engine, region_id, 3).await;
    let has_pending_tails = !pending_tails.is_empty();
    let mut edit = pending_tails_edit(pending_tails);
    edit.wal_recovery.handoff_floor = floor_node_id.map(|node_id| HandoffFloor {
        node_id,
        entry_id: 2,
    });
    commit_edit(&engine, region_id, edit).await;

    reopen(&engine, region_id, request).await;

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(replayed_rows, region.version().memtables.num_rows());
    let scan = engine.scanner(region_id, ScanRequest::default()).await;
    assert_eq!(
        has_pending_tails,
        matches!(scan, Err(Error::RegionPendingTails { .. }))
    );
}

#[tokio::test]
async fn test_open_keeps_last_entry_id_at_flushed_entry_id() {
    let mut env = TestEnv::new().await.with_node_id(NODE_ID);
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_region_with_entries(&engine, region_id, 3).await;
    flush_region(&engine, region_id, None).await;
    let listed = PendingTail {
        node_id: NODE_ID,
        after: 3,
        upto: 100,
        upto_kind: UptoKind::Listed,
    };
    set_pending_tails(&engine, region_id, vec![listed]).await;

    reopen(&engine, region_id, request).await;

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(3, region.version_control.current().last_entry_id);
}

#[tokio::test]
async fn test_flush_keeps_wal_after_own_tails() {
    let mut env = TestEnv::new().await.with_node_id(NODE_ID);
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    create_region_with_entries(&engine, region_id, 3).await;
    set_pending_tails(&engine, region_id, vec![tail(NODE_ID, 1, 2)]).await;

    flush_region(&engine, region_id, None).await;

    assert_eq!((Some(2), Some(3)), wal_span(&env, region_id));
}

#[tokio::test]
async fn test_open_keeps_wal_after_own_tails() {
    let mut env = TestEnv::new().await.with_node_id(NODE_ID);
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_region_with_entries(&engine, region_id, 3).await;
    set_pending_tails(&engine, region_id, vec![tail(NODE_ID, 1, 3)]).await;

    reopen(&engine, region_id, request).await;

    assert_eq!((Some(2), Some(3)), wal_span(&env, region_id));
}

#[tokio::test]
async fn test_drop_releases_wal_with_pending_tails() {
    let mut env = TestEnv::new().await.with_node_id(NODE_ID);
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    create_region_with_entries(&engine, region_id, 3).await;
    set_pending_tails(&engine, region_id, vec![tail(NODE_ID, 0, 3)]).await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
        .unwrap();

    assert_eq!((None, None), wal_span(&env, region_id));
}

/// Creates a region with rows in WAL entries `1..=num_entries` and one pending tail.
async fn region_with_pending_tails(num_entries: usize) -> (TestEnv, MitoEngine, RegionId) {
    let mut env = TestEnv::new().await.with_node_id(NODE_ID);
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    create_region_with_entries(&engine, region_id, num_entries).await;
    set_pending_tails(&engine, region_id, vec![tail(NODE_ID, 0, 1)]).await;
    (env, engine, region_id)
}

fn assert_pending_tails_error(err: &BoxedError) {
    let err = err.as_any().downcast_ref::<Error>();
    assert!(
        matches!(err, Some(Error::RegionPendingTails { .. })),
        "{err:?}"
    );
}

fn add_field(default_constraint: Option<ColumnDefaultConstraint>) -> AlterKind {
    let column_schema = ColumnSchema::new("field_1", ConcreteDataType::float64_datatype(), true)
        .with_default_constraint(default_constraint)
        .unwrap();
    AlterKind::AddColumns {
        columns: vec![AddColumn {
            column_metadata: ColumnMetadata {
                column_schema,
                semantic_type: SemanticType::Field,
                column_id: 10,
            },
            location: None,
        }],
    }
}

#[tokio::test]
async fn test_scan_rejects_region_with_pending_tails() {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;

    let Err(err) = engine.scanner(region_id, ScanRequest::default()).await else {
        panic!("scan succeeded with pending tails");
    };

    assert!(matches!(err, Error::RegionPendingTails { .. }), "{err:?}");
    assert_eq!(StatusCode::RegionNotReady, err.status_code());
    assert_eq!(RetryHint::Retryable, err.retry_hint());
}

#[tokio::test]
async fn test_memtable_and_sst_stats_reject_region_with_pending_tails() {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;

    let result = engine.find_memtable_and_sst_stats(region_id);

    assert!(matches!(result, Err(Error::RegionPendingTails { .. })));
}

#[tokio::test]
async fn test_manual_compaction_skips_region_with_pending_tails() {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;
    flush_region(&engine, region_id, None).await;
    let rows = Rows {
        schema: rows_schema(&CreateRequestBuilder::new().build()),
        rows: build_rows(1, 2),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    let request = RegionCompactRequest {
        options: Options::StrictWindow(StrictWindow {
            window_seconds: 3600,
        }),
        ..Default::default()
    };
    engine
        .handle_request(region_id, RegionRequest::Compact(request))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(2, region.version().ssts.owned_num_files(region_id));
}

#[rstest]
#[case::all(RegionTruncateRequest::All)]
#[case::unflushed(RegionTruncateRequest::Unflushed)]
#[case::by_time_ranges(RegionTruncateRequest::ByTimeRanges {
    time_ranges: vec![(Timestamp::new_millisecond(0), Timestamp::new_millisecond(1000))],
})]
#[tokio::test]
async fn test_truncate_rejects_region_with_pending_tails(#[case] request: RegionTruncateRequest) {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;

    let err = engine
        .handle_request(region_id, RegionRequest::Truncate(request))
        .await
        .unwrap_err();

    assert_pending_tails_error(&err);
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(1, region.version().memtables.num_rows());
}

#[rstest]
#[case::nullable_column(add_field(None), true)]
#[case::region_options(AlterKind::SetRegionOptions {
    options: vec![SetRegionOption::AppendMode(true)],
}, true)]
#[case::default_null_column(add_field(Some(ColumnDefaultConstraint::null_value())), false)]
#[case::drop_column(AlterKind::DropColumns {
    names: vec!["field_0".to_string()],
}, false)]
#[tokio::test]
async fn test_alter_with_pending_tails(#[case] kind: AlterKind, #[case] allowed: bool) {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;
    let metadata = engine.get_region(region_id).unwrap().metadata();

    let result = engine
        .handle_request(region_id, RegionRequest::Alter(RegionAlterRequest { kind }))
        .await;

    if allowed {
        result.unwrap();
    } else {
        assert_pending_tails_error(&result.unwrap_err());
        assert_eq!(metadata, engine.get_region(region_id).unwrap().metadata());
    }
}

#[tokio::test]
async fn test_alter_rejects_no_op_with_pending_tails() {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;
    let column_metadatas = engine
        .get_region(region_id)
        .unwrap()
        .metadata()
        .column_metadatas
        .clone();

    let kind = AlterKind::SyncColumns { column_metadatas };
    let err = engine
        .handle_request(region_id, RegionRequest::Alter(RegionAlterRequest { kind }))
        .await
        .unwrap_err();

    assert_pending_tails_error(&err);
}

#[tokio::test]
async fn test_enter_staging_rejects_region_with_pending_tails() {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;

    let request = EnterStagingRequest {
        partition_directive: StagingPartitionDirective::RejectAllWrites,
    };
    let err = engine
        .handle_request(region_id, RegionRequest::EnterStaging(request))
        .await
        .unwrap_err();

    assert_pending_tails_error(&err);
    assert!(!engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_repartition_flush_rejects_region_with_pending_tails() {
    let (_env, engine, region_id) = region_with_pending_tails(0).await;

    let request = RegionFlushRequest {
        reason: Some(RegionFlushReason::Repartition),
        ..Default::default()
    };
    let err = engine
        .handle_request(region_id, RegionRequest::Flush(request))
        .await
        .unwrap_err();

    assert_pending_tails_error(&err);
}

#[tokio::test]
async fn test_close_with_flush_rejects_region_with_pending_tails() {
    let (_env, engine, region_id) = region_with_pending_tails(1).await;

    let request = RegionCloseRequest {
        flush_on_close: true,
    };
    let err = engine
        .handle_request(region_id, RegionRequest::Close(request))
        .await
        .unwrap_err();

    assert_pending_tails_error(&err);
    assert!(engine.get_region(region_id).is_some());
}
