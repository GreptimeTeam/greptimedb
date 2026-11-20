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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use common_meta::instruction::{
    InstructionError, InstructionReply, InstructionResult, MetadataCompaction, MetadataSyncAttempt,
    SimpleReply,
};
use metric_engine::engine::MetricEngine;
use mito2::compaction::CompactionGuard;
use tokio::sync::{OnceCell, oneshot};
use tokio::time::Instant;

use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

#[derive(Clone, Default)]
pub struct MetadataCompactionHandler {
    leases: Arc<Mutex<HashMap<MetadataSyncAttempt, PauseLease>>>,
}

struct PauseLease {
    deadline: Instant,
    guard: Arc<OnceCell<CompactionGuard>>,
    _cancel_expiry: oneshot::Sender<()>,
}

impl MetadataCompactionHandler {
    fn lease(
        &self,
        attempt: MetadataSyncAttempt,
        deadline: Instant,
    ) -> (Arc<OnceCell<CompactionGuard>>, Instant) {
        match self.leases.lock().unwrap().entry(attempt) {
            Entry::Occupied(entry) => (entry.get().guard.clone(), entry.get().deadline),
            Entry::Vacant(entry) => {
                let guard = Arc::new(OnceCell::new());
                let (cancel, cancelled) = oneshot::channel();
                entry.insert(PauseLease {
                    deadline,
                    guard: guard.clone(),
                    _cancel_expiry: cancel,
                });
                let leases = Arc::downgrade(&self.leases);
                let expected = Arc::downgrade(&guard);
                common_runtime::spawn_global(async move {
                    tokio::select! {
                        _ = cancelled => (),
                        _ = tokio::time::sleep_until(deadline) => {
                            if let Some(leases) = leases.upgrade()
                                && let Entry::Occupied(entry) = leases.lock().unwrap().entry(attempt)
                                && expected.ptr_eq(&Arc::downgrade(&entry.get().guard))
                            {
                                entry.remove();
                            }
                        }
                    }
                });
                (guard, deadline)
            }
        }
    }

    async fn pause(
        &self,
        ctx: &HandlerContext,
        attempt: MetadataSyncAttempt,
        remaining: std::time::Duration,
    ) -> InstructionResult<()> {
        let deadline = Instant::now().checked_add(remaining).ok_or_else(|| {
            InstructionError::legacy_internal_retryable("Invalid metadata sync deadline")
        })?;
        let region_id = attempt.region_id;
        let engine = ctx
            .region_server
            .find_engine(region_id)
            .map_err(|e| InstructionError::from_error(&e))?
            .ok_or_else(|| {
                InstructionError::legacy_internal_retryable(format!(
                    "Source region {region_id} is unavailable"
                ))
            })?;
        let metric = engine
            .as_any()
            .downcast_ref::<MetricEngine>()
            .ok_or_else(|| {
                InstructionError::legacy_internal_retryable(format!(
                    "Source region {region_id} is not a metric region"
                ))
            })?;
        if ctx.region_server.is_region_leader(region_id) != Some(true) {
            return Err(InstructionError::legacy_internal_retryable(format!(
                "Source region {region_id} is not writable"
            )));
        }
        let (lease, deadline) = self.lease(attempt, deadline);
        tokio::time::timeout_at(
            deadline,
            lease.get_or_try_init(|| metric.pause_metadata_compaction(region_id)),
        )
        .await
        .map_err(|_| {
            InstructionError::legacy_internal_retryable(
                "Timed out pausing source metadata compaction",
            )
        })?
        .map_err(|e| InstructionError::from_error(&e))?;
        if Instant::now() >= deadline || ctx.region_server.is_region_leader(region_id) != Some(true)
        {
            return Err(InstructionError::legacy_internal_retryable(
                "Source metadata compaction lease expired or leadership changed",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl InstructionHandler for MetadataCompactionHandler {
    type Instruction = MetadataCompaction;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        instruction: MetadataCompaction,
    ) -> Option<InstructionReply> {
        match instruction {
            MetadataCompaction::Pause { attempt, remaining } => {
                let result = self.pause(ctx, attempt, remaining).await;
                Some(InstructionReply::MetadataCompaction(SimpleReply {
                    result: result.is_ok(),
                    error: result.err(),
                }))
            }
            MetadataCompaction::Release(attempt) => {
                self.leases.lock().unwrap().remove(&attempt);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_meta::kv_backend::memory::MemoryKvBackend;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::metric_engine_consts::{
        METRIC_ENGINE_NAME, METRIC_METADATA_REGION_GROUP, PHYSICAL_TABLE_METADATA_KEY,
    };
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;

    use super::*;
    use crate::tests::mock_region_server;

    #[tokio::test]
    async fn test_metadata_pause_is_attempt_scoped_and_expires() {
        let mut env = TestEnv::with_prefix("metadata-compaction-lease").await;
        let mito = env.create_engine(Default::default()).await;
        let mut server = mock_region_server();
        let metric = MetricEngine::try_new(mito.clone(), Default::default()).unwrap();
        server.register_engine(Arc::new(metric));
        let region_id = RegionId::new(32, 1);
        let mut create = CreateRequestBuilder::new()
            .tag_num(0)
            .insert_option(PHYSICAL_TABLE_METADATA_KEY, "")
            .build();
        create.engine = METRIC_ENGINE_NAME.to_string();
        create.column_metadatas[0].column_schema.name = "greptime_value".into();
        create.column_metadatas[1].column_schema.name = "greptime_timestamp".into();
        server
            .handle_request(region_id, RegionRequest::Create(create))
            .await
            .unwrap();
        let ctx = HandlerContext::new_for_test(server, Arc::new(MemoryKvBackend::new()));
        let handler = MetadataCompactionHandler::default();
        let first = MetadataSyncAttempt {
            region_id,
            attempt_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
        };
        let second = MetadataSyncAttempt {
            region_id,
            attempt_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
        };
        for attempt in [first, first, second] {
            handler
                .pause(&ctx, attempt, Duration::from_secs(60))
                .await
                .unwrap();
        }
        for _ in 0..2 {
            handler
                .handle(&ctx, MetadataCompaction::Release(first))
                .await;
        }
        let meta = RegionId::with_group_and_seq(32, METRIC_METADATA_REGION_GROUP, 1);
        let compact = || mito.handle_request(meta, RegionRequest::Compact(Default::default()));
        let err = compact().await.unwrap_err();
        assert!(matches!(
            err.into_inner()
                .as_any()
                .downcast_ref::<mito2::error::Error>(),
            Some(mito2::error::Error::CompactionCancelled { .. })
        ));
        mito.handle_request(region_id, RegionRequest::Compact(Default::default()))
            .await
            .unwrap();
        mito.handle_request(meta, RegionRequest::Flush(Default::default()))
            .await
            .unwrap();
        handler
            .handle(&ctx, MetadataCompaction::Release(second))
            .await;
        compact().await.unwrap();
        handler
            .pause(&ctx, first, Duration::from_millis(200))
            .await
            .unwrap();
        assert!(compact().await.is_err());
        tokio::time::timeout(Duration::from_secs(5), async {
            while handler.leases.lock().unwrap().contains_key(&first) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        compact().await.unwrap();
    }
}
