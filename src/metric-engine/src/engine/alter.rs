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

mod extract_new_columns;
mod validate;

use std::collections::{BTreeSet, HashMap};

use api::v1::SemanticType;
use common_query::native_histogram::is_native_histogram_value_type;
use extract_new_columns::extract_new_columns;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
use store_api::region_request::{AffectedRows, AlterKind, RegionAlterRequest};
use store_api::storage::RegionId;
use validate::validate_alter_region_requests;

use crate::engine::MetricEngineInner;
use crate::engine::region_metadata;
use crate::error::{
    AddingFieldColumnSnafu, LogicalRegionNotFoundSnafu, PhysicalRegionNotFoundSnafu, Result,
    SerializeColumnMetadataSnafu, UnexpectedRequestSnafu,
};
use crate::utils::{append_manifest_info, encode_manifest_info_to_extensions};

impl MetricEngineInner {
    pub async fn alter_regions(
        &self,
        mut requests: Vec<(RegionId, RegionAlterRequest)>,
        extension_return_value: &mut HashMap<String, Vec<u8>>,
    ) -> Result<AffectedRows> {
        if requests.is_empty() {
            return Ok(0);
        }

        let first_region_id = &requests.first().unwrap().0;
        if self.is_physical_region(*first_region_id) {
            ensure!(
                requests.len() == 1,
                UnexpectedRequestSnafu {
                    reason: "Physical table must be altered with single request".to_string(),
                }
            );
            let (region_id, request) = requests.pop().unwrap();
            self.alter_physical_region(region_id, request).await?;
        } else {
            // Fast path for single logical region alter request
            if requests.len() == 1 {
                // Safety: requests is not empty
                let region_id = requests.first().unwrap().0;
                let physical_region_id = self
                    .state
                    .read()
                    .unwrap()
                    .get_physical_region_id(region_id)
                    .with_context(|| LogicalRegionNotFoundSnafu { region_id })?;
                let mut manifest_infos = Vec::with_capacity(1);
                self.alter_logical_regions(physical_region_id, requests, extension_return_value)
                    .await?;
                append_manifest_info(&self.mito, physical_region_id, &mut manifest_infos);
                encode_manifest_info_to_extensions(&manifest_infos, extension_return_value)?;
            } else {
                let grouped_requests =
                    self.group_logical_region_requests_by_physical_region_id(requests)?;
                let mut manifest_infos = Vec::with_capacity(grouped_requests.len());
                for (physical_region_id, requests) in grouped_requests {
                    self.alter_logical_regions(
                        physical_region_id,
                        requests,
                        extension_return_value,
                    )
                    .await?;
                    append_manifest_info(&self.mito, physical_region_id, &mut manifest_infos);
                }
                encode_manifest_info_to_extensions(&manifest_infos, extension_return_value)?;
            }
        }
        Ok(0)
    }

    /// Groups the alter logical region requests by physical region id.
    fn group_logical_region_requests_by_physical_region_id(
        &self,
        requests: Vec<(RegionId, RegionAlterRequest)>,
    ) -> Result<HashMap<RegionId, Vec<(RegionId, RegionAlterRequest)>>> {
        let mut result = HashMap::with_capacity(requests.len());
        let state = self.state.read().unwrap();

        for (region_id, request) in requests {
            let physical_region_id = state
                .get_physical_region_id(region_id)
                .with_context(|| LogicalRegionNotFoundSnafu { region_id })?;
            result
                .entry(physical_region_id)
                .or_insert_with(Vec::new)
                .push((region_id, request));
        }

        Ok(result)
    }

    /// Alter multiple logical regions on the same physical region.
    pub async fn alter_logical_regions(
        &self,
        physical_region_id: RegionId,
        requests: Vec<(RegionId, RegionAlterRequest)>,
        extension_return_value: &mut HashMap<String, Vec<u8>>,
    ) -> Result<AffectedRows> {
        // Checks all alter requests are add columns.
        validate_alter_region_requests(&requests)?;
        self.validate_logical_field_alters(physical_region_id, &requests)
            .await?;

        // Finds new columns to add
        let mut new_column_indices = HashMap::new();
        let mut new_columns_to_add = vec![];

        let index_options = {
            let state = &self.state.read().unwrap();
            let region_state = state
                .physical_region_states()
                .get(&physical_region_id)
                .with_context(|| PhysicalRegionNotFoundSnafu {
                    region_id: physical_region_id,
                })?;
            let physical_columns = region_state.physical_columns();

            extract_new_columns(
                &requests,
                physical_columns,
                &mut new_column_indices,
                &mut new_columns_to_add,
            )?;

            region_state.options().index
        };

        // Acquire logical region locks in a deterministic order to avoid deadlocks when multiple
        // alter operations target overlapping regions concurrently.
        let region_ids = requests
            .iter()
            .map(|(region_id, _)| *region_id)
            .collect::<BTreeSet<_>>();

        let mut write_guards = Vec::with_capacity(region_ids.len());
        for region_id in region_ids {
            write_guards.push(
                self.metadata_region
                    .write_lock_logical_region(region_id)
                    .await?,
            );
        }

        self.add_physical_columns(physical_region_id, new_columns_to_add, index_options)
            .await?;

        let physical_metadata = self
            .data_region
            .physical_metadata(physical_region_id)
            .await?;

        // The physical region now has every requested column, either because this call
        // added it or because it was already there with the same definition.
        let mut logical_region_columns = Vec::with_capacity(requests.len());
        for (region_id, request) in &requests {
            let AlterKind::AddColumns { columns } = &request.kind else {
                unreachable!()
            };
            let mut column_metadata = HashMap::with_capacity(columns.len());
            for col in columns {
                let column_name = col.column_metadata.column_schema.name.as_str();
                let metadata = region_metadata::physical_column(&physical_metadata, column_name)?;
                column_metadata.insert(column_name, metadata);
            }
            logical_region_columns.push((*region_id, column_metadata));
        }

        self.cache_new_physical_columns(&physical_metadata, new_column_indices.keys().copied())?;

        // Writes logical regions metadata to metadata region
        let result = self
            .metadata_region
            .add_logical_regions(
                physical_region_id,
                false,
                logical_region_columns.into_iter(),
            )
            .await;
        // A failed write may still be applied, e.g. when the region lease expires after the WAL
        // append, so the cached logical columns are dropped either way.
        self.state
            .write()
            .unwrap()
            .invalid_logical_regions_cache(requests.iter().map(|(region_id, _)| *region_id));
        result?;

        extension_return_value.insert(
            ALTER_PHYSICAL_EXTENSION_KEY.to_string(),
            ColumnMetadata::encode_list(&physical_metadata.column_metadatas)
                .context(SerializeColumnMetadataSnafu)?,
        );

        Ok(0)
    }

    async fn validate_logical_field_alters(
        &self,
        physical_region_id: RegionId,
        requests: &[(RegionId, RegionAlterRequest)],
    ) -> Result<()> {
        // Logical metric tables have one field column. Native histograms are a
        // special struct field, so field alters must leave exactly that field.
        for (region_id, request) in requests {
            let AlterKind::AddColumns { columns } = &request.kind else {
                unreachable!()
            };
            let added_fields = columns
                .iter()
                .filter(|col| col.column_metadata.semantic_type == SemanticType::Field)
                .collect::<Vec<_>>();
            let Some(&first_added_field) = added_fields.first() else {
                continue;
            };

            let existing_columns = self
                .load_logical_columns(physical_region_id, *region_id)
                .await?;
            ensure!(
                added_fields.len() == 1
                    && is_native_histogram_value_type(
                        &first_added_field.column_metadata.column_schema.data_type
                    )
                    && existing_columns
                        .iter()
                        .all(|col| col.semantic_type != SemanticType::Field),
                AddingFieldColumnSnafu {
                    name: first_added_field.column_metadata.column_schema.name.clone(),
                }
            );
        }

        Ok(())
    }

    async fn alter_physical_region(
        &self,
        region_id: RegionId,
        request: RegionAlterRequest,
    ) -> Result<()> {
        self.data_region
            .alter_region_options(region_id, request)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use api::v1::SemanticType;
    use common_meta::ddl::test_util::assert_column_name_and_id;
    use common_meta::ddl::utils::{parse_column_metadatas, parse_manifest_infos_from_extensions};
    use common_query::prelude::{greptime_timestamp, greptime_value};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{
        AlterKind, BatchRegionDdlRequest, RegionAlterRequest, SetRegionOption,
    };
    use store_api::storage::RegionId;
    use store_api::storage::consts::ReservedColumnId;

    use common_error::ext::{ErrorExt, RetryHint};
    use common_error::status_code::StatusCode;
    use mito2::config::MitoConfig;
    use mito2::engine::listener::EventListener;
    use mito2::test_util::TestEnv as MitoTestEnv;
    use tokio::sync::{Notify, mpsc};

    use crate::engine::IndexOptions;
    use crate::test_util::{TestEnv, alter_logical_region_request, create_logical_region_request};
    use crate::utils::to_data_region_id;

    async fn check_alter_physical_skip_wal(
        engine: &super::MetricEngineInner,
        region_id: RegionId,
        skip_wal: bool,
    ) {
        let request = RegionAlterRequest {
            kind: AlterKind::SetRegionOptions {
                options: vec![SetRegionOption::SkipWal(skip_wal)],
            },
        };
        engine
            .alter_physical_region(region_id, request)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_alter_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();
        let engine_inner = engine.inner;

        // alter physical region
        let physical_region_id = env.default_physical_region_id();
        let request = alter_logical_region_request(&["tag1"]);

        let result = engine_inner
            .alter_physical_region(physical_region_id, request.clone())
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Alter request to physical region is forbidden".to_string()
        );

        // skip-WAL changes on the physical region should be forwarded to the data region
        check_alter_physical_skip_wal(&engine_inner, physical_region_id, true).await;
        check_alter_physical_skip_wal(&engine_inner, physical_region_id, false).await;

        // alter logical region
        let metadata_region = env.metadata_region();
        let logical_region_id = env.default_logical_region_id();
        let is_column_exist = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, "tag1")
            .await
            .unwrap()
            .is_some();
        assert!(!is_column_exist);

        let region_id = env.default_logical_region_id();
        let response = env
            .metric()
            .handle_batch_ddl_requests(BatchRegionDdlRequest::Alter(vec![(
                region_id,
                request.clone(),
            )]))
            .await
            .unwrap();
        let manifest_infos = parse_manifest_infos_from_extensions(&response.extensions).unwrap();
        assert_eq!(manifest_infos[0].0, physical_region_id);
        assert!(manifest_infos[0].1.is_metric());

        let semantic_type = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, "tag1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(semantic_type, SemanticType::Tag);
        let timestamp_index = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, greptime_timestamp())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(timestamp_index, SemanticType::Timestamp);
        let column_metadatas =
            parse_column_metadatas(&response.extensions, ALTER_PHYSICAL_EXTENSION_KEY).unwrap();
        assert_column_name_and_id(
            &column_metadatas,
            &[
                (greptime_timestamp(), 0),
                (greptime_value(), 1),
                ("__table_id", ReservedColumnId::table_id()),
                ("__tsid", ReservedColumnId::tsid()),
                ("job", 2),
                ("tag1", 3),
            ],
        );
    }

    #[tokio::test]
    async fn test_alter_logical_regions() {
        let env = TestEnv::new().await;
        let engine = env.metric();
        let physical_region_id1 = RegionId::new(1024, 0);
        let physical_region_id2 = RegionId::new(1024, 1);
        let logical_region_id1 = RegionId::new(1025, 0);
        let logical_region_id2 = RegionId::new(1025, 1);
        env.create_physical_region(physical_region_id1, "/test_dir1", vec![])
            .await;
        env.create_physical_region(physical_region_id2, "/test_dir2", vec![])
            .await;

        let region_create_request1 = crate::test_util::create_logical_region_request(
            &["job"],
            physical_region_id1,
            "logical1",
        );
        let region_create_request2 =
            create_logical_region_request(&["job"], physical_region_id2, "logical2");
        engine
            .handle_batch_ddl_requests(BatchRegionDdlRequest::Create(vec![
                (logical_region_id1, region_create_request1),
                (logical_region_id2, region_create_request2),
            ]))
            .await
            .unwrap();

        let region_alter_request1 = alter_logical_region_request(&["tag1"]);
        let region_alter_request2 = alter_logical_region_request(&["tag1"]);
        let response = engine
            .handle_batch_ddl_requests(BatchRegionDdlRequest::Alter(vec![
                (logical_region_id1, region_alter_request1),
                (logical_region_id2, region_alter_request2),
            ]))
            .await
            .unwrap();

        let manifest_infos = parse_manifest_infos_from_extensions(&response.extensions).unwrap();
        assert_eq!(manifest_infos.len(), 2);
        let region_ids = manifest_infos.into_iter().map(|i| i.0).collect::<Vec<_>>();
        assert!(region_ids.contains(&physical_region_id1));
        assert!(region_ids.contains(&physical_region_id2));

        let column_metadatas =
            parse_column_metadatas(&response.extensions, ALTER_PHYSICAL_EXTENSION_KEY).unwrap();
        assert_column_name_and_id(
            &column_metadatas,
            &[
                (greptime_timestamp(), 0),
                (greptime_value(), 1),
                ("__table_id", ReservedColumnId::table_id()),
                ("__tsid", ReservedColumnId::tsid()),
                ("job", 2),
                ("tag1", 3),
            ],
        );
    }

    // Column ids are derived from the current metadata, so concurrent requests on
    // logical regions sharing one physical region must not hand the same id to
    // different columns.
    #[tokio::test]
    async fn test_add_physical_columns_concurrently_assigns_distinct_ids() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let physical_region_id = env.default_physical_region_id();
        let data_region_id = to_data_region_id(physical_region_id);
        let tag = |name: &str| ColumnMetadata {
            column_id: 0,
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(name, ConcreteDataType::string_datatype(), false),
        };

        let inner = env.metric().inner.clone();
        let (first, second) = tokio::join!(
            inner.add_physical_columns(
                physical_region_id,
                vec![tag("tag4")],
                IndexOptions::Inverted,
            ),
            inner.add_physical_columns(
                physical_region_id,
                vec![tag("tag5")],
                IndexOptions::Inverted,
            ),
        );
        first.unwrap();
        second.unwrap();

        let metadata = env.mito().get_metadata(data_region_id).await.unwrap();
        assert_ne!(
            metadata.column_by_name("tag4").unwrap().column_id,
            metadata.column_by_name("tag5").unwrap().column_id
        );
    }

    /// Holds the result of an armed physical alter in Mito, after its manifest is
    /// updated and before the new metadata becomes visible, and reports the requests
    /// the region worker receives.
    struct HoldAlterResultListener {
        armed: AtomicBool,
        altering: Notify,
        release: Notify,
        received: mpsc::UnboundedSender<usize>,
    }

    #[async_trait::async_trait]
    impl EventListener for HoldAlterResultListener {
        fn on_recv_requests(&self, request_num: usize) {
            let _ = self.received.send(request_num);
        }

        async fn on_notify_region_change_result_begin(&self, _region_id: RegionId) {
            if self.armed.swap(false, Ordering::SeqCst) {
                self.altering.notify_one();
                self.release.notified().await;
            }
        }
    }

    // Pins the residual race documented by #10372: the column-add lock is released
    // with a dropped caller while Mito still applies its alter, so a request for
    // another column reads the same metadata, allocates the same column id, and is
    // rejected once the first alter lands. The rejection is not retryable, while a
    // manual retry succeeds with the next id. Tracked by #10377.
    #[tokio::test]
    async fn test_add_physical_columns_cancelled_while_alter_pending() {
        common_telemetry::init_default_ut_logging();
        let mut mito_env = MitoTestEnv::with_prefix("cancel-pending-add-column").await;
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let listener = Arc::new(HoldAlterResultListener {
            armed: AtomicBool::new(false),
            altering: Notify::new(),
            release: Notify::new(),
            received: received_tx,
        });
        let mito = mito_env
            .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
            .await;
        let env = TestEnv::with_mito_engine(mito_env, mito);
        env.init_metric_region().await;
        let physical_region_id = env.default_physical_region_id();
        let data_region_id = to_data_region_id(physical_region_id);
        let tag = |name: &str| ColumnMetadata {
            column_id: 0,
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(name, ConcreteDataType::string_datatype(), false),
        };
        let inner = env.metric().inner.clone();

        // Request A allocates an id for `x` and its alter is held in Mito.
        listener.armed.store(true, Ordering::SeqCst);
        let altering = listener.altering.notified();
        let request_a = {
            let inner = inner.clone();
            let column = tag("x");
            tokio::spawn(async move {
                inner
                    .add_physical_columns(physical_region_id, vec![column], IndexOptions::Inverted)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), altering)
            .await
            .expect("alter of request A should reach Mito");

        // Drop the caller of A: its guard is released while Mito still applies A.
        request_a.abort();
        assert!(request_a.await.unwrap_err().is_cancelled());
        let pending_id = {
            let metadata = env.mito().get_metadata(data_region_id).await.unwrap();
            assert!(metadata.column_by_name("x").is_none());
            1 + metadata
                .column_metadatas
                .iter()
                .map(|c| c.column_id)
                .filter(|id| !ReservedColumnId::is_reserved(*id))
                .max()
                .unwrap()
        };

        // Request B, for another column, reads the same metadata and allocates the same
        // id. Its alter waits in the worker behind the pending alter of A.
        while received_rx.try_recv().is_ok() {}
        let request_b = {
            let inner = inner.clone();
            let column = tag("y");
            tokio::spawn(async move {
                inner
                    .add_physical_columns(physical_region_id, vec![column], IndexOptions::Inverted)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), received_rx.recv())
            .await
            .expect("alter of request B should reach Mito");
        assert!(!request_b.is_finished());

        // A lands first and takes the id, so Mito rejects B.
        listener.release.notify_one();
        let err = tokio::time::timeout(Duration::from_secs(10), request_b)
            .await
            .expect("request B should finish once A is applied")
            .unwrap()
            .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::InvalidArguments, "{err:?}");
        assert_eq!(err.retry_hint(), RetryHint::NonRetryable, "{err:?}");
        assert!(
            format!("{err:?}").contains(&format!("column id {pending_id} already exists")),
            "{err:?}"
        );

        let metadata = env.mito().get_metadata(data_region_id).await.unwrap();
        assert_eq!(metadata.column_by_name("x").unwrap().column_id, pending_id);
        assert!(metadata.column_by_name("y").is_none());

        // A manual retry sees `y` absent and allocates the next id.
        inner
            .add_physical_columns(physical_region_id, vec![tag("y")], IndexOptions::Inverted)
            .await
            .unwrap();
        let metadata = env.mito().get_metadata(data_region_id).await.unwrap();
        assert_eq!(metadata.column_by_name("x").unwrap().column_id, pending_id);
        assert_eq!(
            metadata.column_by_name("y").unwrap().column_id,
            pending_id + 1
        );
    }

    // A request that Mito accepted but that never wrote the logical metadata leaves the
    // physical column installed while the cache still misses it. The retry must reuse the
    // installed column id and repair both the cache and the logical metadata.
    #[tokio::test]
    async fn test_alter_logical_region_after_cancelled_physical_alter() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let physical_region_id = env.default_physical_region_id();
        let logical_region_id = env.default_logical_region_id();
        let data_region_id = to_data_region_id(physical_region_id);

        env.data_region()
            .add_columns(
                physical_region_id,
                vec![ColumnMetadata {
                    column_id: 0,
                    semantic_type: SemanticType::Tag,
                    column_schema: ColumnSchema::new(
                        "tag1",
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                }],
                IndexOptions::Inverted,
            )
            .await
            .unwrap();
        let installed_id = env
            .mito()
            .get_metadata(data_region_id)
            .await
            .unwrap()
            .column_by_name("tag1")
            .unwrap()
            .column_id;

        env.metric()
            .handle_batch_ddl_requests(BatchRegionDdlRequest::Alter(vec![(
                logical_region_id,
                alter_logical_region_request(&["tag1"]),
            )]))
            .await
            .unwrap();

        let metadata = env.mito().get_metadata(data_region_id).await.unwrap();
        assert_eq!(
            metadata.column_by_name("tag1").unwrap().column_id,
            installed_id
        );
        assert_eq!(
            env.metadata_region()
                .column_semantic_type(physical_region_id, logical_region_id, "tag1")
                .await
                .unwrap(),
            Some(SemanticType::Tag)
        );

        let engine = env.metric();
        let state = engine.inner.state.read().unwrap();
        let physical_columns = state
            .physical_region_states()
            .get(&data_region_id)
            .unwrap()
            .physical_columns();
        assert!(physical_columns.contains_key("tag1"));
    }
}
