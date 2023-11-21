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

use common_meta::kv_backend::ResettableKvBackendRef;
use common_meta::rpc::store::{DeleteRangeRequest, PutRequest, RangeRequest};
use common_meta::RegionIdent;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::keys::InactiveRegionKey;
use crate::metrics::METRIC_META_INACTIVE_REGIONS;

pub struct InactiveRegionManager<'a> {
    store: &'a ResettableKvBackendRef,
}

impl<'a> InactiveRegionManager<'a> {
    pub fn new(store: &'a ResettableKvBackendRef) -> Self {
        Self { store }
    }

    pub async fn register_inactive_region(&self, region_ident: &RegionIdent) -> Result<()> {
        let region_id = region_ident.get_region_id().as_u64();
        let key = InactiveRegionKey {
            cluster_id: region_ident.cluster_id,
            node_id: region_ident.datanode_id,
            region_id,
        };
        let req = PutRequest {
            key: key.into(),
            value: vec![],
            prev_kv: false,
        };
        self.store.put(req).await.context(error::KvBackendSnafu)?;

        METRIC_META_INACTIVE_REGIONS.inc();

        Ok(())
    }

    pub async fn deregister_inactive_region(&self, region_ident: &RegionIdent) -> Result<()> {
        let region_id = region_ident.get_region_id().as_u64();
        let key: Vec<u8> = InactiveRegionKey {
            cluster_id: region_ident.cluster_id,
            node_id: region_ident.datanode_id,
            region_id,
        }
        .into();
        self.store
            .delete(&key, false)
            .await
            .context(error::KvBackendSnafu)?;

        METRIC_META_INACTIVE_REGIONS.dec();

        Ok(())
    }

    /// Scan all inactive regions in the cluster.
    ///
    /// When will these data appear?
    /// Generally, it is because the corresponding Datanode is disconnected and
    /// did not respond to the `Failover` scheduling instructions of metasrv.
    pub async fn scan_all_inactive_regions(
        &self,
        cluster_id: u64,
    ) -> Result<Vec<InactiveRegionKey>> {
        let prefix = InactiveRegionKey::get_prefix_by_cluster(cluster_id);
        let request = RangeRequest::new().with_prefix(prefix);
        let resp = self
            .store
            .range(request)
            .await
            .context(error::KvBackendSnafu)?;
        let kvs = resp.kvs;
        kvs.into_iter()
            .map(|kv| InactiveRegionKey::try_from(kv.key))
            .collect::<Result<Vec<_>>>()
    }

    pub async fn clear_all_inactive_regions(&self, cluster_id: u64) -> Result<()> {
        let prefix = InactiveRegionKey::get_prefix_by_cluster(cluster_id);
        let request = DeleteRangeRequest::new().with_prefix(prefix);
        let _ = self
            .store
            .delete_range(request)
            .await
            .context(error::KvBackendSnafu)?;
        Ok(())
    }
}
