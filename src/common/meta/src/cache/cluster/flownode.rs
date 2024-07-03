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

use futures::future::BoxFuture;
use moka::future::Cache;
use snafu::{OptionExt, ResultExt};

use crate::cache::{CacheContainer, Initializer};
use crate::cluster::ClusterInfoRef;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::peer::Peer;
use crate::{error, FlownodeId};

/// [FlownodePeerCache] caches the [FlownodeId] to [Peer] mapping.
pub type FlownodePeerCache = CacheContainer<FlownodeId, Arc<Peer>, CacheIdent>;

pub type FlownodePeerCacheRef = Arc<FlownodePeerCache>;

/// Constructs a [FlownodePeerCache].
pub fn new_flownode_peer_cache(
    name: String,
    cache: Cache<FlownodeId, Arc<Peer>>,
    cluster_info: ClusterInfoRef,
) -> FlownodePeerCache {
    let init = init_factory(cluster_info);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(cluster_info: ClusterInfoRef) -> Initializer<FlownodeId, Arc<Peer>> {
    Arc::new(move |flownode_id| {
        let cluster_info = cluster_info.clone();
        Box::pin(async move {
            let peer = cluster_info
                .get_flownode(*flownode_id)
                .await
                .context(error::GetClusterInfoSnafu)?
                .context(error::ValueNotExistSnafu {})?;

            Ok(Some(Arc::new(peer)))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<FlownodeId, Arc<Peer>>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if let CacheIdent::FlownodeId(flownode_id) = ident {
            cache.invalidate(flownode_id).await
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::FlownodeId(_))
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use common_error::ext::BoxedError;
    use moka::future::CacheBuilder;

    use crate::cache::new_flownode_peer_cache;
    use crate::cluster::{ClusterInfo, NodeInfo, Role};
    use crate::instruction::CacheIdent;
    use crate::peer::Peer;
    use crate::{error, FlownodeId};

    struct MockClusterInfo {
        error: bool,
    }

    #[async_trait::async_trait]
    impl ClusterInfo for MockClusterInfo {
        async fn list_nodes(
            &self,
            _role: Option<Role>,
        ) -> std::result::Result<Vec<NodeInfo>, BoxedError> {
            unimplemented!()
        }

        async fn get_flownode(
            &self,
            id: FlownodeId,
        ) -> std::result::Result<Option<Peer>, BoxedError> {
            if self.error {
                return Err(BoxedError::new(
                    error::UnexpectedSnafu {
                        err_msg: "mock error".to_string(),
                    }
                    .build(),
                ));
            }
            Ok(Some(Peer {
                id,
                addr: format!("{}.flownode", id),
            }))
        }
    }

    #[tokio::test]
    async fn test_get() {
        let mock_cluster_info = Arc::new(MockClusterInfo { error: false });
        let cache = CacheBuilder::new(128).build();
        let cache = new_flownode_peer_cache("test".to_string(), cache, mock_cluster_info);
        let peer_1 = cache.get(1).await.unwrap().unwrap();
        assert_eq!(peer_1.id, 1);
        let peer_2 = cache.get(2).await.unwrap().unwrap();
        assert_eq!(peer_2.id, 2);
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        cache
            .invalidate(&[CacheIdent::FlownodeId(1)])
            .await
            .unwrap();
        assert!(!cache.contains_key(&1));
        assert!(cache.contains_key(&2));
    }
}
