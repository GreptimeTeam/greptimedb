use std::sync::Arc;

use async_trait::async_trait;
use common_error::prelude::ErrorExt;
use store_api::manifest::{MetaAction, Snapshot};

use crate::error::{Error, Result};
use crate::manifest::ManifestImpl;

#[async_trait]
pub trait Checkpointer: Send + Sync + std::fmt::Debug {
    type Snapshot: Snapshot<Error = Error>;
    type MetaAction: MetaAction<Error = Error>;

    async fn do_checkpoint(
        &self,
        manifest: &ManifestImpl<Self::Snapshot, Self::MetaAction>,
    ) -> Result<Self::Snapshot>;
}
