use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::logging::info;
use memmap2::Mmap;
use tonic::{Request, Response};

use crate::error::Result;
use crate::grpc::TonicResult;
use crate::shm_server::Shm;
use crate::{NotificationRequest, NotificationResponse};

#[async_trait]
pub trait ShmipcHandler: Send + Sync {
    fn read_range(&self, start_offset: usize, end_offset: usize, mmap: &Mmap) -> Result<()>;

    async fn handle(
        &self,
        notification: NotificationRequest,
        mmap: &Mmap,
    ) -> TonicResult<Response<NotificationResponse>>;
}

pub type ShmipcHandlerRef = Arc<dyn ShmipcHandler>;

pub(crate) struct ShmipcService {
    handler: ShmipcHandlerRef,
    mmap: Mmap,
}

impl ShmipcService {
    pub(crate) fn new(handler: ShmipcHandlerRef, file_name: &str) -> Self {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(file_name)
            .unwrap();

        let mmap = unsafe { Mmap::map(&file).unwrap() };

        Self { handler, mmap }
    }
}

#[async_trait]
impl Shm for ShmipcService {
    async fn notify(
        &self,
        request: Request<NotificationRequest>,
    ) -> TonicResult<Response<NotificationResponse>> {
        info!("receive notification request");
        // TODO(zhuziyi): return values
        let request = request.into_inner();
        let response = self.handler.handle(request, &self.mmap).await?;
        Ok(response)
    }
}
