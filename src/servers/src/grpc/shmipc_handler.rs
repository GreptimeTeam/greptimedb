use std::sync::Arc;

use async_trait::async_trait;
// use common_runtime::Runtime;
use tonic::{Request, Response, Status};

// use crate::shm_client::ShmClient;
use crate::{shm_server::Shm, NotificationRequest, NotificationResponse};
#[async_trait]
pub trait ShmipcHandler: Send + Sync {
    async fn handle(
        &self,
        notification: NotificationRequest,
    ) -> Result<Response<NotificationResponse>, Status>;
}

pub type ShmipcHandlerRef = Arc<dyn ShmipcHandler>;

#[derive(Clone)]
pub(crate) struct ShmipcService {
    handler: ShmipcHandlerRef,
    // runtime: Arc<Runtime>,
}

impl ShmipcService {
    pub(crate) fn new(handler: ShmipcHandlerRef) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl Shm for ShmipcService {
    async fn notify(
        &self,
        request: Request<NotificationRequest>,
    ) -> Result<Response<NotificationResponse>, Status> {
        // TODO zhuziyi
        let request = request.into_inner();
        let response = self.handler.handle(request).await?;
        Ok(response)
    }
}
