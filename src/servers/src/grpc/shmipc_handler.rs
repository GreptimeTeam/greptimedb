use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::logging::info;
use tonic::{Request, Response};

use crate::grpc::TonicResult;
use crate::shm_server::Shm;
use crate::{NotificationRequest, NotificationResponse};
#[async_trait]
pub trait ShmipcHandler: Send + Sync {
    async fn handle(
        &self,
        notification: NotificationRequest,
    ) -> TonicResult<Response<NotificationResponse>>;
}

pub type ShmipcHandlerRef = Arc<dyn ShmipcHandler>;

#[derive(Clone)]
pub(crate) struct ShmipcService {
    handler: ShmipcHandlerRef,
}

impl ShmipcService {
    pub(crate) fn new(handler: ShmipcHandlerRef) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl Shm for ShmipcService {
    async fn notify(
        &self,
        request: Request<NotificationRequest>,
    ) -> TonicResult<Response<NotificationResponse>> {
        info!("receive notification request");
        // TODO(zhuziyi)
        let request = request.into_inner();
        let response = self.handler.handle(request).await?;
        Ok(response)
    }
}
