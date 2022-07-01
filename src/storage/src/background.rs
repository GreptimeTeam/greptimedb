//! Background job management.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::{self, JoinHandle};
use snafu::ResultExt;

use crate::error::{self, Result};

/// Background job context.
#[derive(Clone, Default)]
pub struct Context {
    inner: Arc<ContextInner>,
}

impl Context {
    fn new() -> Context {
        Context::default()
    }

    pub fn cancel(&self) {
        self.inner.cancelled.store(false, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct ContextInner {
    cancelled: AtomicBool,
}

/// Handle to the background job.
pub struct JobHandle {
    ctx: Context,
    handle: JoinHandle<Result<()>>,
}

impl JobHandle {
    pub async fn join(self) -> Result<()> {
        self.handle.await.context(error::JoinTaskSnafu)?
    }

    #[allow(unused)]
    pub async fn cancel(self) -> Result<()> {
        self.ctx.cancel();

        self.join().await
    }
}

#[async_trait]
pub trait Job: Send {
    async fn run(&mut self, ctx: &Context) -> Result<()>;
}

type BoxedJob = Box<dyn Job>;

/// Thread pool that runs all background jobs.
#[async_trait]
pub trait JobPool: Send + Sync {
    /// Submit a job to run in background.
    ///
    /// Returns the [JobHandle] to the job.
    async fn submit(&self, job: BoxedJob) -> Result<JobHandle>;

    /// Shutdown the manager, pending background jobs may be discarded.
    async fn shutdown(&self) -> Result<()>;
}

pub type JobPoolRef = Arc<dyn JobPool>;

pub struct JobPoolImpl {}

#[async_trait]
impl JobPool for JobPoolImpl {
    async fn submit(&self, mut job: BoxedJob) -> Result<JobHandle> {
        // TODO(yingwen): [flush] Schedule background jobs to background workers, controlling parallelism.

        let ctx = Context::new();
        let job_ctx = ctx.clone();
        let handle = common_runtime::spawn_bg(async move { job.run(&job_ctx).await });

        Ok(JobHandle { ctx, handle })
    }

    async fn shutdown(&self) -> Result<()> {
        // TODO(yingwen): [flush] Stop background workers.
        unimplemented!()
    }
}
