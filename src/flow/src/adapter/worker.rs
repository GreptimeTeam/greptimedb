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

//! For single-thread flow worker

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use snafu::ResultExt;
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::adapter::error::{Error, EvalSnafu};
use crate::adapter::{ActiveDataflowState, FlowTickManager, TaskId};
use crate::expr::error::InternalSnafu;
use crate::expr::GlobalId;
use crate::plan::TypedPlan;
use crate::repr::DiffRow;

pub type SharedBuf = Arc<Mutex<VecDeque<DiffRow>>>;
pub struct WorkerHandle {
    itc_client: Mutex<InterThreadCallClient>,
}

/// Create both worker(`!Send`) and worker handle(`Send + Sync`)
pub fn create_worker<'a>(tick_manager: FlowTickManager) -> (WorkerHandle, Worker<'a>) {
    let (itc_client, itc_server) = create_inter_thread_call();
    let worker_handle = WorkerHandle {
        itc_client: Mutex::new(itc_client),
    };
    let worker = Worker {
        task_states: BTreeMap::new(),
        itc_server: Mutex::new(itc_server),
        tick_manager,
    };
    (worker_handle, worker)
}
#[test]
fn check_if_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<WorkerHandle>();
}

impl WorkerHandle {
    /// create task, return task id
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn create_task(
        &self,
        task_id: TaskId,
        plan: TypedPlan,
        sink_id: GlobalId,
        sink_sender: mpsc::UnboundedSender<DiffRow>,
        source_ids: &[GlobalId],
        src_recvs: Vec<broadcast::Receiver<DiffRow>>,
        create_if_not_exist: bool,
    ) -> Result<Option<TaskId>, Error> {
        let req = Request::Create {
            task_id,
            plan,
            sink_id,
            sink_sender,
            source_ids: source_ids.to_vec(),
            src_recvs,
            create_if_not_exist,
        };

        let ret = self.itc_client.blocking_lock().call_blocking(req)?;
        if let Response::Create { task_create_result } = ret {
            task_create_result
        } else {
            InternalSnafu {
                reason: format!(
                    "Flow Node/Worker itc failed, expect Response::Create, found {ret:?}"
                ),
            }
            .fail()
            .with_context(|_| EvalSnafu {})
        }
    }

    /// remove task, return task id
    pub fn remove_task(&self, task_id: TaskId) -> Result<TaskId, Error> {
        let req = Request::Remove { task_id };
        let ret = self.itc_client.blocking_lock().call_blocking(req)?;
        if let Response::Remove { task_remove_result } = ret {
            task_remove_result
        } else {
            InternalSnafu {
                reason: format!(
                    "Flow Node/Worker itc failed, expect Response::Remove, found {ret:?}"
                ),
            }
            .fail()
            .with_context(|_| EvalSnafu {})
        }
    }

    // trigger running the worker
    pub fn trigger_run(&self) {
        self.itc_client
            .blocking_lock()
            .call_non_resp(Request::TriggerRun);
    }
}

/// The actual worker that does the work and contain active state
pub struct Worker<'subgraph> {
    pub task_states: BTreeMap<TaskId, ActiveDataflowState<'subgraph>>,
    itc_server: Mutex<InterThreadCallServer>,
    tick_manager: FlowTickManager,
}

impl<'s> Worker<'s> {
    #[allow(clippy::too_many_arguments)]
    pub fn create_task(
        &mut self,
        task_id: TaskId,
        plan: TypedPlan,
        sink_id: GlobalId,
        sink_sender: mpsc::UnboundedSender<DiffRow>,
        source_ids: &[GlobalId],
        src_recvs: Vec<broadcast::Receiver<DiffRow>>,
        create_if_not_exist: bool,
    ) -> Result<Option<TaskId>, Error> {
        if create_if_not_exist {
            // check if the task already exists
            if self.task_states.contains_key(&task_id) {
                return Ok(None);
            }
        }

        let mut cur_task_state = ActiveDataflowState::<'s>::default();

        {
            let mut ctx = cur_task_state.new_ctx(sink_id);
            for (source_id, src_recv) in source_ids.iter().zip(src_recvs) {
                let bundle = ctx.render_source(src_recv)?;
                ctx.insert_global(*source_id, bundle);
            }

            let rendered = ctx.render_plan(plan.plan)?;
            ctx.render_unbounded_sink(rendered, sink_sender);
        }
        self.task_states.insert(task_id, cur_task_state);
        Ok(Some(task_id))
    }

    pub fn remove_task(&mut self, task_id: TaskId) -> Result<(), Error> {
        self.task_states.remove(&task_id);
        Ok(())
    }

    /// run the worker until it is dropped
    ///
    /// This method should be called inside a `LocalSet` since it's `!Send`
    pub async fn run(&mut self) {
        let (req_id, req) = self.itc_server.lock().await.recv().await.unwrap();
        match req {
            Request::Create {
                task_id,
                plan,
                sink_id,
                sink_sender,
                source_ids,
                src_recvs,
                create_if_not_exist,
            } => {
                let task_create_result = self.create_task(
                    task_id,
                    plan,
                    sink_id,
                    sink_sender,
                    &source_ids,
                    src_recvs,
                    create_if_not_exist,
                );
                self.itc_server
                    .lock()
                    .await
                    .resp(req_id, Response::Create { task_create_result });
            }
            Request::Remove { task_id } => {
                let ret = self.remove_task(task_id).map(|_| task_id);
                self.itc_server.lock().await.resp(
                    req_id,
                    Response::Remove {
                        task_remove_result: ret,
                    },
                )
            }
            Request::TriggerRun => self.run_tick(),
        }
    }

    /// return true if any task is running
    pub fn run_tick(&mut self) {
        let now = self.tick_manager.tick();
        for (_task_id, task_state) in self.task_states.iter_mut() {
            task_state.set_current_ts(now);
            task_state.run_available();
        }
    }
}

#[derive(Debug)]
enum Request {
    Create {
        task_id: TaskId,
        plan: TypedPlan,
        sink_id: GlobalId,
        sink_sender: mpsc::UnboundedSender<DiffRow>,
        source_ids: Vec<GlobalId>,
        src_recvs: Vec<broadcast::Receiver<DiffRow>>,
        create_if_not_exist: bool,
    },
    Remove {
        task_id: TaskId,
    },
    /// Trigger the worker to run, useful after input buffer is full
    TriggerRun,
}

#[derive(Debug)]
enum Response {
    Create {
        task_create_result: Result<Option<TaskId>, Error>,
    },
    Remove {
        task_remove_result: Result<TaskId, Error>,
    },
}

fn create_inter_thread_call() -> (InterThreadCallClient, InterThreadCallServer) {
    let (arg_send, arg_recv) = mpsc::unbounded_channel();
    let (ret_send, ret_recv) = mpsc::unbounded_channel();
    let client = InterThreadCallClient {
        call_id: Arc::new(Mutex::new(0)),
        arg_sender: arg_send,
        ret_recv,
    };
    let server = InterThreadCallServer {
        arg_recv,
        ret_sender: ret_send,
    };
    (client, server)
}

struct InterThreadCallClient {
    call_id: Arc<Mutex<usize>>,
    arg_sender: mpsc::UnboundedSender<(usize, Request)>,
    ret_recv: mpsc::UnboundedReceiver<(usize, Response)>,
}

impl InterThreadCallClient {
    /// call without expecting responses
    fn call_non_resp(&mut self, req: Request) {
        let call_id = {
            let mut call_id = self.call_id.blocking_lock();
            *call_id += 1;
            *call_id
        };
        self.arg_sender.send((call_id, req)).unwrap();
    }
    /// call blocking, and return the result
    fn call_blocking(&mut self, req: Request) -> Result<Response, Error> {
        let call_id = {
            let mut call_id = self.call_id.blocking_lock();
            *call_id += 1;
            *call_id
        };
        self.arg_sender.send((call_id, req)).unwrap();
        // TODO(discord9): better inter thread call impl
        let (ret_call_id, ret) = self.ret_recv.blocking_recv().unwrap();
        if ret_call_id != call_id {
            return InternalSnafu {
                reason: "call id mismatch, worker/worker handler should be in sync",
            }
            .fail()
            .with_context(|_| EvalSnafu {});
        }
        Ok(ret)
    }
}

struct InterThreadCallServer {
    pub arg_recv: mpsc::UnboundedReceiver<(usize, Request)>,
    pub ret_sender: mpsc::UnboundedSender<(usize, Response)>,
}

impl InterThreadCallServer {
    pub async fn recv(&mut self) -> Option<(usize, Request)> {
        self.arg_recv.recv().await
    }

    /// Send response back to the client
    pub fn resp(&self, call_id: usize, resp: Response) {
        self.ret_sender.send((call_id, resp)).unwrap();
    }
}
