use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::SubgraphId;

use crate::compute::types::ErrCollector;
use crate::repr::{self, Timestamp};

/// input/output of a dataflow
/// One `ComputeState` manage the input/output/schedule of one `Hydroflow`
#[derive(Default)]
pub struct ComputeState {
    /// it is important to use a deque to maintain the order of subgraph here
    /// TODO(discord9): consider dedup? Also not necessary for hydroflow itself also do dedup when schedule
    schedule_subgraph: Rc<RefCell<BTreeMap<Timestamp, VecDeque<SubgraphId>>>>,
    /// Frontier (in sys time) before which updates should not be emitted.
    ///
    /// We *must* apply it to sinks, to ensure correct outputs.
    /// We *should* apply it to sources and imported shared state, because it improves performance.
    /// Which means it's also the current time in temporal filter to get current correct result
    pub as_of: Rc<RefCell<repr::Timestamp>>,
    /// error collector local to this `ComputeState`,
    /// useful for distinguishing errors from different `Hydroflow`
    pub err_collector: ErrCollector,
}

impl ComputeState {
    /// schedule all subgraph that need to run with time <= `as_of` and run_available()
    ///
    /// return true if any subgraph actually executed
    pub fn run_available_with_schedule(&mut self, df: &mut Hydroflow) -> bool {
        // first split keys <= as_of into another map
        let mut before = self
            .schedule_subgraph
            .borrow_mut()
            .split_off(&(*self.as_of.borrow() + 1));
        std::mem::swap(&mut before, &mut self.schedule_subgraph.borrow_mut());
        for (_, v) in before {
            for subgraph in v {
                df.schedule_subgraph(subgraph);
            }
        }
        df.run_available()
    }
    pub fn get_scheduler(&self) -> Scheduler {
        Scheduler {
            schedule_subgraph: self.schedule_subgraph.clone(),
            cur_subgraph: Rc::new(RefCell::new(None)),
        }
    }
}

pub struct Scheduler {
    schedule_subgraph: Rc<RefCell<BTreeMap<Timestamp, VecDeque<SubgraphId>>>>,
    pub cur_subgraph: Rc<RefCell<Option<SubgraphId>>>,
}

impl Scheduler {
    pub fn schedule_at(&self, next_run_time: Timestamp) {
        let mut schedule_subgraph = self.schedule_subgraph.borrow_mut();
        let subgraph = self.cur_subgraph.borrow();
        let subgraph = subgraph
            .as_ref()
            .expect("Call register_subgraph before schedule");
        let subgraph_queue = schedule_subgraph.entry(next_run_time).or_default();
        subgraph_queue.push_back(*subgraph);
    }
}
