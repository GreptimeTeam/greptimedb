use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::handoff::TeeingHandoff;
use hydroflow::scheduled::port::RecvPort;
use hydroflow::scheduled::SubgraphId;
use tokio::sync::RwLock;

use crate::compute::render::Context;
use crate::expr::{EvalError, ScalarExpr};
use crate::repr::DiffRow;
use crate::utils::{ArrangeHandler, Arrangement};

pub type Toff = TeeingHandoff<DiffRow>;

/// A collection, represent a collections of data that is received from a handoff.
pub struct Collection<T: 'static> {
    /// represent a stream of updates recv from this port
    pub stream: RecvPort<TeeingHandoff<T>>,
}

impl<T: 'static + Clone> Collection<T> {
    pub fn from_port(port: RecvPort<TeeingHandoff<T>>) -> Self {
        Collection { stream: port }
    }
    /// clone a collection, require a mutable reference to the hydroflow instance
    ///
    /// Note: need to be the same hydroflow instance that this collection is created from
    pub fn clone(&self, df: &mut Hydroflow) -> Self {
        Collection {
            stream: self.stream.tee(df),
        }
    }
}

pub struct Arranged {
    pub arrangement: ArrangeHandler,
    /// maintain a list of readers for the arrangement for the ease of scheduling
    pub readers: Arc<RwLock<Vec<SubgraphId>>>,
}

impl Arranged {
    fn try_clone_future(&self) -> Option<Self> {
        self.arrangement
            .clone_future_only()
            .map(|arrangement| Arranged {
                arrangement,
                readers: self.readers.clone(),
            })
    }
    fn try_clone_full(&self) -> Option<Self> {
        self.arrangement
            .clone_full_arrange()
            .map(|arrangement| Arranged {
                arrangement,
                readers: self.readers.clone(),
            })
    }
}

/// A bundle of the various ways a collection can be represented.
///
/// This type maintains the invariant that it does contain at least one(or both) valid
/// source of data, either a collection or at least one arrangement. This is for convenience
/// of reading the data from the collection.
pub struct CollectionBundle {
    pub collection: Collection<DiffRow>,
    /// the key [`ScalarExpr`] indicate how the keys(also a [`Row`]) used in Arranged is extract from collection's [`Row`]
    /// So it is the "index" of the arrangement
    pub arranged: BTreeMap<Vec<ScalarExpr>, Arranged>,
}

impl CollectionBundle {
    pub fn from_collection(collection: Collection<DiffRow>) -> Self {
        Self {
            collection,
            arranged: BTreeMap::default(),
        }
    }
    pub fn clone(&self, df: &mut Hydroflow) -> Self {
        Self {
            collection: self.collection.clone(df),
            arranged: self
                .arranged
                .iter()
                .map(|(k, v)| (k.clone(), v.try_clone_future().unwrap()))
                .collect(),
        }
    }
}

/// A thread local error collector, used to collect errors during the evaluation of the plan
pub struct ErrCollector {
    inner: Rc<RefCell<Vec<EvalError>>>,
}

impl ErrCollector {
    pub fn run<F>(&self, f: F)
    where
        F: FnOnce() -> Result<(), EvalError>,
    {
        if let Err(err) = f() {
            self.inner.borrow_mut().push(err);
        }
    }
}
