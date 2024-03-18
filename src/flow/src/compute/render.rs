use std::cell::RefCell;
use std::collections::BTreeMap;

use hydroflow::lattices::cc_traits::Get;
use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::graph_ext::GraphExt;
use snafu::OptionExt;

use crate::adapter::error::{Error, InvalidQuerySnafu};
use crate::compute::state::ComputeState;
use crate::compute::types::{Collection, CollectionBundle, Toff};
use crate::expr::{self, GlobalId, LocalId};
use crate::plan::Plan;
use crate::repr::DiffRow;

/// The Context for build a Operator with id of `GlobalId`
pub struct Context<'referred, 'df> {
    pub id: GlobalId,
    pub df: &'referred mut Hydroflow<'df>,
    pub compute_state: &'referred mut ComputeState,
    /// a list of all collections being used in the operator
    pub recv_collections: BTreeMap<GlobalId, CollectionBundle>,
    /// used by `Get`/`Let` Plan for getting/setting local variables
    local_scope: Vec<BTreeMap<LocalId, CollectionBundle>>,
    // TODO: error Collector
}

impl<'referred, 'df> Context<'referred, 'df> {
    /// Interpret and execute plan
    ///
    /// return the output of this plan
    pub fn render_plan(&mut self, plan: Plan) -> Result<CollectionBundle, Error> {
        let ret = match plan {
            Plan::Constant { rows } => self.render_constant(rows),
            Plan::Get { id } => self.get_by_id(id)?,
            Plan::Let { id, value, body } => self.eval_let(id, value, body)?,
            Plan::Mfp { input, mfp } => todo!(),
            Plan::Reduce {
                input,
                key_val_plan,
                reduce_plan,
            } => todo!(),
            Plan::Join { inputs, plan } => todo!(),
            Plan::Union {
                inputs,
                consolidate_output,
            } => todo!(),
        };
        Ok(ret)
    }

    /// render Constant, will only emit the `rows` once.
    pub fn render_constant(&mut self, mut rows: Vec<DiffRow>) -> CollectionBundle {
        let (send_port, recv_port) = self.df.make_edge::<_, Toff>("constant");

        self.df
            .add_subgraph_source("Constant", send_port, move |_ctx, send_port| {
                send_port.give(std::mem::take(&mut rows));
            });
        CollectionBundle::from_collection(Collection::from_port(recv_port))
    }

    pub fn get_by_id(&mut self, id: expr::Id) -> Result<CollectionBundle, Error> {
        let ret = match id {
            expr::Id::Local(local) => {
                let bundle = self
                    .local_scope
                    .iter()
                    .rev()
                    .find_map(|scope| scope.get(&local))
                    .context(InvalidQuerySnafu {
                        reason: format!("Local variable {:?} not found", local),
                    })?;
                bundle.clone(self.df)
            }
            expr::Id::Global(id) => {
                let bundle = self.recv_collections.get(&id).context(InvalidQuerySnafu {
                    reason: format!("Collection {:?} not found", id),
                })?;
                bundle.clone(self.df)
            }
        };
        Ok(ret)
    }

    pub fn eval_let(
        &mut self,
        id: LocalId,
        value: Box<Plan>,
        body: Box<Plan>,
    ) -> Result<CollectionBundle, Error> {
        let value = self.render_plan(*value)?;
        let local_scope = if let Some(last) = self.local_scope.last_mut() {
            last
        } else {
            self.local_scope.push(Default::default());
            self.local_scope.last_mut().unwrap()
        };
        local_scope.insert(id, value);
        let ret = self.render_plan(*body)?;
        Ok(ret)
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::rc::Rc;

    use hydroflow::scheduled::graph::Hydroflow;
    use hydroflow::scheduled::graph_ext::GraphExt;
    use hydroflow::scheduled::handoff::VecHandoff;

    use super::*;
    use crate::repr::Row;

    fn harness_test_ctx<'r, 'h>(
        df: &'r mut Hydroflow<'h>,
        state: &'r mut ComputeState,
    ) -> Context<'r, 'h> {
        Context {
            id: GlobalId::User(0),
            df,
            compute_state: state,
            recv_collections: BTreeMap::new(),
            local_scope: Default::default(),
        }
    }

    #[test]
    fn test_render_constant() {
        let mut df = Hydroflow::new();
        let mut state = ComputeState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::empty(), 1, 1),
            (Row::empty(), 2, 2),
            (Row::empty(), 3, 3),
        ];
        let collection = ctx.render_constant(rows.clone());
        let collection = collection.collection.clone(ctx.df);
        let cnt = Rc::new(RefCell::new(0));
        let cnt_inner = cnt.clone();
        ctx.df.add_subgraph_sink(
            "test_render_constant",
            collection.stream,
            move |_ctx, recv| {
                let data = recv.take_inner();
                *cnt_inner.borrow_mut() += data.iter().map(|v| v.len()).sum::<usize>();
            },
        );
        ctx.df.run_available();
        assert_eq!(*cnt.borrow(), 3);
        ctx.df.run_available();
        assert_eq!(*cnt.borrow(), 3);
    }

    #[test]
    fn example_source_sink() {
        let mut df = Hydroflow::new();
        let (send_port, recv_port) = df.make_edge::<_, VecHandoff<i32>>("test_handoff");
        df.add_subgraph_source("test_handoff_source", send_port, move |_ctx, send| {
            for i in 0..10 {
                send.give(vec![i]);
            }
        });

        let sum = Rc::new(RefCell::new(0));
        let sum_move = sum.clone();
        let sink = df.add_subgraph_sink("test_handoff_sink", recv_port, move |_ctx, recv| {
            let data = recv.take_inner();
            *sum_move.borrow_mut() += data.iter().sum::<i32>();
        });

        df.run_available();
        assert_eq!(sum.borrow().to_owned(), 45);
        df.schedule_subgraph(sink);
        df.run_available();

        assert_eq!(sum.borrow().to_owned(), 45);
    }
}
