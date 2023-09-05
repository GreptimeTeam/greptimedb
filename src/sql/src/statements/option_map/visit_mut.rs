use std::ops::ControlFlow;

use sqlparser::ast::{VisitMut, VisitorMut};

use crate::statements::OptionMap;

impl VisitMut for OptionMap {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        for (_, v) in self.map.iter_mut() {
            v.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}
