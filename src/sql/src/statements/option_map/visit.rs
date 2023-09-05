use std::ops::ControlFlow;

use sqlparser::ast::{Visit, Visitor};

use crate::statements::OptionMap;

impl Visit for OptionMap {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        for (k, v) in &self.map {
            k.visit(visitor)?;
            v.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}
