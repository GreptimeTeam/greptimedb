use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;

/// Create a function signature with oneof signatures of interleaving two arguments.
pub fn one_of_sigs2(args1: Vec<ConcreteDataType>, args2: Vec<ConcreteDataType>) -> Signature {
    let mut sigs = Vec::with_capacity(args1.len() + args2.len());

    for arg1 in &args1 {
        for arg2 in &args2 {
            sigs.push(TypeSignature::Exact(vec![arg1.clone(), arg2.clone()]));
        }
    }

    Signature::one_of(sigs, Volatility::Immutable)
}
