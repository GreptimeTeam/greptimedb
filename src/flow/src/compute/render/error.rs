use std::hash::Hash;

use differential_dataflow::ExchangeData;

use crate::repr::Row;

/// Used to make possibly-validating code generic: think of this as a kind of `MaybeResult`,
/// specialized for use in compute.  Validation code will only run when the error constructor is
/// Some.
pub(super) trait MaybeValidatingRow<T, E>: ExchangeData + Hash {
    fn ok(t: T) -> Self;
    fn into_error() -> Option<fn(E) -> Self>;
}

impl<E> MaybeValidatingRow<Row, E> for Row {
    fn ok(t: Row) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<E> MaybeValidatingRow<(), E> for () {
    fn ok(t: ()) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<E, R> MaybeValidatingRow<Vec<R>, E> for Vec<R>
where
    R: ExchangeData + Hash,
{
    fn ok(t: Vec<R>) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<T, E> MaybeValidatingRow<T, E> for Result<T, E>
where
    T: ExchangeData + Hash,
    E: ExchangeData + Hash,
{
    fn ok(row: T) -> Self {
        Ok(row)
    }

    fn into_error() -> Option<fn(E) -> Self> {
        Some(Err)
    }
}
