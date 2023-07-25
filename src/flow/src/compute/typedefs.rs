use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};

use crate::repr::{Diff, Row, Timestamp};
use crate::storage::errors::DataflowError;

// TODO(discord9): consider use ColValSpine for columnation storage

/// T: Time, R: Diff, O: Offset
pub type RowSpine<K, V, T, R, O = usize> = OrdValSpine<K, V, T, R, O>;
/// T: Time, R: Diff, O: Offset
pub type RowKeySpine<K, T, R, O = usize> = OrdKeySpine<K, T, R, O>;
/// T: Time, R: Diff, O: Offset
pub type ErrSpine<K, T, R, O = usize> = OrdKeySpine<K, T, R, O>;
/// T: Time, R: Diff, O: Offset
pub type ErrValSpine<K, T, R, O = usize> = OrdValSpine<K, DataflowError, T, R, O>;
pub type TraceRowHandle<K, V, T, R> = TraceAgent<RowSpine<K, V, T, R>>;
pub type TraceErrHandle<K, T, R> = TraceAgent<ErrSpine<K, T, R>>;
pub type KeysValsHandle = TraceRowHandle<Row, Row, Timestamp, Diff>;
pub type ErrsHandle = TraceErrHandle<DataflowError, Timestamp, Diff>;
