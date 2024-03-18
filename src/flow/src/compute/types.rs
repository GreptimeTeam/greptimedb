use hydroflow::scheduled::handoff::TeeingHandoff;
use hydroflow::scheduled::port::RecvPort;

/// A collection, represent a collections of data that is received from a handoff.
pub type Collection<T> = RecvPort<TeeingHandoff<T>>;

