# Simplify Pending Regular Compaction State

## Goal

Represent a retained regular compaction follow-up with one field while preserving the rule that an explicit regular-compaction waiter completes only after the follow-up pick finishes.

## Design

Replace `CompactionStatus::regular_replan_pending: bool` and `pending_regular_waiters: Vec<OutputTx>` with `pending_regular: Option<Vec<OutputTx>>`.

`None` means no follow-up pick is required. `Some(Vec::new())` represents an automatic regular trigger without a waiter. `Some(waiters)` represents a required follow-up pick whose explicit request waiters must move into the active `waiters` list when that pick starts.

While the current phase is `Picking` and no DDL is pending, `merge_regular_trigger` creates `pending_regular` if necessary and appends an optional waiter. `start_regular_picking` consumes the option, transfers its waiters, and starts the next picking phase. Planning/execution completion checks `pending_regular.is_some()` to decide whether to schedule a follow-up. Failure and cancellation paths drain both active waiters and the optional pending waiters.

## Invariants

- A retained automatic trigger remains observable even when it has no waiter.
- Explicit regular waiters are never completed by the older pick that preceded their trigger.
- A queued DDL still fences later regular triggers from creating follow-up picks.
- Pending regular state is consumed exactly once when the follow-up pick starts.

## Testing

Update retained scheduler tests to use the single option. Keep behavioral tests covering DDL fencing, lifecycle failure/cancellation, and follow-up waiter completion; do not add field-level tests.
