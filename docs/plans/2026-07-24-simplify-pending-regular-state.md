# Simplify Pending Regular Compaction State Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the separate regular-replan flag and waiter vector with one optional waiter vector without changing compaction scheduling or waiter completion behavior.

**Architecture:** `CompactionStatus::pending_regular: Option<Vec<OutputTx>>` is the single source of truth. `Some(empty)` records an automatic trigger without a waiter; `Some(waiters)` records explicit callers waiting for the follow-up pick. Starting the follow-up consumes the option and promotes its waiters to active waiters.

**Tech Stack:** Rust, mito2 compaction scheduler, Tokio oneshot waiters.

---

### Task 1: Collapse Pending Regular State

**Files:**
- Modify: `src/mito2/src/compaction.rs:383,872,1296-1498`
- Test: existing behavioral tests in `src/mito2/src/compaction.rs`

**Step 1: Replace the fields and initialization**

Replace:

```rust
regular_replan_pending: bool,
pending_regular_waiters: Vec<OutputTx>,
```

with:

```rust
/// Waiters owned by a retained regular follow-up; `Some(empty)` records an automatic trigger.
pending_regular: Option<Vec<OutputTx>>,
```

Initialize it with `None` in `CompactionStatus::new`.

**Step 2: Update trigger retention and follow-up startup**

Implement `merge_regular_trigger` with `get_or_insert_default()` so an automatic trigger remains observable without a waiter. Update `start_regular_picking` to consume `pending_regular` and append any stored waiters before entering `Picking`.

**Step 3: Update completion checks**

Replace both `regular_replan_pending` checks with `pending_regular.is_some()` while preserving current manual-compaction and DDL ordering.

**Step 4: Update terminal waiter cleanup**

In `on_failure` and `on_cancel`, consume `pending_regular.unwrap_or_default()` and chain those waiters with active waiters so every retained explicit request receives the same terminal error as before.

**Step 5: Run static verification**

Run:

```bash
/usr/bin/git diff --check
/usr/bin/rg -n "regular_replan_pending|pending_regular_waiters" src/mito2/src
/usr/bin/rg -n "pending_regular" src/mito2/src/compaction.rs
```

Expected:

- `git diff --check` exits successfully.
- No old field-name matches remain.
- The new field appears in initialization, trigger retention, follow-up scheduling, and terminal cleanup paths.

Do not run cargo or make commands unless the user explicitly requests a named verification command.

**Step 6: Commit**

```bash
git add src/mito2/src/compaction.rs
git commit -s -m "refactor(mito2): simplify pending regular compaction state"
```
