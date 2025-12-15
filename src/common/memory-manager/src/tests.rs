use tokio::time::{Duration, sleep};

use crate::{MemoryManager, NoOpMetrics, PERMIT_GRANULARITY_BYTES};

#[test]
fn test_try_acquire_unlimited() {
    let manager = MemoryManager::new(0, NoOpMetrics);
    let guard = manager.try_acquire(10 * PERMIT_GRANULARITY_BYTES).unwrap();
    assert_eq!(manager.limit_bytes(), 0);
    assert_eq!(guard.granted_bytes(), 0);
}

#[test]
fn test_try_acquire_limited_success_and_release() {
    let bytes = 2 * PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(bytes, NoOpMetrics);
    {
        let guard = manager.try_acquire(PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(guard.granted_bytes(), PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), PERMIT_GRANULARITY_BYTES);
        drop(guard);
    }
    assert_eq!(manager.used_bytes(), 0);
}

#[test]
fn test_try_acquire_exceeds_limit() {
    let limit = PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(limit, NoOpMetrics);
    let result = manager.try_acquire(limit + PERMIT_GRANULARITY_BYTES);
    assert!(result.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn test_acquire_blocks_and_unblocks() {
    let bytes = 2 * PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(bytes, NoOpMetrics);
    let guard = manager.try_acquire(bytes).unwrap();

    // Spawn a task that will block on acquire()
    let waiter = {
        let manager = manager.clone();
        tokio::spawn(async move {
            // This will block until memory is available
            let _guard = manager.acquire(bytes).await.unwrap();
        })
    };

    sleep(Duration::from_millis(10)).await;
    // Release memory - this should unblock the waiter
    drop(guard);

    // Waiter should complete now
    waiter.await.unwrap();
}

#[test]
fn test_request_additional_success() {
    let limit = 10 * PERMIT_GRANULARITY_BYTES; // 10MB limit
    let manager = MemoryManager::new(limit, NoOpMetrics);

    // Acquire base quota (5MB)
    let base = 5 * PERMIT_GRANULARITY_BYTES;
    let mut guard = manager.try_acquire(base).unwrap();
    assert_eq!(guard.granted_bytes(), base);
    assert_eq!(manager.used_bytes(), base);

    // Request additional memory (3MB) - should succeed and merge
    assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
}

#[test]
fn test_request_additional_exceeds_limit() {
    let limit = 10 * PERMIT_GRANULARITY_BYTES; // 10MB limit
    let manager = MemoryManager::new(limit, NoOpMetrics);

    // Acquire base quota (5MB)
    let base = 5 * PERMIT_GRANULARITY_BYTES;
    let mut guard = manager.try_acquire(base).unwrap();

    // Request additional memory (3MB) - should succeed
    assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

    // Request more (3MB) - should fail (would exceed 10MB limit)
    let result = guard.request_additional(3 * PERMIT_GRANULARITY_BYTES);
    assert!(!result);

    // Still at 8MB
    assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(guard.granted_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
}

#[test]
fn test_request_additional_auto_release_on_guard_drop() {
    let limit = 10 * PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(limit, NoOpMetrics);

    {
        let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Request additional - memory is merged into guard
        assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

        // When guard drops, all memory (base + additional) is released together
    }

    // After scope, all memory should be released
    assert_eq!(manager.used_bytes(), 0);
}

#[test]
fn test_request_additional_unlimited() {
    let manager = MemoryManager::new(0, NoOpMetrics); // Unlimited
    let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

    // Should always succeed with unlimited manager
    assert!(guard.request_additional(100 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 0);
    assert_eq!(manager.used_bytes(), 0);
}

#[test]
fn test_request_additional_zero_bytes() {
    let limit = 10 * PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(limit, NoOpMetrics);

    let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

    // Request 0 bytes should succeed without affecting anything
    assert!(guard.request_additional(0));
    assert_eq!(guard.granted_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
}

#[test]
fn test_early_release_partial_success() {
    let limit = 10 * PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(limit, NoOpMetrics);

    let mut guard = manager.try_acquire(8 * PERMIT_GRANULARITY_BYTES).unwrap();
    assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

    // Release half
    assert!(guard.early_release_partial(4 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 4 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 4 * PERMIT_GRANULARITY_BYTES);

    // Released memory should be available to others
    let _guard2 = manager.try_acquire(4 * PERMIT_GRANULARITY_BYTES).unwrap();
    assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
}

#[test]
fn test_early_release_partial_exceeds_granted() {
    let manager = MemoryManager::new(10 * PERMIT_GRANULARITY_BYTES, NoOpMetrics);
    let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

    // Try to release more than granted - should fail
    assert!(!guard.early_release_partial(10 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
}

#[test]
fn test_early_release_partial_unlimited() {
    let manager = MemoryManager::new(0, NoOpMetrics);
    let mut guard = manager.try_acquire(100 * PERMIT_GRANULARITY_BYTES).unwrap();

    // Unlimited guard - release should succeed (no-op)
    assert!(guard.early_release_partial(50 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 0);
}

#[test]
fn test_request_and_early_release_symmetry() {
    let limit = 20 * PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(limit, NoOpMetrics);

    let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

    // Request additional
    assert!(guard.request_additional(5 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 10 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 10 * PERMIT_GRANULARITY_BYTES);

    // Early release some
    assert!(guard.early_release_partial(3 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 7 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 7 * PERMIT_GRANULARITY_BYTES);

    // Request again
    assert!(guard.request_additional(2 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 9 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 9 * PERMIT_GRANULARITY_BYTES);

    // Early release again
    assert!(guard.early_release_partial(4 * PERMIT_GRANULARITY_BYTES));
    assert_eq!(guard.granted_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
    assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);

    drop(guard);
    assert_eq!(manager.used_bytes(), 0);
}

#[test]
fn test_small_allocation_rounds_up() {
    // Test that allocations smaller than PERMIT_GRANULARITY_BYTES
    // round up to 1 permit and can use request_additional()
    let limit = 10 * PERMIT_GRANULARITY_BYTES;
    let manager = MemoryManager::new(limit, NoOpMetrics);

    let mut guard = manager.try_acquire(512 * 1024).unwrap(); // 512KB
    assert_eq!(guard.granted_bytes(), PERMIT_GRANULARITY_BYTES); // Rounds up to 1MB
    assert!(guard.request_additional(2 * PERMIT_GRANULARITY_BYTES)); // Can request more
    assert_eq!(guard.granted_bytes(), 3 * PERMIT_GRANULARITY_BYTES);
}

#[test]
fn test_acquire_zero_bytes_lazy_allocation() {
    // Test that acquire(0) returns 0 permits but can request_additional() later
    let manager = MemoryManager::new(10 * PERMIT_GRANULARITY_BYTES, NoOpMetrics);

    let mut guard = manager.try_acquire(0).unwrap();
    assert_eq!(guard.granted_bytes(), 0); // No permits consumed
    assert_eq!(manager.used_bytes(), 0);

    assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES)); // Lazy allocation
    assert_eq!(guard.granted_bytes(), 3 * PERMIT_GRANULARITY_BYTES);
}
