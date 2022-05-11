use std::backtrace::Backtrace;
use std::panic;
#[cfg(feature = "deadlock_detection")]
use std::time::Duration;

pub fn set_panic_hook() {
    // Set a panic hook that records the panic as a `tracing` event at the
    // `ERROR` verbosity level.
    //
    // If we are currently in a span when the panic occurred, the logged event
    // will include the current span, allowing the context in which the panic
    // occurred to be recorded.
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic| {
        let backtrace = Backtrace::force_capture();
        let backtrace = format!("{:?}", backtrace);
        if let Some(location) = panic.location() {
            tracing::error!(
                message = %panic,
                backtrace = %backtrace,
                panic.file = location.file(),
                panic.line = location.line(),
                panic.column = location.column(),
            );
        } else {
            tracing::error!(message = %panic, backtrace = %backtrace);
        }
        default_hook(panic);
    }));

    #[cfg(feature = "deadlock_detection")]
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(5));
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        tracing::info!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            tracing::info!("Deadlock #{}", i);
            for t in threads {
                tracing::info!("Thread Id {:#?}", t.thread_id());
                tracing::info!("{:#?}", t.backtrace());
            }
        }
    });
}
