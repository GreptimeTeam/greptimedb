#[cfg(all(unix, not(miri)))]
mod unix;
// todo(hl): maybe support windows seek_write/seek_read
#[cfg(any(not(unix), miri))]
mod fallback;

#[cfg(any(all(not(unix), not(windows)), miri))]
use fallback::{pread_exact, pread_exact_or_eof, pwrite_all};
#[cfg(all(unix, not(miri)))]
use unix::{pread_exact, pread_exact_or_eof, pwrite_all};
