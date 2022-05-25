use std::fmt;
use std::ops::{Deref, DerefMut};

/// Cache line padded value wrapper to avoid false sharing.
/// X86-64 and ARM is 128 byte aligned.
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(128)))]
#[cfg_attr(
    not(any(target_arch = "x86_64", target_arch = "aarch64")),
    repr(align(64))
)]
#[derive(Default, PartialEq)]
pub struct Padded<T> {
    val: T,
}
#[allow(unsafe_code)]
unsafe impl<T: Send> Send for Padded<T> {}

#[allow(unsafe_code)]
unsafe impl<T: Sync> Sync for Padded<T> {}

impl<T> Padded<T> {
    /// Pads and aligns a value to the length of a cache line.
    pub const fn new(t: T) -> Padded<T> {
        Padded::<T> { val: t }
    }
}

impl<T> Deref for Padded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.val
    }
}

impl<T> DerefMut for Padded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.val
    }
}

impl<T: fmt::Debug> fmt::Debug for Padded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Padded").field("val", &self.val).finish()
    }
}

impl<T> From<T> for Padded<T> {
    fn from(t: T) -> Self {
        Padded::new(t)
    }
}
