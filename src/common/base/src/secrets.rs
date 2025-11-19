// This file is copied from https://github.com/iqlusioninc/crates/blob/f98d4ccf/secrecy/src/lib.rs

//! [`SecretBox`] wrapper type for more carefully handling secret values
//! (e.g. passwords, cryptographic keys, access tokens or other credentials)
//!
//! # Goals
//!
//! - Make secret access explicit and easy-to-audit via the
//!   [`ExposeSecret`] and [`ExposeSecretMut`] traits.
//! - Prevent accidental leakage of secrets via channels like debug logging
//! - Ensure secrets are wiped from memory on drop securely
//!   (using the [`zeroize`] crate)
//!
//! Presently this crate favors a simple, `no_std`-friendly, safe i.e.
//! `forbid(unsafe_code)`-based implementation and does not provide more advanced
//! memory protection mechanisms e.g. ones based on `mlock(2)`/`mprotect(2)`.
//! We may explore more advanced protection mechanisms in the future.
//! Those who don't mind `std` and `libc` dependencies should consider using
//! the [`secrets`](https://crates.io/crates/secrets) crate.
//!
//! # `serde` support
//!
//! When the `serde` feature of this crate is enabled, the [`SecretBox`] type will
//! receive a [`Deserialize`] impl for all `SecretBox<T>` types where
//! `T: DeserializeOwned`. This allows *loading* secret values from data
//! deserialized from `serde` (be careful to clean up any intermediate secrets
//! when doing this, e.g. the unparsed input!)
//!
//! To prevent exfiltration of secret values via `serde`, by default `SecretBox<T>`
//! does *not* receive a corresponding [`Serialize`] impl. If you would like
//! types of `SecretBox<T>` to be serializable with `serde`, you will need to impl
//! the [`SerializableSecret`] marker trait on `T`

use std::fmt::{Debug, Display};
use std::{any, fmt};

use serde::{Deserialize, Serialize, de, ser};
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Wrapper type for strings that contains secrets. See also [SecretBox].
pub type SecretString = SecretBox<String>;

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        SecretString::new(Box::new(value))
    }
}

impl Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.expose_secret().is_empty() {
            write!(f, "")
        } else {
            write!(f, "SecretString([REDACTED])")
        }
    }
}

/// Wrapper type for values that contains secrets.
///
/// It attempts to limit accidental exposure and ensure secrets are wiped from memory when dropped.
/// (e.g. passwords, cryptographic keys, access tokens or other credentials)
///
/// Access to the secret inner value occurs through the [`ExposeSecret`]
/// or [`ExposeSecretMut`] traits, which provide methods for accessing the inner secret value.
pub struct SecretBox<S: Zeroize> {
    inner_secret: Box<S>,
}

impl<S: Zeroize> Zeroize for SecretBox<S> {
    fn zeroize(&mut self) {
        self.inner_secret.as_mut().zeroize()
    }
}

impl<S: Zeroize> Drop for SecretBox<S> {
    fn drop(&mut self) {
        self.zeroize()
    }
}

impl<S: Zeroize> ZeroizeOnDrop for SecretBox<S> {}

impl<S: Zeroize> From<Box<S>> for SecretBox<S> {
    fn from(source: Box<S>) -> Self {
        Self::new(source)
    }
}

impl<S: Zeroize> SecretBox<S> {
    /// Create a secret value using a pre-boxed value.
    pub fn new(boxed_secret: Box<S>) -> Self {
        Self {
            inner_secret: boxed_secret,
        }
    }
}

impl<S: Zeroize + Default> SecretBox<S> {
    /// Create a secret value using a function that can initialize the vale in-place.
    pub fn new_with_mut(ctr: impl FnOnce(&mut S)) -> Self {
        let mut secret = Self::default();
        ctr(secret.expose_secret_mut());
        secret
    }
}

impl<S: Zeroize + Clone> SecretBox<S> {
    /// Create a secret value using the provided function as a constructor.
    ///
    /// The implementation makes an effort to zeroize the locally constructed value
    /// before it is copied to the heap, and constructing it inside the closure minimizes
    /// the possibility of it being accidentally copied by other code.
    ///
    /// **Note:** using [`Self::new`] or [`Self::new_with_mut`] is preferable when possible,
    /// since this method's safety relies on empyric evidence and may be violated on some targets.
    pub fn new_with_ctr(ctr: impl FnOnce() -> S) -> Self {
        let mut data = ctr();
        let secret = Self {
            inner_secret: Box::new(data.clone()),
        };
        data.zeroize();
        secret
    }

    /// Same as [`Self::new_with_ctr`], but the constructor can be fallible.
    ///
    ///
    /// **Note:** using [`Self::new`] or [`Self::new_with_mut`] is preferable when possible,
    /// since this method's safety relies on empyric evidence and may be violated on some targets.
    pub fn try_new_with_ctr<E>(ctr: impl FnOnce() -> Result<S, E>) -> Result<Self, E> {
        let mut data = ctr()?;
        let secret = Self {
            inner_secret: Box::new(data.clone()),
        };
        data.zeroize();
        Ok(secret)
    }
}

impl<S: Zeroize + Default> Default for SecretBox<S> {
    fn default() -> Self {
        Self {
            inner_secret: Box::<S>::default(),
        }
    }
}

impl<S: Zeroize> Debug for SecretBox<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretBox<{}>([REDACTED])", any::type_name::<S>())
    }
}

impl<S> Clone for SecretBox<S>
where
    S: Clone + Zeroize,
{
    fn clone(&self) -> Self {
        SecretBox {
            inner_secret: self.inner_secret.clone(),
        }
    }
}

impl<S: Zeroize> ExposeSecret<S> for SecretBox<S> {
    fn expose_secret(&self) -> &S {
        self.inner_secret.as_ref()
    }
}

impl<S: Zeroize> ExposeSecretMut<S> for SecretBox<S> {
    fn expose_secret_mut(&mut self) -> &mut S {
        self.inner_secret.as_mut()
    }
}

impl<S> PartialEq for SecretBox<S>
where
    S: PartialEq + Zeroize,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner_secret == other.inner_secret
    }
}

/// Expose a reference to an inner secret
pub trait ExposeSecret<S> {
    /// Expose secret: this is the only method providing access to a secret.
    fn expose_secret(&self) -> &S;
}

/// Expose a mutable reference to an inner secret
pub trait ExposeSecretMut<S> {
    /// Expose secret: this is the only method providing access to a secret.
    fn expose_secret_mut(&mut self) -> &mut S;
}

/// Marker trait for secret types which can be [`Serialize`]-d by [`serde`].
///
/// When the `serde` feature of this crate is enabled and types are marked with
/// this trait, they receive a [`Serialize` impl][1] for `SecretBox<T>`.
/// (NOTE: all types which impl `DeserializeOwned` receive a [`Deserialize`]
/// impl)
///
/// This is done deliberately to prevent accidental exfiltration of secrets
/// via `serde` serialization.
///
/// If you really want to have `serde` serialize those types, use the
/// [`serialize_with`][2] attribute to specify a serializer that exposes the secret.
///
/// [1]: https://docs.rs/secrecy/latest/secrecy/struct.Secret.html#implementations
/// [2]: https://serde.rs/field-attrs.html#serialize_with
pub trait SerializableSecret: Serialize {}

impl<'de, T> Deserialize<'de> for SecretBox<T>
where
    T: Zeroize + Clone + de::DeserializeOwned + Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Self::try_new_with_ctr(|| T::deserialize(deserializer))
    }
}

impl<T> Serialize for SecretBox<T>
where
    T: Zeroize + SerializableSecret + Serialize + Sized,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        self.expose_secret().serialize(serializer)
    }
}
