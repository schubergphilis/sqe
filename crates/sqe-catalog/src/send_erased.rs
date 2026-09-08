//! Hold a value across an `.await` without letting the coroutine's `Send`
//! proof walk its type graph (compile time, see
//! docs/internal/plans/2026-09-07-compile-time-async-hubs.md, "second look").
//!
//! An `async` block that keeps a `Vec<Expr>` or a `SessionState` alive
//! across an await is only `Send` if rustc can prove those types `Send`, and
//! that proof walks DataFusion's entire `Expr` / `LogicalPlan` graph, about
//! 2 s per method, once per method, because every method has its own
//! parameter environment. The proof is not in doubt (both types are `Send`
//! and `Sync`, and a unit test below asserts it), only expensive.
//! [`SendErased`] states the fact once with an `unsafe impl` so the
//! per-method proof stops at the wrapper.
//!
//! Constructing one requires [`KnownSend`], an `unsafe` marker implemented
//! only for the types this crate has verified, so the wrapper cannot be used
//! to smuggle a non-`Send` value. Prefer boxing a future at its definition
//! or type-erasing to a `dyn` trait object where that is possible; this is
//! for the cases where DataFusion's trait signature forces the coroutine to
//! own the value (a borrowed `&[Expr]` the inner scan needs, a
//! `SessionState` built for a scoped scan).

use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::Expr;

/// Types whose `Send + Sync`-ness this crate asserts by hand for
/// [`SendErased`].
///
/// # Safety
///
/// The implementor promises the type is `Send + Sync`. Every implementation
/// is checked the ordinary way by `known_types_are_send_and_sync` in the
/// test build; add the new type there when adding an `impl`.
pub(crate) unsafe trait KnownSend {}

unsafe impl KnownSend for Vec<Expr> {}
unsafe impl KnownSend for &[Expr] {}
unsafe impl KnownSend for SessionState {}

/// A value the coroutine may hold across an await without paying the
/// auto-trait proof for its type. See the module docs.
pub(crate) struct SendErased<T: KnownSend>(T);

// SAFETY: `T: KnownSend` is only implemented for types that are `Send` and
// `Sync` (asserted structurally in `tests::known_types_are_send_and_sync`);
// the wrapper adds no interior mutability or raw pointers of its own.
unsafe impl<T: KnownSend> Send for SendErased<T> {}
unsafe impl<T: KnownSend> Sync for SendErased<T> {}

impl<T: KnownSend> SendErased<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(value)
    }

    pub(crate) fn get(&self) -> &T {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    /// The fact the `unsafe impl`s above rely on, proven by rustc the
    /// ordinary way. Adding a `KnownSend` impl means adding a line here.
    #[test]
    fn known_types_are_send_and_sync() {
        assert_send_sync::<Vec<Expr>>();
        assert_send_sync::<&[Expr]>();
        assert_send_sync::<SessionState>();
    }
}
