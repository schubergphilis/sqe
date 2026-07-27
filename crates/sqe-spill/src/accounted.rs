//! [`Accounted<T>`]: a value that carries its [`BytePermit`] so ownership
//! moves never double-charge the budget.

use crate::budget::BytePermit;

/// A value paired with the byte permit that paid for it.
///
/// Moving `Accounted<T>` between queues, tasks, or the Flight encoder does
/// **not** change the charge. Dropping it (or calling [`Accounted::into_parts`]
/// and dropping the permit) releases the charge exactly once.
#[derive(Debug)]
pub struct Accounted<T> {
    value: T,
    permit: BytePermit,
    /// Logical (pre-rounding) size the caller associated with `value`.
    logical_bytes: usize,
}

impl<T> Accounted<T> {
    /// Pair `value` with an already-acquired permit.
    pub fn new(value: T, permit: BytePermit, logical_bytes: usize) -> Self {
        Self {
            value,
            permit,
            logical_bytes,
        }
    }

    /// Borrow the inner value.
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Mutably borrow the inner value.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Logical bytes requested when the permit was acquired.
    pub fn logical_bytes(&self) -> usize {
        self.logical_bytes
    }

    /// Bytes actually charged (rounded to budget units).
    pub fn charged_bytes(&self) -> usize {
        self.permit.charged_bytes()
    }

    /// Split into value and permit. The caller becomes responsible for the
    /// permit's lifetime (e.g. hold it across encoding, then drop).
    pub fn into_parts(self) -> (T, BytePermit) {
        (self.value, self.permit)
    }

    /// Consume and return only the value, dropping the permit immediately.
    pub fn into_inner(self) -> T {
        self.value
    }

    /// Map the inner value while keeping the same permit.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Accounted<U> {
        Accounted {
            value: f(self.value),
            permit: self.permit,
            logical_bytes: self.logical_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::ByteBudget;

    #[tokio::test]
    async fn move_does_not_change_charge() {
        let budget = ByteBudget::with_granularity("acc", 1024, 256, None);
        let permit = budget.acquire(100).await.unwrap();
        let a = Accounted::new(vec![1u8, 2, 3], permit, 100);
        assert_eq!(budget.used_bytes(), 256);

        let b = a; // move
        assert_eq!(budget.used_bytes(), 256);
        assert_eq!(b.get(), &vec![1u8, 2, 3]);

        let (val, permit) = b.into_parts();
        assert_eq!(val, vec![1u8, 2, 3]);
        assert_eq!(budget.used_bytes(), 256);
        drop(permit);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn drop_releases_exactly_once() {
        let budget = ByteBudget::with_granularity("drop", 1024, 256, None);
        {
            let permit = budget.acquire(1).await.unwrap();
            let _a = Accounted::new(42i32, permit, 1);
            assert_eq!(budget.used_bytes(), 256);
        }
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn map_preserves_permit() {
        let budget = ByteBudget::with_granularity("map", 1024, 256, None);
        let permit = budget.acquire(50).await.unwrap();
        let a = Accounted::new(1u32, permit, 50).map(|x| x + 1);
        assert_eq!(*a.get(), 2);
        assert_eq!(budget.used_bytes(), 256);
        drop(a);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn into_inner_releases_permit() {
        let budget = ByteBudget::with_granularity("inner", 1024, 256, None);
        let permit = budget.acquire(1).await.unwrap();
        let a = Accounted::new(String::from("x"), permit, 1);
        let s = a.into_inner();
        assert_eq!(s, "x");
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn arc_clone_of_budget_shares_capacity() {
        let budget = ByteBudget::with_granularity("share", 512, 256, None);
        let b2 = budget.clone();
        let p1 = budget.acquire(1).await.unwrap();
        let p2 = b2.acquire(1).await.unwrap();
        assert_eq!(budget.used_bytes(), 512);
        assert!(b2.try_acquire(1).is_err());
        drop(p1);
        drop(p2);
        assert_eq!(budget.used_bytes(), 0);
    }
}
