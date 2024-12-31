use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU8, Ordering},
};

/// A mutex wrapping an option that exposes whether the contents
/// are `Some` or `None` without requiring locking.
pub struct OptionMutex<T> {
    lock: AtomicU8,
    data: UnsafeCell<Option<T>>,
}

pub struct OptionMutexGuard<'a, T> {
    lock: &'a OptionMutex<T>,
}

const UNLOCKED_NONE: u8 = 0;
const UNLOCKED_SOME: u8 = 1;
const LOCKED: u8 = 2;

impl<T> OptionMutex<T> {
    pub fn new(value: T) -> Self {
        Self {
            lock: AtomicU8::new(UNLOCKED_SOME),
            data: UnsafeCell::new(Some(value)),
        }
    }

    pub fn new_none() -> Self {
        Self {
            lock: AtomicU8::new(UNLOCKED_NONE),
            data: UnsafeCell::new(None),
        }
    }

    pub fn is_unlocked_some(&self, ordering: Ordering) -> bool {
        self.lock.load(ordering) == UNLOCKED_SOME
    }

    pub fn is_unlocked_none(&self, ordering: Ordering) -> bool {
        self.lock.load(ordering) == UNLOCKED_NONE
    }

    pub fn try_lock(&self) -> Option<OptionMutexGuard<T>> {
        // Unlike lock_if, we assume that this operation will succeed,
        // so we don't spend time on an extra load.
        (self.lock.swap(LOCKED, Ordering::Acquire) != LOCKED)
            .then(|| OptionMutexGuard { lock: self })
    }

    fn lock_if(&self, state: u8) -> Option<OptionMutexGuard<T>> {
        // Check the state with an ordinary load before doing a compare_exchange to be friendlier to caches.
        // compare_exchange will take exclusive access to the cache line,
        // and we only want to take shared access when we fail to lock.
        (self.lock.load(Ordering::Relaxed) == state
            && self
                .lock
                .compare_exchange(state, LOCKED, Ordering::Acquire, Ordering::Relaxed)
                .is_ok())
        .then(|| OptionMutexGuard { lock: self })
    }

    pub fn lock_if_none(&self) -> Option<OptionMutexGuard<T>> {
        self.lock_if(UNLOCKED_NONE)
    }

    pub fn lock_if_some(&self) -> Option<OptionMutexGuard<T>> {
        self.lock_if(UNLOCKED_SOME)
    }

    pub fn take(&self) -> Option<T> {
        self.lock_if_some()?.take()
    }
}

impl<T> Default for OptionMutex<T> {
    fn default() -> Self {
        Self::new_none()
    }
}

unsafe impl<T: Send> Sync for OptionMutex<T> {}

unsafe impl<T: Send> Send for OptionMutex<T> {}

impl<T> Deref for OptionMutexGuard<'_, T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        // SAFETY: We own the lock on the mutex
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for OptionMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: We own the lock on the mutex
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for OptionMutexGuard<'_, T> {
    fn drop(&mut self) {
        let val = if self.is_some() {
            UNLOCKED_SOME
        } else {
            UNLOCKED_NONE
        };
        self.lock.lock.store(val, Ordering::Release);
    }
}
