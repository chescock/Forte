//! This module provides a toolbox of scheduling primitives called Latches.
//!
//! In the abstract, a latch is a way to block some logic from progressing on a
//! given thread. All latches begin "closed", blocking some logic. When the
//! latch is "opened", the logic progresses. The exact meaning of "closed" and
//! "open" depend on the latch in question.
//!
//! Most latches implement one or both of the two core traits: [`Latch`] and
//! [`Probe`].
//!
//! [`Latch`] represents the "write-side" of the api, which allows consumers to
//! open arbitrary latches (and thus unblock whatever logic is blocked using the
//! latch). It defines a single function, [`Latch::set`], which (possibly)
//! opens the latch.
//!
//! [`Probe`] represents the "read-side" of the api, which allows consumers to
//! test if is latch is closed and spin (or do something else) while waiting for
//! it to open. It defines a single method, [`Probe::probe`], which returns
//! a boolean to indicate if the latch is open.
//!
//! # Safety
//!
//! Latches are an inherently somewhat unsafe construct, because once a latch is
//! becomes "open", the logic it unblocks often deallocates the lock. Refer to
//! specific safety comments for more information.

use std::{
    borrow::Cow,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
    task::Wake,
};

use parking_lot::{Condvar, Mutex};

use crate::thread_pool::{Registry, WorkerThread};

// -----------------------------------------------------------------------------
// Latches and probes

/// This trait represents the "write-side" of the latch api. It exists to allow
/// consumers to open arbitrary latches (and thus unblock whatever logic is
/// blocked using the latch).
///
/// Latches may choose not implement this trait if they cannot be opened without
/// additional context.
pub trait Latch {
    /// Possibly opens the latch. Calling this method does not have to open the
    /// latch, but there should be some situation in which it does.
    ///
    /// # Safety
    ///
    /// Opening a latch triggers other threads to wake up and (in some cases)
    /// complete. This may, in turn, cause memory to be deallocated and so
    /// forth.
    ///
    /// This function operates on `*const Self` instead of `&self` to allow it
    /// to become dangling during this call. The caller must ensure that the
    /// pointer is valid upon entry, and not invalidated during the call by any
    /// actions other than `set` itself.
    ///
    /// The implementer can assume the pointer is valid when passed into the
    /// function, but must not assume the pointer is valid after the latch is
    /// "opened" (eg. after whatever side-effect can cause logic elsewhere to
    /// progress). It's typically better to read all the fields you will need to
    /// access *before* a latch is set!
    unsafe fn set(this: *const Self);
}

/// This trait represents the "read-side" of the latch api. It exists to allow
/// consumers to check is a latch is open.
///
/// Latches may choose not to implement this if they do not support polling (for
/// example if they use a mutex to block a thread).
pub trait Probe {
    /// Returns `true` if the latch is open, and `false` if it is closed.
    fn probe(&self) -> bool;
}

// -----------------------------------------------------------------------------
// Atomic latch

/// A simple latch implemented using an atomic bool.
pub struct AtomicLatch {
    /// The state of the latch, `true` for open and `false` for closed.
    state: AtomicBool,
}

impl AtomicLatch {
    /// Creates a new closed latch.
    #[inline]
    pub fn new() -> Self {
        Self {
            state: AtomicBool::new(false),
        }
    }

    /// Resets the latch back to closed.
    #[inline]
    pub fn reset(&self) {
        self.state.store(false, Ordering::Release);
    }
}

impl Default for AtomicLatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Latch for AtomicLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: We assume the pointer is valid when passed in. We do not use
        // it after the side-effects (in this case an atomic store) so it is
        // fine if the pointer becomes dangling.
        unsafe {
            (*this).state.store(true, Ordering::Release);
        }
    }
}

impl Probe for AtomicLatch {
    #[inline]
    fn probe(&self) -> bool {
        self.state.load(Ordering::Acquire)
    }
}

// -----------------------------------------------------------------------------
// Wake latch

/// A simple wrapper around an `AtomicLatch` that can wake a worker when set, if
/// provided with a reference to a registry.
///
/// This is preferred to `RegistryLatch` when stored alongside a reference to a
/// registry, or within the registry itself.
pub struct WakeLatch {
    /// An internal atomic latch.
    atomic_latch: AtomicLatch,
    /// The index of the worker thread to wake when `set()` is called.
    thread_index: usize,
}

impl WakeLatch {
    /// Creates a new closed latch.
    #[inline]
    pub fn new(thread_index: usize) -> WakeLatch {
        WakeLatch {
            atomic_latch: AtomicLatch::new(),
            thread_index,
        }
    }

    /// Opens the latch and uses the provided registry to wake a specific worker.
    ///
    /// # Safety
    ///
    /// The safety requirements of `Latch::set` apply to this function as well.
    /// The caller must ensure that the pointer is valid upon entry, and not
    /// invalidated during the call by any actions other than `set_and_wake`.
    #[inline]
    pub unsafe fn set_and_wake(this: *const Self, registry: &Registry) {
        // SAFETY: We assume the pointer is valid when passed in. We do not use
        // it after `Latch::set` so it is fine if the pointer becomes dangling.
        unsafe {
            let thread_index = (*this).thread_index;
            Latch::set(&(*this).atomic_latch);
            registry.wake_thread(thread_index);
        }
    }

    /// Resets the latch back to closed.
    #[inline]
    pub fn reset(&self) {
        self.atomic_latch.reset();
    }
}

impl Probe for WakeLatch {
    #[inline]
    fn probe(&self) -> bool {
        self.atomic_latch.probe()
    }
}

/// A trait for latches that implement probe and are also capable of waking a
/// sleeping worker thread.
///
/// This trait exists to prevent a thread from going to sleep waiting on a latch
/// that can't wake it back up again.
pub trait WakeProbe: Probe {}

impl WakeProbe for WakeLatch {}

// -----------------------------------------------------------------------------
// Registry latch

/// A wrapper around a `WakeLatch` which embeds a reference to the registry.
pub struct RegistryLatch<'r> {
    /// The internal waker latch.
    wake_latch: WakeLatch,
    /// A reference to the registry, which can be either "strong" (an
    /// `Arc<Registry>`) or "weak" (an `&'r Arc<Registry>`).
    registry: Cow<'r, Arc<Registry>>,
}

impl<'r> RegistryLatch<'r> {
    /// Creates a new registry latch with a weak reference to the registry. This
    /// latch is only valid for the lifetime of the reference to the worker
    /// thread. It relies on the worker thread to keep the registry alive.
    #[inline]
    pub fn new(thread: &'r WorkerThread) -> RegistryLatch<'r> {
        RegistryLatch {
            wake_latch: WakeLatch::new(thread.index()),
            registry: Cow::Borrowed(thread.registry()),
        }
    }

    /// Creates a new registry latch with a strong reference to the registry,
    /// ensuring that the registry will remain alive so long as the latch is held.
    ///
    /// This is useful for latches that need to outlive the stack-frame in which
    /// they were created.
    #[inline]
    pub fn new_static(thread: &'r WorkerThread) -> RegistryLatch<'static> {
        RegistryLatch {
            wake_latch: WakeLatch::new(thread.index()),
            registry: Cow::Owned(Arc::clone(thread.registry())),
        }
    }

    /// Resets the latch back to closed.
    #[inline]
    pub fn reset(&self) {
        self.wake_latch.reset();
    }
}

impl<'r> Latch for RegistryLatch<'r> {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: We assume the pointer is valid when passed in.
        //
        // It's possible that the pointer will become dangling during the call
        // to `set_and_wake`. If that happens, then our local `registry` field
        // will also be dropped, and if this contains a `Cow::Owned` value and
        // this is the last reference to the registry, the whole registry may be
        // dropped as well. This would invalidate the reference to the registry.
        //
        // To prevent that from happening, we clone the `Cow` before using it.
        // When owned, this has the effect of cloning the arc, ensuring that the
        // registry will not be dropped. When borrowed, this is as cheap as an
        // additional borrow, and we know the registry cannot be dropped because
        // of the lifetime requirement.
        //
        // Therefore the reference `&registry` must be valid until the end of
        // this block.
        unsafe {
            let registry = Cow::clone(&(*this).registry);
            WakeLatch::set_and_wake(&(*this).wake_latch, &registry);
        }
    }
}

impl<'r> Probe for RegistryLatch<'r> {
    #[inline]
    fn probe(&self) -> bool {
        self.wake_latch.probe()
    }
}

impl<'r> WakeProbe for RegistryLatch<'r> {}

// -----------------------------------------------------------------------------
// Mutex-lock latch

/// A latch that can be used to block a thread, implemented using a mutex.
pub struct LockLatch {
    mutex: Mutex<bool>,
    cond: Condvar,
}

impl LockLatch {
    /// Creates a new closed latch.
    #[inline]
    pub fn new() -> LockLatch {
        LockLatch {
            mutex: Mutex::new(false),
            cond: Condvar::new(),
        }
    }

    /// Waits for the latch to open by blocking the thread.
    pub fn wait(&self) {
        let mut guard = self.mutex.lock();
        while !*guard {
            self.cond.wait(&mut guard);
        }
    }

    /// Waits for the latch to open by blocking the thread, then sets it back to closed.
    pub fn wait_and_reset(&self) {
        let mut guard = self.mutex.lock();
        while !*guard {
            self.cond.wait(&mut guard);
        }
        *guard = false;
    }
}

impl Default for LockLatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Latch for LockLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: We assume the pointer is valid when passed in. Side-effects
        // are not transmitted until the `notify_all` call at the very end, so
        // the pointer remains valid for the entire block.
        unsafe {
            let mut guard = (*this).mutex.lock();
            *guard = true;
            (*this).cond.notify_all();
        }
    }
}

// -----------------------------------------------------------------------------
// Counting latch

/// A counting latch stores a decrementing counter and only opens when the
/// counter reaches zero. This means that, unlike other latches, multiple calls
/// to `Latch::set` may be required to open the latch.
///
/// Counting latches wrap a static registry latch, and like registry latches can
/// be used to wake worker threads.
pub struct CountLatch {
    counter: AtomicUsize,
    latch: RegistryLatch<'static>,
}

impl CountLatch {
    /// Creates a new closed latch with the specified count.
    #[inline]
    pub fn with_count(count: usize, owner: &WorkerThread) -> Self {
        let latch = RegistryLatch::new_static(owner);

        Self {
            counter: AtomicUsize::new(count),
            latch,
        }
    }

    /// Increments the count. An additional call to `Latch::set()` will be
    /// required before the latch opens.
    #[inline]
    pub fn increment(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }
}

impl Latch for CountLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: We assume the pointer is valid when passed in. Side-effects
        // are not transmitted until the `Latch::set` call at the very end, so
        // it is fine if the pointer becomes dangling.
        unsafe {
            if (*this).counter.fetch_sub(1, Ordering::SeqCst) == 1 {
                Latch::set(&(*this).latch)
            }
        }
    }
}

impl Probe for CountLatch {
    #[inline]
    fn probe(&self) -> bool {
        self.latch.probe()
    }
}

impl WakeProbe for CountLatch {}

// -----------------------------------------------------------------------------
// Async set-on-wake

// An async task waker that sets a latch on wake.
pub struct SetOnWake<L>
where
    L: Latch,
{
    latch: L,
}

impl<L> SetOnWake<L>
where
    L: Latch,
{
    /// Creates a waker from a latch.
    pub fn new(latch: L) -> Arc<Self> {
        Arc::new(Self { latch })
    }

    /// Returns a reference to the inner latch.
    pub fn latch(&self) -> &L {
        &self.latch
    }
}

impl<L> Wake for SetOnWake<L>
where
    L: Latch,
{
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // SAFETY: The pointer passed to `Latch::set()` is valid and cannot be
        // invalidated while the arc is held.
        unsafe { Latch::set(&self.latch) };
    }
}
