//! This module defines a executable unit of work called a `Job`. Jobs are what
//! get scheduled on the thread-pool. After the are allocated, a reference
//! (specifically a `JobRef`) is queued, passed to a thread, end executed.
//!
//! This module defines two core job types: `StackJob` and `HeapJob`. The former
//! is more efficent, but can only be used when the work won't outlive the
//! current stack. `HeapJob` requires an allocation, but can outlive the current
//! stack.
//!
//! When using a job, one must be extreamly careful to ensure that:
//! (a) The job does not outlive anything it closes over.
//! (b) The job remains valid until it is executed for the last time.
//! (c) Each job reference is executed exactly once.

use std::{cell::UnsafeCell, mem::offset_of, ptr::NonNull};

use async_task::Runnable;

// -----------------------------------------------------------------------------
// Job

/// A job is a unit of work that may be executed by a worker thread.
/// This struct should be embedded in a structure that acts as a job
/// so that we can pass around a pointer to a common type.
// Ensure that the struct is sufficiently aligned that we can use the bottom bit as a tag.
// This will generally not have any effect, as function pointers will already be more aligned than this.
#[repr(align(2))]
pub struct Job {
    /// Calling this function runs the job.
    execute_fn: unsafe fn(*const Job),
}

impl Job {
    /// Creates a `Job` structure.
    ///
    /// # Safety
    ///
    /// This may be called from a different thread than the one which scheduled
    /// the job, so the caller must ensure the appropriate traits are met,
    /// whether `Send`, `Sync`, or both.
    unsafe fn new(execute_fn: unsafe fn(*const Job)) -> Self {
        Self { execute_fn }
    }
}

// -----------------------------------------------------------------------------
// JobRef

/// A pointer-sized value that represents a job that can be executed.
pub struct JobRef {
    /// A tagged pointer.
    /// If the bottom bit is clear, this is a pointer to a `Job` value embedded in a larger structure.
    /// If the bottom bit is set, this is actually a pointer to a `Runnable` value.
    pointer: *const Job,
}

impl JobRef {
    /// Creates a new `JobRef` from a `Job`.
    ///
    /// # Safety
    ///
    /// Caller must ensure `job` will remain valid until the job is executed,
    /// and that the job is executed to completion exactly once.
    pub unsafe fn from_job(pointer: *const Job) -> Self {
        JobRef { pointer }
    }

    pub fn from_runnable(runnable: Runnable) -> Self {
        let ptr = runnable.into_raw().as_ptr() as usize;
        debug_assert!(
            (ptr & 1) == 0,
            "Runnables must be sufficiently aligned to store a tag bit"
        );
        JobRef {
            pointer: (ptr | 1) as *const Job,
        }
    }

    /// Returns an opaque handle that can be saved and compared, without making
    /// `JobRef` itself `Copy + Eq`.
    #[inline]
    pub fn id(&self) -> impl Eq {
        self.pointer
    }

    /// Executes a `JobRef`.
    ///
    /// # Safety
    ///
    /// Caller must ensure that `JobRef::pointer` is still valid.
    #[inline]
    pub unsafe fn execute(self) {
        if self.pointer as usize & 1 == 0 {
            // The bottom bit was clear, so this was created by `from_job`
            // and points to a `Job` that may be executed.
            let execute_fn = (*self.pointer).execute_fn;
            execute_fn(self.pointer);
        } else {
            // The bottom bit was set, so this was created by `from_runnable`.
            // Clear the bit to recover the pointer returned by `Runnable::into_raw()`.
            // SAFETY: We provide a pointer to a non-null runnable, and we turn
            // it back into a non-null runnable. The runnable will remain valid
            // until the task is run.
            let ptr = NonNull::new_unchecked((self.pointer as usize & !1) as *mut ());
            let runnable = Runnable::<()>::from_raw(ptr);
            runnable.run();
        }
    }
}

// SAFETY: !Send for raw pointers is not for safety, just as a lint
unsafe impl Send for JobRef {}

// SAFETY: !Sync for raw pointers is not for safety, just as a lint
unsafe impl Sync for JobRef {}

// -----------------------------------------------------------------------------
// Stack allocated job

/// A job that will be owned by a stack slot. This means that when it executes
/// it need not free any heap data, the cleanup occurs when the stack frame is
/// later popped.
///
/// This is, from an allocation and freeing point of view, the most efficient
/// type of job. It is used to implement `join` and `on_worker`.
pub struct StackJob<F>
where
    F: FnOnce() + Send,
{
    job: Job,
    f: UnsafeCell<Option<F>>,
}

impl<F> StackJob<F>
where
    F: FnOnce() + Send,
{
    /// Creates a new `StackJob` and returns it directly.
    pub fn new(f: F) -> StackJob<F> {
        StackJob {
            job: unsafe { Job::new(Self::execute) },
            f: UnsafeCell::new(Some(f)),
        }
    }

    /// Executes the job without having to go through the `JobRef`. This has the
    /// benifit of saving some dynamic lookups, and allows the compiler to do
    /// inline optimization (because the function type is known).
    ///
    /// This is used in `join` to run the job syncrhonously after failing to
    /// share it.
    pub fn run_inline(self) {
        let job = self.f.into_inner().unwrap();
        job();
    }

    /// Creates a `JobRef` pointing to this job.
    ///
    /// # Safety
    ///
    /// Caller must ensure the `StackJob` remains valid until the `JobRef` is
    /// executed. This amounts to ensuring the job is executed before the stack
    /// frame is popped.
    pub unsafe fn as_job_ref(&self) -> JobRef {
        JobRef::from_job(&self.job)
    }

    /// Executes a `StackJob` from a const pointer.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the pointer points to the `Job` embedded in a valid `StackJob`; or,
    /// equivalently, that this is called before the stack frame in which the
    /// job is allocated is popped.
    unsafe fn execute(job: *const Job) {
        let this = &*(job
            .byte_offset(-(offset_of!(Self, job) as isize))
            .cast::<Self>());
        let job = (*this.f.get()).take().unwrap();
        job();
    }
}

// -----------------------------------------------------------------------------
// Heap allocated job

/// Represents a job stored in the heap. Used to implement `scope` and `spawn`.
pub struct HeapJob<F>
where
    F: FnOnce() + Send,
{
    job: Job,
    f: F,
}

impl<F> HeapJob<F>
where
    F: FnOnce() + Send,
{
    /// Allocates a new `HeapJob` on the heap.
    pub fn new(f: F) -> Box<HeapJob<F>> {
        let job = unsafe { Job::new(Self::execute) };
        Box::new(HeapJob { job, f })
    }

    /// A version of `into_job_ref` that is safe because of the static lifetime.
    pub fn into_static_job_ref(self: Box<Self>) -> JobRef
    where
        F: 'static,
    {
        // SAFETY: The closure this job points to has static lifetime, so it
        // will be valid until `JobRef` is executed, and it cannot close over
        // any non-static data.
        unsafe { self.into_job_ref() }
    }

    /// Creates a `JobRef` pointing to this job.
    ///
    /// # Safety
    ///
    /// Caller must ensure the `Box<HeapJob>` remains valid until the `JobRef`
    /// is executed. This hides all lifetimes, so the caller must ensure that it
    /// dosn't outlive any data it closes over.
    pub unsafe fn into_job_ref(self: Box<Self>) -> JobRef {
        JobRef::from_job(&raw const (*Box::into_raw(self)).job)
    }

    /// Executes a `HeapJob` from a const pointer.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the pointer points to the `Job` embedded in a valid `HeapJob`.
    unsafe fn execute(job: *const Job) {
        let this = job
            .byte_offset(-(offset_of!(Self, job) as isize))
            .cast::<Self>()
            .cast_mut();
        let this = Box::from_raw(this);
        (this.f)();
    }
}
