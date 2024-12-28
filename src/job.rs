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

use std::cell::UnsafeCell;

// -----------------------------------------------------------------------------
// Job

/// A job is a unit of work that may be executed by a worker thread.
pub trait Job {
    /// Calling this function runs the job.
    ///
    /// # Safety
    ///
    /// This may be called from a different thread than the one which scheduled
    /// the job, so the implementer must ensure the appropriate traits are met,
    /// whether `Send`, `Sync`, or both.
    unsafe fn execute(this: *const ());
}

// -----------------------------------------------------------------------------
// JobRef

/// Effectively a Job trait object. It can be treated as such, even though
/// sometimes a `JobRef` will not point to a type that implements `Job`.
pub struct JobRef {
    /// A raw pointer to data that can be executed with the `execute_fn`. This
    /// will usually point to either a `StackJob` or a `HeapJob`.
    pointer: *const (),
    /// A function pointer that can execute the job stored at `pointer`.
    execute_fn: unsafe fn(*const ()),
}

impl JobRef {
    /// Creates a new `JobRef` from a `Job`.
    ///
    /// # Safety
    ///
    /// Caller must ensure `job` will remain valid until the job is executed,
    /// and that the job is executed to completion exactly once.
    pub unsafe fn new<J>(job: *const J) -> JobRef
    where
        J: Job,
    {
        JobRef {
            pointer: job as *const (),
            execute_fn: <J as Job>::execute,
        }
    }

    /// Creates a new `JobRef` from raw pointers.
    ///
    /// # Safety
    ///
    /// Caller must ensure the data at the pointer will remain valid until the
    /// job is executed, and that the job is executed to completion exactly
    /// once. Additionally the caller must ensure that `execute_fn` can be
    /// called on `pointer`.
    pub unsafe fn new_raw(pointer: *const (), execute_fn: unsafe fn(*const ())) -> JobRef {
        JobRef {
            pointer,
            execute_fn,
        }
    }

    /// Returns an opaque handle that can be saved and compared, without making
    /// `JobRef` itself `Copy + Eq`.
    #[inline]
    pub fn id(&self) -> impl Eq {
        (self.pointer, self.execute_fn)
    }

    /// Executes a `JobRef`.
    ///
    /// # Safety
    ///
    /// Caller must ensure that `JobRef::pointer` is still valid.
    #[inline]
    pub unsafe fn execute(self) {
        (self.execute_fn)(self.pointer)
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
    job: UnsafeCell<Option<F>>,
}

impl<F> StackJob<F>
where
    F: FnOnce() + Send,
{
    /// Creates a new `StackJob` and returns it directly.
    pub fn new(job: F) -> StackJob<F> {
        StackJob {
            job: UnsafeCell::new(Some(job)),
        }
    }

    /// Executes the job without having to go through the `JobRef`. This has the
    /// benifit of saving some dynamic lookups, and allows the compiler to do
    /// inline optimization (because the function type is known).
    ///
    /// This is used in `join` to run the job syncrhonously after failing to
    /// share it.
    pub fn run_inline(self) {
        let job = self.job.into_inner().unwrap();
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
        JobRef::new(self)
    }
}

impl<F> Job for StackJob<F>
where
    F: FnOnce() + Send,
{
    /// Executes a `StackJob` from a const pointer.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the pointer points to a valid `StackJob`; or,
    /// equivalently, that this is called before the stack frame in which the
    /// job is allocated is popped.
    unsafe fn execute(this: *const ()) {
        let this = &*(this as *const Self);
        let job = (*this.job.get()).take().unwrap();
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
    job: F,
}

impl<F> HeapJob<F>
where
    F: FnOnce() + Send,
{
    /// Allocates a new `HeapJob` on the heap.
    pub fn new(job: F) -> Box<HeapJob<F>> {
        Box::new(HeapJob { job })
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
        JobRef::new(Box::into_raw(self))
    }
}

impl<F> Job for HeapJob<F>
where
    F: FnOnce() + Send,
{
    /// Executes a `HeapJob` from a const pointer.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the pointer points to a valid `HeapJob`.
    unsafe fn execute(this: *const ()) {
        let this = Box::from_raw(this as *mut Self);
        (this.job)();
    }
}
