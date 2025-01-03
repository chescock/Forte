use std::sync::atomic::{AtomicPtr, Ordering};

/// A `Box<T>` that can be swapped atomically, along with
/// a `bool` tag that identifies the value as 'is_empty' or not.
pub struct TaggedAtomicBox<T>(
    /// The lowest bit of the inner pointer is a tag that indicates whether `is_empty` was passed.
    /// The remaining bits are a valid pointer obtained from `Box::into_raw`.
    AtomicPtr<T>,
);

impl<T> TaggedAtomicBox<T> {
    /// Constructs a new `TaggedAtomicBox` given the initial value and initial `is_empty` flag.
    pub fn new(value: Box<T>, is_empty: bool) -> Self {
        if const { align_of::<T>() & 1 != 0 } {
            panic!("TaggedAtomicBox may only be used on types with alignment of at least 2");
        }
        Self(AtomicPtr::new(Self::to_tagged_ptr(value, is_empty)))
    }

    /// Packs a `Box` and a tag into a single pointer.
    /// This value should later be recovered using `from_tagged_ptr` to avoid leaking the box.
    fn to_tagged_ptr(value: Box<T>, is_empty: bool) -> *mut T {
        (Box::into_raw(value) as usize | is_empty as usize) as *mut T
    }

    /// Converts a tagged pointer back to the original box.
    ///
    /// # Safety
    ///
    /// The pointer must have been obtained from `to_tagged_ptr()`,
    /// and must not be passed to `from_tagged_ptr()` again.
    unsafe fn from_tagged_ptr(ptr: *mut T) -> Box<T> {
        unsafe { Box::from_raw((ptr as usize & !1) as *mut T) }
    }

    /// Whether the last update to this box was made with the `is_empty` flag set.
    /// Remember that the value may be changed by other threads,
    /// so the flag may have changed if `swap` is called later.
    pub fn is_empty(&self) -> bool {
        // Use `Relaxed` ordering as we are only reading the tag and not
        // reading any other data through the pointer.
        (self.0.load(Ordering::Relaxed) as usize & 1) != 0
    }

    /// Replaces the contained value and sets the `is_empty` flag, returning the previous value.
    pub fn swap(&self, value: Box<T>, is_empty: bool) -> Box<T> {
        let new_ptr = Self::to_tagged_ptr(value, is_empty);
        // Use `AcqRel` to ensure that all data behind the pointer is synchronized.
        // A given box is stored with release on one thread and loaded with acquire
        // on another thread, which ensures that all writes to the box before the store
        // are ordered before all reads after the load.
        let old_ptr = self.0.swap(new_ptr, Ordering::AcqRel);
        // SAFETY: The value stored in the `AtomicPtr` is always obtained from
        // `to_tagged_ptr`, and the `swap` call ensures it is not read again.
        unsafe { Self::from_tagged_ptr(old_ptr) }
    }
}

impl<T: Default> Default for TaggedAtomicBox<T> {
    fn default() -> Self {
        Self::new(Box::default(), true)
    }
}

impl<T> Drop for TaggedAtomicBox<T> {
    fn drop(&mut self) {
        // SAFETY: The value stored in the `AtomicPtr` is always obtained from
        // `to_tagged_ptr`, we are dropping the value so it is not read again.
        drop(unsafe { Self::from_tagged_ptr(*self.0.get_mut()) });
    }
}
