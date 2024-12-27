// -----------------------------------------------------------------------------
// Utilities

// A guard that calls the specified closure when it is dropped. This is used
// internally to run logic when a `Future` is canceled or completed.
pub struct CallOnDrop<F: FnMut()>(pub F);

impl<F: FnMut()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
