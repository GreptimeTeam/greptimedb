use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::Data;
use timely::communication::Push;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Capability, InputCapability};
use timely::progress::Timestamp;

/// A buffer that consolidates updates
///
/// The buffer implements a wrapper around [OutputHandle] consolidating elements pushed to it. It is
/// backed by a capacity-limited buffer, which means that compaction only occurs within the
/// dimensions of the buffer, i.e. the number of unique keys is less than half of the buffer's
/// capacity.
///
/// A cap is retained whenever the current time changes to be able to flush on drop or when the time
/// changes again.
///
/// The buffer is filled with updates until it reaches its capacity. At this point, the updates are
/// consolidated to free up space. This process repeats until the consolidation recovered less than
/// half of the buffer's capacity, at which point the buffer will be shipped.
///
/// The buffer retains a capability to send data on flush. It will flush all data once dropped, if
/// time changes, or if the buffer capacity is reached.
pub struct ConsolidateBuffer<'a, 'b, T, D: Data, R: Semigroup, P>
where
    P: Push<Bundle<T, (D, T, R)>> + 'a,
    T: Data + Timestamp + 'a,
    D: 'a,
{
    // a buffer for records, to send at self.cap
    // Invariant: Buffer only contains data if cap is Some.
    buffer: Vec<(D, T, R)>,
    output_handle: &'b mut OutputHandle<'a, T, (D, T, R), P>,
    cap: Option<Capability<T>>,
    port: usize,
    previous_len: usize,
}

impl<'a, 'b, T, D: Data, R: Semigroup, P> ConsolidateBuffer<'a, 'b, T, D, R, P>
where
    T: Data + Timestamp + 'a,
    P: Push<Bundle<T, (D, T, R)>> + 'a,
{
    /// Create a new [ConsolidateBuffer], wrapping the provided session.
    ///
    /// * `output_handle`: The output to send data to.
    /// * 'port': The output port to retain capabilities for.
    pub fn new(output_handle: &'b mut OutputHandle<'a, T, (D, T, R), P>, port: usize) -> Self {
        Self {
            output_handle,
            port,
            cap: None,
            buffer: Vec::with_capacity(::timely::container::buffer::default_capacity::<(D, T, R)>()),
            previous_len: 0,
        }
    }

    #[inline]
    /// Provides an iterator of elements to the buffer
    pub fn give_iterator<I: Iterator<Item = (D, T, R)>>(
        &mut self,
        cap: &InputCapability<T>,
        iter: I,
    ) {
        for item in iter {
            self.give(cap, item);
        }
    }

    /// Give an element to the buffer
    pub fn give(&mut self, cap: &InputCapability<T>, data: (D, T, R)) {
        // Retain a cap for the current time, which will be used on flush.
        if self.cap.as_ref().map_or(true, |t| t.time() != cap.time()) {
            // Flush on capability change
            self.flush();
            // Retain capability for the specified output port.
            self.cap = Some(cap.delayed_for_output(cap.time(), self.port));
        }
        self.give_internal(data);
    }

    /// Give an element to the buffer, using a pre-fabricated capability. Note that the capability
    /// must be valid for the associated output.
    pub fn give_at(&mut self, cap: &Capability<T>, data: (D, T, R)) {
        // Retain a cap for the current time, which will be used on flush.
        if self.cap.as_ref().map_or(true, |t| t.time() != cap.time()) {
            // Flush on capability change
            self.flush();
            // Retain capability.
            self.cap = Some(cap.clone());
        }
        self.give_internal(data);
    }

    /// Give an element and possibly flush the buffer. Note that this needs to have access
    /// to a capability, which the public functions ensure.
    fn give_internal(&mut self, data: (D, T, R)) {
        self.buffer.push(data);

        // Limit, if possible, the lifetime of the allocations for data
        // and consolidate smaller buffers if we're in the lucky case
        // of a small domain for D
        if self.buffer.len() >= 2 * self.previous_len {
            // Consolidate while the consolidation frees at least half the buffer
            consolidate_updates(&mut self.buffer);
            if self.buffer.len() > self.buffer.capacity() / 2 {
                self.flush();
            } else {
                self.previous_len = self.buffer.len();
            }
            // At this point, it is an invariant across give calls that self.previous_len
            // will be in the interval [0, self.buffer.capacity() / 2]. So, we will enter
            // this if-statement block again when self.buffer.len() == self.buffer.capacity()
            // or earlier. If consolidation is not effective to keep self.buffer.len()
            // below half capacity, then flushing when more than half-full will
            // maintain the invariant.
        }
    }

    /// Flush the internal buffer to the underlying session
    pub fn flush(&mut self) {
        if let Some(cap) = &self.cap {
            self.output_handle.session(cap).give_vec(&mut self.buffer);

            // Ensure that the capacity is at least equal to the default in case
            // it was reduced by give_vec. Note that we cannot rely here on give_vec
            // returning us a buffer with zero capacity.
            if self.buffer.capacity() < ::timely::container::buffer::default_capacity::<(D, T, R)>()
            {
                let to_reserve = ::timely::container::buffer::default_capacity::<(D, T, R)>()
                    - self.buffer.capacity();
                self.buffer.reserve_exact(to_reserve);
            }
            self.previous_len = 0;
        }
    }
}

impl<'a, 'b, T, D: Data, R: Semigroup, P> Drop for ConsolidateBuffer<'a, 'b, T, D, R, P>
where
    P: Push<Bundle<T, (D, T, R)>> + 'a,
    T: Data + Timestamp + 'a,
    D: 'a,
{
    fn drop(&mut self) {
        self.flush();
    }
}
