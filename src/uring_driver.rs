use io_uring::{cqueue, IoUring};
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use io_uring::squeue::Entry;
use slab::Slab;

thread_local! {
    pub(crate) static URING_DRIVER: RefCell<UringDriver> = RefCell::new(UringDriver::new());
}

// pub(crate) static URING_DRIVER: Lazy<Arc<UringDriver>> = Lazy::new(|| Arc::new(UringDriver::new()));

pub struct UringDriver {
    pub(crate) ring: IoUring,
    pub(crate) life_cycle: Slab<LifeCycle>,
}

impl UringDriver {
    fn new() -> Self {
        todo!()
    }
    
    pub(crate) fn generate_id(&mut self) -> u64 {
        self.life_cycle.insert(LifeCycle::Submitted) as u64
    }
    
    pub(crate) fn push_sqe(&mut self, entry: &Entry) {
        while unsafe { URING_DRIVER.with_borrow_mut(|driver| driver.ring.submission().push(entry)).is_err() } {
            // If the submission queue is full, flush it to the kernel
            URING_DRIVER.with_borrow_mut(|driver| driver.submit().unwrap()); // todo: remove unwrap
        }
    }

    pub(crate) fn submit(&mut self) -> io::Result<()> {
        loop {
            match self.ring.submit() {
                Ok(_) => {
                    self.ring.submission().sync();
                    return Ok(());
                }
                Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {
                    // sq is full
                    self.dispatch_completions();
                }
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => {
                    // interrupted by signals
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn dispatch_completions(&mut self) {
        let mut completion_queue = self.ring.completion();
        
        // `sync` here claims all the cqe in `completion_queue`, and sync the state of the kernel cq
        // so the following iteration consume the entries
        completion_queue.sync();
        
        for cqe in completion_queue {
            let id = cqe.user_data() as usize;
            let lifecycle = self.life_cycle.get_mut(id).unwrap();
            
            if let Some(new_lifecycle) = std::mem::take(lifecycle).complete(cqe) {
                let _ = std::mem::replace(lifecycle, new_lifecycle);
            } else {
                self.life_cycle.remove(id);
            }
        }
    }
}

pub struct Op {
    buf: Vec<u8>,
    id: u64,
}

impl Op {
    pub fn new(buf: Vec<u8>, id: u64) -> Self {
        Self {buf, id}
    }
}

impl Future for Op {
    type Output = Vec<u8>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

impl Drop for Op {
    fn drop(&mut self) {
        todo!()
    }
}

// ignore the `IORING_CQE_F_MORE` flag
enum LifeCycle {
    Submitted, // after future is constructed
    Waiting(Waker), // after first polled
    Ignored(Box<dyn std::any::Any>), // after future dropped
    Completed(cqueue::Entry),
}

impl Default for LifeCycle {
    fn default() -> Self {
        LifeCycle::Submitted
    }
}

impl LifeCycle {
    fn complete(self, cqe: cqueue::Entry) -> Option<Self> {
        use LifeCycle::*;
        let state = match self {
            Submitted => Some(Completed(cqe)),
            Waiting(waker) => {
                waker.wake();
                Some(Completed(cqe))
            }
            Ignored(_) => None,
            Completed(_) => unreachable!("invalid operation state"), // todo: remove this
        };
        state
    }
}
