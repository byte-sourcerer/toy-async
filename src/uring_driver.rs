use io_uring::squeue::Entry;
use io_uring::{cqueue, IoUring};
use slab::Slab;
use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

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
        while unsafe {
            URING_DRIVER
                .with_borrow_mut(|driver| driver.ring.submission().push(entry))
                .is_err()
        } {
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
            map_slab_or_remove(&mut self.life_cycle, id, |lifecycle| {
                lifecycle.complete(cqe)
            });
        }
    }
}

fn map_slab_or_remove<T: Default>(slab: &mut Slab<T>, id: usize, f: impl FnOnce(T) -> Option<T>) {
    let element = slab.get_mut(id).unwrap();
    if let Some(new_element) = f(std::mem::take(element)) {
        let _ = std::mem::replace(element, new_element);
    } else {
        slab.remove(id);
    }
}

// todo: Op as Future can be moved to other threads?
// so we need to save a reference to local driver?
pub struct Op {
    buf: Option<Vec<u8>>,
    id: u64,
}

impl Op {
    pub fn new(buf: Vec<u8>, id: u64) -> Self {
        Self { buf: Some(buf), id }
    }
}

impl Op {
    fn cqe_to_result(&mut self, cqe: cqueue::Entry) -> (io::Result<usize>, Vec<u8>) {
        let res: CqeResult = cqe.into();
        let res = res.result.map(|n| n as usize);
        let mut buf = self.buf.take().unwrap();

        // If the operation was successful, advance the initialized cursor.
        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                if buf.len() < n {
                    buf.set_len(n);
                }
            }
        }

        (res, buf)
    }
}

impl Future for Op {
    type Output = (io::Result<usize>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        URING_DRIVER.with_borrow_mut(|driver| {
            let id = self.id as usize;
            let lifecycle_slot = driver.life_cycle.get_mut(id).unwrap();
            match std::mem::take(lifecycle_slot).poll(cx.waker()) {
                PollState::Pending(new_lifecycle) => {
                    let _ = std::mem::replace(lifecycle_slot, new_lifecycle);
                    Poll::Pending
                }
                PollState::Ready(cqe) => Poll::Ready(self.get_mut().cqe_to_result(cqe)),
            }
        })
    }
}

impl Drop for Op {
    fn drop(&mut self) {
        let id = self.id as usize;
        URING_DRIVER.with_borrow_mut(|driver| {
            map_slab_or_remove(&mut driver.life_cycle, id, |lifecycle| {
                lifecycle.ignore(self)
            })
        });
    }
}

// ignore the `IORING_CQE_F_MORE` flag
enum LifeCycle {
    Submitted,                       // after future is constructed
    Waiting(Waker),                  // after first polled
    Ignored(Box<dyn std::any::Any>), // after future dropped
    Completed(cqueue::Entry),
}

impl Default for LifeCycle {
    fn default() -> Self {
        LifeCycle::Submitted
    }
}

impl LifeCycle {
    fn poll(self, waker: &Waker) -> PollState<cqueue::Entry> {
        use LifeCycle::*;
        use PollState::*;

        match self {
            Submitted => Pending(Waiting(waker.clone())),
            Waiting(old_waker) => {
                if old_waker.will_wake(waker) {
                    Pending(Waiting(old_waker))
                } else {
                    Pending(Waiting(waker.clone()))
                }
            }
            Ignored(_) => unreachable!("invalid operation state"), // todo: remove it
            Completed(cqe) => Ready(cqe),
        }
    }

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

    fn ignore(self, op: &mut Op) -> Option<Self> {
        use LifeCycle::*;
        match self {
            Submitted | Waiting(_) => {
                let data = op.buf.take();
                Some(Ignored(Box::new(data)))
            }
            Ignored(_) => unreachable!(),
            Completed(_) => None,
        }
    }
}

enum PollState<T> {
    Pending(LifeCycle),
    Ready(T),
}

struct CqeResult {
    pub(crate) result: io::Result<u32>,
    pub(crate) flags: u32,
}

impl From<cqueue::Entry> for CqeResult {
    fn from(cqe: cqueue::Entry) -> Self {
        let res = cqe.result();
        let flags = cqe.flags();
        let result = if res >= 0 {
            Ok(res as u32)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        };
        CqeResult { result, flags }
    }
}
