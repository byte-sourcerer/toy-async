use std::sync::Arc;

use io_uring::IoUring;
use once_cell::sync::Lazy;

static URING_DRIVER: Lazy<Arc<UringDriver>> = Lazy::new(|| Arc::new(UringDriver::new()));

pub struct UringDriver {
    ring: IoUring,
}

impl UringDriver {
    fn new() -> Self {
        todo!()
    }
}
