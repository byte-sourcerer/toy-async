use std::ffi::CString;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::uring_driver::{BufResult, Op, UringDriver, URING_DRIVER};
use io_uring::opcode;
use io_uring::types;

pub struct File {
    file: std::fs::File,
}

impl File {
    pub fn new(file: std::fs::File) -> Self {
        Self { file }
    }

    pub async fn read_at(&self, mut buffer: Vec<u8>, pos: u64) -> BufResult {
        let ptr = buffer.as_mut_ptr();
        let len = buffer.capacity();
        let id = URING_DRIVER.with_borrow_mut(|driver| driver.generate_id());

        let sqe = opcode::Read::new(types::Fd(self.file.as_raw_fd()), ptr, len as _)
            .offset(pos as _)
            .build()
            .user_data(id);

        let op = Op::new(buffer, id);

        URING_DRIVER.with_borrow_mut(|driver| driver.push_sqe(&sqe));

        op.await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;
    use crate::file::File;

    #[test]
    fn test_read_at() {
        let file = tempfile().unwrap();
        let file = File::new(file);
        let buf = vec![0; 1024];
        let res = file.read_at(buf, 0);
    }
}