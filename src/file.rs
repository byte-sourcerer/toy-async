use std::ffi::CString;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::uring_driver::{Op, UringDriver, URING_DRIVER};
use io_uring::opcode;
use io_uring::types;

pub struct File {
    file: std::fs::File,
}

impl File {
    // pub async fn open(
    //     options: std::fs::OpenOptions,
    //     path: impl AsRef<Path>,
    // ) -> Result<Self, io::Error> {
    //     let path = {
    //         let path = path.as_ref().as_os_str().as_bytes();
    //         CString::new(path)?.as_c_str().as_ptr()
    //     };
    //     let flags = libc::O_WRONLY | libc::O_CREAT;
    //     let mode = 0o666;
    //     let op = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path)
    //         .flags(flags)
    //         .mode(mode)
    //         .build();
    //     todo!()
    // }

    pub async fn read_at(&self, mut buffer: Vec<u8>, pos: u64) -> Vec<u8> {
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
