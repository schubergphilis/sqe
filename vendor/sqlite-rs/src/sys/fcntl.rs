// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Vendored `fcntl(F_SETLK`/`F_GETLK)` byte-range locking FFI (#563) —
//! replaces `nix::fcntl` — plus (macOS-only, #652) a plain `fsync(2)`
//! wrapper that bypasses `std::fs`'s Apple-only upgrade to
//! `fcntl(F_FULLFSYNC)`; see [`fsync`]'s doc comment. SQLite's
//! journal-mode lock ladder needs POSIX record locks; there is no
//! pure-Rust or `std` equivalent, so the `unsafe extern "C"` boundary
//! here is unavoidable, not merely convenient. `flock`'s field layout
//! and lock/cmd constants differ
//! between macOS and Linux (verified against each platform's own headers:
//! `<sys/fcntl.h>` on macOS, glibc's `bits/fcntl.h` on Linux) — the two
//! are kept in separate `cfg`-gated modules below rather than one
//! `#[cfg(...)]`-sprinkled definition, so each stays a faithful,
//! independently-checkable copy of its platform's ABI.
#![allow(unsafe_code)]
// Field/constant names below mirror the platform ABI (C struct field names,
// POSIX `fcntl.h` macro names) exactly, on purpose — inventing prose docs
// for `l_type`/`F_RDLCK`/etc. would just restate the name.
#![allow(missing_docs)]
// `FcntlArg`'s variants and the `flock`/`off_t` aliases mirror the C/POSIX
// names (`F_SETLK`, `struct flock`, `off_t`) exactly, matching `nix`'s own
// naming at the API this replaces.
#![allow(non_camel_case_types)]

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::raw::c_int;

#[cfg(target_os = "macos")]
mod abi {
    /// `<sys/_types/_off_t.h>`: `off_t` is a 64-bit signed type on every
    /// macOS ABI this crate targets.
    pub type OffT = i64;
    /// `<sys/_types/_pid_t.h>`.
    pub type PidT = i32;

    /// `<sys/fcntl.h>`'s `struct flock` — field order matches exactly.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Flock {
        pub l_start: OffT,
        pub l_len: OffT,
        pub l_pid: PidT,
        pub l_type: i16,
        pub l_whence: i16,
    }

    pub const F_GETLK: c_int = 7;
    pub const F_SETLK: c_int = 8;
    pub const F_SETLKW: c_int = 9;
    pub const F_RDLCK: i16 = 1;
    pub const F_UNLCK: i16 = 2;
    pub const F_WRLCK: i16 = 3;
    /// `<sys/errno.h>`.
    pub const EAGAIN: c_int = 35;
    pub const EACCES: c_int = 13;
    /// `<sys/fcntl.h>`.
    pub const O_NOFOLLOW: c_int = 0x0100;

    use std::os::raw::c_int;
}

#[cfg(target_os = "linux")]
mod abi {
    /// glibc x86_64/aarch64: `__off_t` is `long`, 64-bit on every Linux
    /// ABI this crate targets.
    pub type OffT = i64;
    /// glibc `pid_t`.
    pub type PidT = i32;

    /// glibc `bits/fcntl.h`'s `struct flock` — field order matches
    /// exactly (`l_type`/`l_whence` first, unlike macOS).
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Flock {
        pub l_type: i16,
        pub l_whence: i16,
        pub l_start: OffT,
        pub l_len: OffT,
        pub l_pid: PidT,
    }

    pub const F_GETLK: c_int = 5;
    pub const F_SETLK: c_int = 6;
    pub const F_SETLKW: c_int = 7;
    pub const F_RDLCK: i16 = 0;
    pub const F_UNLCK: i16 = 2;
    pub const F_WRLCK: i16 = 1;
    /// glibc `bits/errno.h` (generic Linux `asm-generic/errno-base.h`).
    pub const EAGAIN: c_int = 11;
    pub const EACCES: c_int = 13;
    /// glibc `bits/fcntl-linux.h`.
    pub const O_NOFOLLOW: c_int = 0o400000;

    use std::os::raw::c_int;
}

pub use abi::Flock as flock;
pub use abi::OffT as off_t;
pub use abi::{EACCES, EAGAIN, F_GETLK, F_RDLCK, F_SETLK, F_SETLKW, F_UNLCK, F_WRLCK, O_NOFOLLOW};

extern "C" {
    // `fcntl`'s third argument is `...` in its real C prototype — every
    // call site here passes exactly one `*mut flock`, so a variadic
    // `extern "C"` declaration (not a fixed `*mut flock` parameter) is
    // what actually matches the ABI the platform's `fcntl` symbol expects.
    fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
}

// macOS-only: `std::fs::File::sync_data`/`sync_all` upgrade to
// `fcntl(F_FULLFSYNC)` on Apple targets (a full flush past the drive's
// write cache) — see `fsync`'s doc comment below for why that's the wrong
// default for this crate to inherit. `#[link_name]` renames the raw
// extern symbol so the safe wrapper below can keep the same name POSIX
// and every other caller expects.
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "fsync"]
    fn raw_fsync(fd: c_int) -> c_int;
}

/// Plain POSIX `fsync(2)` — used in place of `std::fs::File::sync_data`/
/// `sync_all` on macOS. `std::fs`'s two sync methods both call
/// `fcntl(F_FULLFSYNC)` on Apple platforms rather than a bare `fsync()`,
/// trading a large latency cost (measured ~80x slower than plain `fsync`
/// on this crate's own dev hardware, #652) for a stronger guarantee (a
/// full flush past the drive's write cache) that real SQLite does not
/// enable by default either: `PRAGMA fullfsync` governs exactly this on
/// SQLite's own macOS VFS, and it defaults to `0`/off (verified against
/// the pinned oracle, #652) — `synchronous=FULL` alone calls plain
/// `fsync()`. Using `std`'s upgraded sync unconditionally made this
/// crate's default durability behavior *stronger* than the oracle's own
/// default, and most of the wall-clock gap between the two on
/// write-heavy benchmarks traced to exactly this call. Linux is
/// unaffected — `std`'s `sync_data` already calls plain `fdatasync`
/// there, matching SQLite's own Linux default — so this wrapper and its
/// callers are `cfg(target_os = "macos")`-only; non-macOS call sites keep
/// calling `File::sync_data` directly.
#[cfg(target_os = "macos")]
pub fn fsync(file: &File) -> io::Result<()> {
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is a valid, open descriptor for the duration of this
    // call, borrowed from `file` — the same precondition `fcntl_call`
    // above documents.
    let ret = unsafe { raw_fsync(fd) };
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// One `fcntl(F_SETLK)`/`fcntl(F_GETLK)` call: `F_SETLK(&fl)` acquires or
/// releases the byte range described by `fl` (never blocks — SQLite's
/// locking protocol is entirely non-blocking); `F_GETLK(&mut fl)` queries
/// whether some *other* process holds a conflicting lock, writing the
/// result back into `fl.l_type` (`F_UNLCK` if not).
pub enum FcntlArg<'a> {
    /// `F_SETLK`: acquire/release/downgrade the byte range in `flock`,
    /// never blocking (fails with `EAGAIN`/`EACCES` on contention).
    F_SETLK(&'a flock),
    /// `F_SETLKW`: like `F_SETLK`, but blocks until the byte range is
    /// available — test-only (`tests/helpers/lock_probe.rs`'s
    /// `holdlock` mode); SQLite's own locking protocol never blocks.
    F_SETLKW(&'a flock),
    /// `F_GETLK`: query lock state; the kernel writes the answer back into
    /// `l_type`/`l_whence`/`l_start`/`l_len`/`l_pid`.
    F_GETLK(&'a mut flock),
}

/// Safe wrapper over the vendored `fcntl` FFI call — mirrors `nix::fcntl`'s
/// call shape (`fcntl(file, FcntlArg::F_SETLK(&fl))`) so this is a drop-in
/// replacement at every call site.
pub fn fcntl_call(file: &File, arg: FcntlArg) -> io::Result<c_int> {
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is a valid, open descriptor for the lifetime of this
    // call (borrowed from `file`, which outlives the call). `F_SETLK`
    // reads `*p` as a `flock`; `F_GETLK` reads and then writes `*p` as a
    // `flock`. Both pointers are valid, properly aligned `&`/`&mut`
    // references to a real `Flock` for the duration of the call, matching
    // what the kernel's `fcntl(2)` contract requires for these commands.
    let ret = unsafe {
        match arg {
            FcntlArg::F_SETLK(fl) => fcntl(fd, F_SETLK, fl as *const flock),
            FcntlArg::F_SETLKW(fl) => fcntl(fd, F_SETLKW, fl as *const flock),
            FcntlArg::F_GETLK(fl) => fcntl(fd, F_GETLK, fl as *mut flock),
        }
    };
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn temp_file() -> (File, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "sqlite-rs-sys-fcntl-test-{}-{}",
            std::process::id(),
            n
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        (file, path)
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn fsync_succeeds_and_does_not_lose_the_write() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let (mut file, path) = temp_file();
        file.write_all(b"hello, fsync").unwrap();
        fsync(&file).unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "hello, fsync");

        std::fs::remove_file(&path).unwrap();
    }

    fn lock_at(file: &File, kind: i16, start: off_t, len: off_t) -> io::Result<()> {
        let fl = flock {
            l_start: start,
            l_len: len,
            l_pid: 0,
            l_type: kind,
            l_whence: 0, // SEEK_SET; same numeric value on macOS and Linux.
        };
        fcntl_call(file, FcntlArg::F_SETLK(&fl)).map(|_| ())
    }

    #[test]
    fn getlk_never_conflicts_with_a_lock_this_process_already_holds() {
        // POSIX record locks are scoped to `(process, inode)`, not to a
        // file descriptor — so `F_GETLK` from a second fd *in this same
        // process* must still report `F_UNLCK`, matching
        // `src/vfs/lock.rs::check_reserved_lock`'s documented contract.
        // Real cross-process contention is covered by
        // `src/vfs/lock.rs`'s subprocess-based tests.
        let (file, path) = temp_file();
        lock_at(&file, F_WRLCK, 0, 10).unwrap();

        let second = OpenOptions::new().write(true).open(&path).unwrap();
        let mut probe = flock {
            l_start: 0,
            l_len: 10,
            l_pid: 0,
            l_type: F_WRLCK,
            l_whence: 0,
        };
        fcntl_call(&second, FcntlArg::F_GETLK(&mut probe)).unwrap();
        assert_eq!(probe.l_type, F_UNLCK);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn unlock_releases_the_range() {
        let (file, path) = temp_file();
        lock_at(&file, F_WRLCK, 0, 10).unwrap();
        lock_at(&file, F_UNLCK, 0, 10).unwrap();

        let mut probe = flock {
            l_start: 0,
            l_len: 10,
            l_pid: 0,
            l_type: F_WRLCK,
            l_whence: 0,
        };
        fcntl_call(&file, FcntlArg::F_GETLK(&mut probe)).unwrap();
        assert_eq!(
            probe.l_type, F_UNLCK,
            "a process never conflicts with its own released lock"
        );

        std::fs::remove_file(&path).unwrap();
    }
}
