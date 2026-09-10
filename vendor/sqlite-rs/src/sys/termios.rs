// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Vendored terminal raw-mode FFI (#563) — replaces `nix::sys::termios`.
//! `tcgetattr`/`tcsetattr`/`cfmakeraw` are ordinary exported libc symbols
//! on both macOS and Linux (no raw `ioctl` numbers to hand-roll), so the
//! only platform-specific piece is `struct termios`'s field layout —
//! verified against each platform's own headers (macOS's
//! `<sys/termios.h>`, glibc's `bits/termios.h`).
#![allow(unsafe_code)]
// Field names below mirror the platform ABI (C struct field names)
// exactly, on purpose — inventing prose docs for `c_iflag`/etc. would just
// restate the name.
#![allow(missing_docs)]
// `SetArg`'s variants and the `termios` alias mirror the C/POSIX names
// (`TCSANOW`, `struct termios`) exactly, matching `nix`'s own naming at
// the API this replaces.
#![allow(non_camel_case_types)]

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::raw::c_int;

#[cfg(target_os = "macos")]
mod abi {
    /// `<sys/termios.h>`: `tcflag_t`/`speed_t` are `unsigned long` (64-bit)
    /// on macOS; `cc_t` is `unsigned char`. `NCCS` is 20.
    pub type TcflagT = u64;
    pub type CcT = u8;
    pub type SpeedT = u64;
    pub const NCCS: usize = 20;

    /// `<sys/termios.h>`'s `struct termios` — field order matches exactly.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Termios {
        pub c_iflag: TcflagT,
        pub c_oflag: TcflagT,
        pub c_cflag: TcflagT,
        pub c_lflag: TcflagT,
        pub c_cc: [CcT; NCCS],
        pub c_ispeed: SpeedT,
        pub c_ospeed: SpeedT,
    }
}

#[cfg(target_os = "linux")]
mod abi {
    /// glibc `bits/termios.h`: `tcflag_t`/`speed_t` are `unsigned int`
    /// (32-bit); `cc_t` is `unsigned char`. `NCCS` is 32, and glibc's
    /// struct carries a `c_line` field before `c_cc` that macOS's does
    /// not.
    pub type TcflagT = u32;
    pub type CcT = u8;
    pub type SpeedT = u32;
    pub const NCCS: usize = 32;

    /// glibc `bits/termios.h`'s `struct termios` — field order matches
    /// exactly, including the `c_line` field absent on macOS.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Termios {
        pub c_iflag: TcflagT,
        pub c_oflag: TcflagT,
        pub c_cflag: TcflagT,
        pub c_lflag: TcflagT,
        pub c_line: CcT,
        pub c_cc: [CcT; NCCS],
        pub c_ispeed: SpeedT,
        pub c_ospeed: SpeedT,
    }
}

pub use abi::Termios as termios;

/// `TCSANOW`/`TCSAFLUSH` share the same numeric value on macOS and Linux.
pub const TCSANOW: c_int = 0;
pub const TCSAFLUSH: c_int = 2;

extern "C" {
    fn tcgetattr(fd: c_int, termios_p: *mut termios) -> c_int;
    fn tcsetattr(fd: c_int, optional_actions: c_int, termios_p: *const termios) -> c_int;
    fn cfmakeraw(termios_p: *mut termios);
    fn isatty(fd: c_int) -> c_int;
}

/// Borrows the process's stdin descriptor (fd 0) for the `'static`
/// lifetime, without going through `io::stdin()`'s handle/lock.
pub fn stdin_fd() -> BorrowedFd<'static> {
    // SAFETY: fd 0 is stdin on POSIX; it is open for the lifetime of the
    // process and never closed by this crate, so the borrow is valid for
    // `'static` and never aliases an owned descriptor we could close.
    unsafe { BorrowedFd::borrow_raw(0) }
}

/// Whether `fd` refers to a terminal — POSIX `isatty(3)`.
pub fn is_tty(fd: BorrowedFd) -> bool {
    // SAFETY: `fd` is a valid, open descriptor for the duration of this
    // call.
    unsafe { isatty(fd.as_raw_fd()) == 1 }
}

/// Which change-timing semantics [`tcsetattr`] applies — mirrors
/// `nix::sys::termios::SetArg`'s call shape at every call site.
pub enum SetArg {
    /// `TCSANOW`: apply the change immediately.
    TCSANOW,
    /// `TCSAFLUSH`: drain pending output, discard pending input, then
    /// apply the change.
    TCSAFLUSH,
}

/// Reads `fd`'s current terminal attributes.
pub fn tcgetattr_call(fd: BorrowedFd) -> io::Result<termios> {
    let mut t: termios = unsafe { std::mem::zeroed() };
    // SAFETY: `fd` is a valid, open descriptor for the duration of this
    // call (borrowed for the call only). `t` is a valid, aligned
    // `*mut termios` the kernel/libc fills in on success; on failure it
    // may be left partially written, which is fine — it is discarded by
    // every caller here.
    let ret = unsafe { tcgetattr(fd.as_raw_fd(), &mut t) };
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(t)
    }
}

/// Applies `t` to `fd` per `action`'s timing.
pub fn tcsetattr_call(fd: BorrowedFd, action: SetArg, t: &termios) -> io::Result<()> {
    let action = match action {
        SetArg::TCSANOW => TCSANOW,
        SetArg::TCSAFLUSH => TCSAFLUSH,
    };
    // SAFETY: `fd` is a valid, open descriptor for the duration of this
    // call. `t` is a valid `&termios` the libc call only reads.
    let ret = unsafe { tcsetattr(fd.as_raw_fd(), action, t) };
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Mutates `t` in place into "raw mode" (no line buffering, no signal
/// generation, no echo) — the same transformation as POSIX/BSD's
/// `cfmakeraw(3)`, called directly rather than reimplemented: it is a
/// fixed, well-known bit-flag transform with no OS state to query, so the
/// libc symbol is exactly as trustworthy as hand-rolling the same flags.
pub fn cfmakeraw_call(t: &mut termios) {
    // SAFETY: `t` is a valid, aligned `&mut termios` for the duration of
    // this call.
    unsafe { cfmakeraw(t) }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::os::fd::AsFd;

    use super::*;

    // `ICANON`/`ECHO` (`c_lflag` bits `cfmakeraw` must clear) are at
    // different bit positions on macOS/BSD vs. Linux — `ECHO` happens to
    // coincide (0x0008) but `ICANON` does not, so this is `cfg`-gated
    // rather than one shared constant.
    #[cfg(target_os = "macos")]
    const ICANON: u64 = 0x0100;
    #[cfg(target_os = "linux")]
    const ICANON: u64 = 0x0002;
    const ECHO: u64 = 0x0008;

    // `tcgetattr`/`tcsetattr` need a real tty; `cfmakeraw` needs neither,
    // so it is the one primitive here testable without one (CI has no
    // controlling tty).
    #[test]
    fn cfmakeraw_clears_canonical_and_echo_flags() {
        let mut t: termios = unsafe { std::mem::zeroed() };
        t.c_lflag = 0xFFFF_FFFF_FFFF_FFFFu64 as _;
        cfmakeraw_call(&mut t);
        assert_eq!(t.c_lflag as u64 & ICANON, 0, "ICANON must be cleared");
        assert_eq!(t.c_lflag as u64 & ECHO, 0, "ECHO must be cleared");
    }

    #[test]
    fn stdin_fd_borrows_fd_zero() {
        assert_eq!(stdin_fd().as_raw_fd(), 0);
    }

    // A regular file's fd is valid but not a tty, so `tcgetattr`/
    // `tcsetattr` fail with `ENOTTY` — exercising the `ret == -1` error
    // branch without needing a real controlling tty.
    #[test]
    fn tcgetattr_call_errors_on_non_tty_fd() {
        let file = std::fs::File::open("Cargo.toml").unwrap();
        let fd = file.as_fd();
        assert!(tcgetattr_call(fd).is_err());
    }

    #[test]
    fn tcsetattr_call_errors_on_non_tty_fd() {
        let file = std::fs::File::open("Cargo.toml").unwrap();
        let fd = file.as_fd();
        let t: termios = unsafe { std::mem::zeroed() };
        assert!(tcsetattr_call(fd, SetArg::TCSANOW, &t).is_err());
        assert!(tcsetattr_call(fd, SetArg::TCSAFLUSH, &t).is_err());
    }

    #[test]
    fn is_tty_false_on_non_tty_fd() {
        let file = std::fs::File::open("Cargo.toml").unwrap();
        assert!(!is_tty(file.as_fd()));
    }
}
