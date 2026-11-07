// Copyright (c) 2026 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//! File descriptors passed with capabilities over a Unix socket: the server opens a file and
//! returns it as a capability with the file's descriptor attached; the client reads the file
//! through that descriptor.
//!
//!   fd-passing server <socket path>
//!   fd-passing client <socket path> <file>
//!
//! `FdSocket` makes a Tokio Unix socket one that capnp-futures can pass descriptors over; an
//! application using Tokio can copy it.

use std::io::{self, IoSlice, IoSliceMut, Read};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{ready, Context, Poll};

use capnp_futures::fd::{AsyncFdRead, AsyncFdWrite};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use rustix::net::{
    recvmsg, sendmsg, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags,
};
use tokio::io::Interest;
use tokio::net::UnixStream;

capnp::generated_code!(pub mod files_capnp);

use files_capnp::{file, files};

/// At most this many descriptors with a message: Linux's limit (`SCM_MAX_FD`).
const MAX_FDS: usize = 253;

/// A Tokio Unix socket, shared by the network's reading and writing halves.
#[derive(Clone)]
struct FdSocket(Rc<UnixStream>);

impl FdSocket {
    fn new(socket: UnixStream) -> io::Result<Self> {
        // A write to a closed socket fails with EPIPE rather than killing the process. Elsewhere
        // `SendFlags::NOSIGNAL` does it per write; Apple's systems have a socket option instead.
        #[cfg(target_vendor = "apple")]
        rustix::net::sockopt::set_socket_nosigpipe(&socket, true)?;
        Ok(Self(Rc::new(socket)))
    }
}

impl AsyncFdRead for FdSocket {
    fn poll_read_with_fds(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        fds: &mut Vec<OwnedFd>,
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.0.poll_read_ready(cx))?;
            match self.0.try_io(Interest::READABLE, || {
                // Room for the most descriptors a message may bring: a smaller buffer would have
                // the kernel close (Linux) or leak (macOS) those that don't fit.
                let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
                let mut ancillary = RecvAncillaryBuffer::new(&mut space);
                // Received close-on-exec, so that no child process inherits them; atomically
                // where the system can.
                #[cfg(any(target_os = "linux", target_os = "android"))]
                let flags = RecvFlags::DONTWAIT | RecvFlags::CMSG_CLOEXEC;
                #[cfg(not(any(target_os = "linux", target_os = "android")))]
                let flags = RecvFlags::DONTWAIT;
                let received = recvmsg(
                    self.0.as_ref(),
                    &mut [IoSliceMut::new(buf)],
                    &mut ancillary,
                    flags,
                )?;
                for message in ancillary.drain() {
                    if let RecvAncillaryMessage::ScmRights(received_fds) = message {
                        for fd in received_fds {
                            #[cfg(not(any(target_os = "linux", target_os = "android")))]
                            rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)?;
                            fds.push(fd);
                        }
                    }
                }
                Ok(received.bytes)
            }) {
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue
                }
                result => return Poll::Ready(result),
            }
        }
    }
}

impl AsyncFdWrite for FdSocket {
    fn poll_write_with_fds(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.0.poll_write_ready(cx))?;
            match self.0.try_io(Interest::WRITABLE, || {
                // Built again for each try: `sendmsg` may consume it.
                let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
                let mut ancillary = SendAncillaryBuffer::new(&mut space);
                if !fds.is_empty() && !ancillary.push(SendAncillaryMessage::ScmRights(fds)) {
                    return Err(io::Error::other(
                        "too many file descriptors for one message",
                    ));
                }
                #[cfg(not(target_vendor = "apple"))]
                let flags = SendFlags::DONTWAIT | SendFlags::NOSIGNAL;
                #[cfg(target_vendor = "apple")]
                let flags = SendFlags::DONTWAIT;
                Ok(sendmsg(
                    self.0.as_ref(),
                    &[IoSlice::new(buf)],
                    &mut ancillary,
                    flags,
                )?)
            }) {
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue
                }
                result => return Poll::Ready(result),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// The two-party network over `socket`, passing descriptors both ways.
fn network(
    socket: UnixStream,
    side: rpc_twoparty_capnp::Side,
) -> io::Result<Box<twoparty::VatNetwork<capnp_futures::fd::ReadFds<FdSocket>>>> {
    let socket = FdSocket::new(socket)?;
    Ok(Box::new(twoparty::VatNetwork::new_with_fds(
        socket.clone(),
        socket,
        4,
        side,
        Default::default(),
    )))
}

struct FilesImpl;

impl files::Server for FilesImpl {
    async fn open(
        self: Rc<Self>,
        params: files::OpenParams,
        mut results: files::OpenResults,
    ) -> Result<(), capnp::Error> {
        let name = params.get()?.get_name()?.to_str()?;
        let opened = std::fs::File::open(name)?;
        results
            .get()
            .set_file(capnp_rpc::new_client_with_fd(FileImpl, opened));
        Ok(())
    }
}

struct FileImpl;

impl file::Server for FileImpl {}

async fn server(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::UnixListener::bind(path)?;
    let files: files::Client = capnp_rpc::new_client(FilesImpl);
    loop {
        let (socket, _) = listener.accept().await?;
        let network = network(socket, rpc_twoparty_capnp::Side::Server)?;
        let rpc_system = RpcSystem::new(network, Some(files.clone().client));
        tokio::task::spawn_local(rpc_system);
    }
}

async fn client(path: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let network = network(
        UnixStream::connect(path).await?,
        rpc_twoparty_capnp::Side::Client,
    )?;
    let mut rpc_system = RpcSystem::new(network, None);
    let files: files::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(rpc_system);

    let mut request = files.open_request();
    request.get().set_name(name);
    let reply = request.send().promise.await?;
    let file = reply.get()?.get_file()?;
    let fd = capnp_rpc::get_fd(&file)
        .await?
        .ok_or("the file came without its descriptor")?;
    let mut text = String::new();
    std::fs::File::from(fd.as_fd().try_clone_to_owned()?).read_to_string(&mut text)?;
    print!("{text}");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let local = tokio::task::LocalSet::new();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[1..] {
        ["server", path] => local.run_until(server(path)).await,
        ["client", path, name] => local.run_until(client(path, name)).await,
        _ => {
            eprintln!(
                "usage: {} server <socket path> | client <socket path> <file>",
                args[0]
            );
            Ok(())
        }
    }
}
