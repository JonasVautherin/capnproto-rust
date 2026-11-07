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

//! Messages with file descriptors attached, as Unix sockets pass them (`SCM_RIGHTS`).
//!
//! A stream that can carry descriptors implements [`AsyncFdRead`] and [`AsyncFdWrite`]; this
//! crate depends on no runtime, so the implementation for a runtime's Unix socket is the
//! application's (`capnp-rpc/examples/fd-passing` has one for Tokio). The descriptors of a
//! message go with its first byte. Reads never run past the message being read, so the
//! descriptors that arrive while reading a message are that message's: an operating system that
//! passes descriptors on a stream socket stops a read at the end of the bytes they came with.

use std::future::Future;
use std::io;
use std::os::fd::{BorrowedFd, OwnedFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{AsyncRead, StreamExt};

use capnp::serialize::OwnedSegments;
use capnp::{message, Result};

use crate::serialize::AsOutputSegments;
use crate::write_queue::{Item, Sender};

/// A byte stream that may receive file descriptors with its bytes.
pub trait AsyncFdRead {
    /// As `futures::AsyncRead::poll_read`, and the descriptors that came with the bytes read are
    /// pushed onto `fds` (an implementation may close any beyond a limit of its own).
    fn poll_read_with_fds(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        fds: &mut Vec<OwnedFd>,
    ) -> Poll<io::Result<usize>>;
}

/// A byte stream that may send file descriptors with its bytes.
pub trait AsyncFdWrite {
    /// As `futures::AsyncWrite::poll_write`; when it writes anything, `fds` went with the first
    /// byte written. The caller passes them again until something was written.
    fn poll_write_with_fds(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> Poll<io::Result<usize>>;

    /// As `futures::AsyncWrite::poll_flush`.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

impl<T: AsyncFdRead + Unpin + ?Sized> AsyncFdRead for &mut T {
    fn poll_read_with_fds(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        fds: &mut Vec<OwnedFd>,
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_read_with_fds(cx, buf, fds)
    }
}

impl<T: AsyncFdRead + Unpin + ?Sized> AsyncFdRead for Box<T> {
    fn poll_read_with_fds(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        fds: &mut Vec<OwnedFd>,
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_read_with_fds(cx, buf, fds)
    }
}

impl<T: AsyncFdWrite + Unpin + ?Sized> AsyncFdWrite for &mut T {
    fn poll_write_with_fds(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_write_with_fds(cx, buf, fds)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut **self).poll_flush(cx)
    }
}

impl<T: AsyncFdWrite + Unpin + ?Sized> AsyncFdWrite for Box<T> {
    fn poll_write_with_fds(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut **self).poll_write_with_fds(cx, buf, fds)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut **self).poll_flush(cx)
    }
}

/// An [`AsyncFdRead`] read as a plain `AsyncRead`, keeping the descriptors that come with the
/// bytes until [`take_fds()`](Self::take_fds). Only for readers that never read past the message
/// they read, as this crate's `serialize::read_message()`: a buffered reader would mix up the
/// descriptors of the messages it reads ahead.
pub struct ReadFds<R> {
    inner: R,
    fds: Vec<OwnedFd>,
}

impl<R> ReadFds<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            fds: Vec::new(),
        }
    }

    /// The descriptors received since the last call.
    pub fn take_fds(&mut self) -> Vec<OwnedFd> {
        std::mem::take(&mut self.fds)
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncFdRead + Unpin> AsyncRead for ReadFds<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_read_with_fds(cx, buf, &mut this.fds)
    }
}

/// Reads a message and the descriptors that came with it, at most `max_fds` of them (those beyond
/// are closed). `None` at the end of the stream, before a message starts.
pub async fn try_read_message<R>(
    reader: R,
    options: message::ReaderOptions,
    max_fds: usize,
) -> Result<Option<(message::Reader<OwnedSegments>, Vec<OwnedFd>)>>
where
    R: AsyncFdRead + Unpin,
{
    let mut reader = ReadFds::new(reader);
    let message = crate::serialize::try_read_message(&mut reader, options).await?;
    let mut fds = reader.take_fds();
    fds.truncate(max_fds);
    Ok(message.map(|m| (m, fds)))
}

/// Writes a message with `fds` attached to its first byte, then flushes. The message goes in as
/// few writes as the stream takes: one where its buffer allows.
pub async fn write_message<W, M>(mut writer: W, message: M, fds: &[BorrowedFd<'_>]) -> Result<()>
where
    W: AsyncFdWrite + Unpin,
    M: AsOutputSegments,
{
    let mut bytes = Vec::new();
    crate::serialize::write_message(&mut bytes, message).await?;
    let mut written = 0;
    while written < bytes.len() {
        let attached = if written == 0 { fds } else { &[] };
        let n = futures_util::future::poll_fn(|cx| {
            Pin::new(&mut writer).poll_write_with_fds(cx, &bytes[written..], attached)
        })
        .await?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        written += n;
    }
    futures_util::future::poll_fn(|cx| Pin::new(&mut writer).poll_flush(cx)).await?;
    Ok(())
}

/// An outgoing message that may have file descriptors attached (`write_queue()`).
pub trait AttachedFds {
    /// The descriptors to send with the message, in the order the receiver numbers them.
    fn attached_fds(&self) -> Vec<BorrowedFd<'_>>;
}

/// As `crate::write_queue()`, over a stream that passes file descriptors: each message is
/// written with its own (`AttachedFds`).
pub fn write_queue<W, M>(mut writer: W) -> (Sender<M>, impl Future<Output = Result<()>>)
where
    W: AsyncFdWrite + Unpin,
    M: AsOutputSegments + AttachedFds,
{
    let (sender, mut rx, in_flight) = Sender::<M>::new();
    let queue = async move {
        while let Some(item) = rx.next().await {
            match item {
                Item::Message(m, returner) => {
                    let result = write_message(&mut writer, &m, &m.attached_fds()).await;
                    in_flight.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    result?;
                    let _ = returner.send(m);
                }
                Item::Done(r, finisher) => {
                    let _ = finisher.send(());
                    return r;
                }
            }
        }
        Ok(())
    };
    (sender, queue)
}

impl AsyncFdWrite for Vec<u8> {
    /// Collects the bytes; the descriptors are dropped (for tests and buffers).
    fn poll_write_with_fds(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
        _fds: &[BorrowedFd<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::os::fd::AsFd;

    /// An in-memory stream: what is written is read back, descriptors at the offset of the
    /// first byte they were written with, and a read stops after those bytes' write, as Linux
    /// stops a read at the end of the bytes descriptors came with.
    #[derive(Default)]
    struct Pipe {
        bytes: Vec<u8>,
        /// (offset of a write's first byte, its end, its descriptors)
        writes: Vec<(usize, usize, Vec<OwnedFd>)>,
        read: usize,
        /// Bytes per read, to exercise partial reads.
        chunk: usize,
    }

    impl AsyncFdWrite for Pipe {
        fn poll_write_with_fds(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
            fds: &[BorrowedFd<'_>],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let start = this.bytes.len();
            this.bytes.extend_from_slice(buf);
            let owned = fds
                .iter()
                .map(|f| f.try_clone_to_owned().unwrap())
                .collect();
            this.writes.push((start, this.bytes.len(), owned));
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncFdRead for Pipe {
        fn poll_read_with_fds(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
            fds: &mut Vec<OwnedFd>,
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let mut end = (this.read + buf.len().min(this.chunk.max(1))).min(this.bytes.len());
            // Descriptors come with the read that reaches their write's first byte, and that
            // read goes no further than their write's end.
            for (start, write_end, attached) in &mut this.writes {
                if *start >= this.read && *start < end && !attached.is_empty() {
                    end = end.min(*write_end);
                    fds.append(attached);
                }
            }
            let n = end - this.read;
            buf[..n].copy_from_slice(&this.bytes[this.read..end]);
            this.read = end;
            Poll::Ready(Ok(n))
        }
    }

    fn message(text: &str) -> message::Builder<message::HeapAllocator> {
        let mut m = message::Builder::new_default();
        m.set_root(text).unwrap();
        m
    }

    #[test]
    fn descriptors_stay_with_their_message() {
        let (a, _a_peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let (b, _b_peer) = std::os::unix::net::UnixStream::pair().unwrap();
        for chunk in [3, 8, 1 << 20] {
            let mut pipe = Pipe {
                chunk,
                ..Pipe::default()
            };
            block_on(async {
                write_message(&mut pipe, message("one"), &[a.as_fd(), b.as_fd()])
                    .await
                    .unwrap();
                write_message(&mut pipe, message("two"), &[]).await.unwrap();
                write_message(&mut pipe, message("three"), &[b.as_fd()])
                    .await
                    .unwrap();
                let opts = message::ReaderOptions::new();
                let (m, fds) = try_read_message(&mut pipe, opts, 8).await.unwrap().unwrap();
                assert_eq!(m.get_root::<capnp::text::Reader>().unwrap(), "one");
                assert_eq!(fds.len(), 2);
                let (m, fds) = try_read_message(&mut pipe, opts, 8).await.unwrap().unwrap();
                assert_eq!(m.get_root::<capnp::text::Reader>().unwrap(), "two");
                assert!(fds.is_empty());
                // At most `max_fds`: the others closed.
                let (m, fds) = try_read_message(&mut pipe, opts, 0).await.unwrap().unwrap();
                assert_eq!(m.get_root::<capnp::text::Reader>().unwrap(), "three");
                assert!(fds.is_empty());
                assert!(try_read_message(&mut pipe, opts, 8)
                    .await
                    .unwrap()
                    .is_none());
            });
        }
    }

    struct WithFds(message::Builder<message::HeapAllocator>, Vec<OwnedFd>);

    impl AsOutputSegments for WithFds {
        fn as_output_segments(&self) -> capnp::OutputSegments<'_> {
            self.0.get_segments_for_output()
        }
    }

    impl AttachedFds for WithFds {
        fn attached_fds(&self) -> Vec<BorrowedFd<'_>> {
            self.1.iter().map(|f| f.as_fd()).collect()
        }
    }

    #[test]
    fn queue_sends_each_message_with_its_descriptors() {
        let (a, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut pipe = Pipe {
            chunk: 1 << 20,
            ..Pipe::default()
        };
        block_on(async {
            let (mut sender, queue) = write_queue(&mut pipe);
            let first = sender.send(WithFds(message("a"), vec![a.into()]));
            let second = sender.send(WithFds(message("b"), vec![]));
            let done = sender.terminate(Ok(()));
            queue.await.unwrap();
            first.await.unwrap();
            second.await.unwrap();
            done.await.unwrap();
        });
        let opts = message::ReaderOptions::new();
        block_on(async {
            let (_, fds) = try_read_message(&mut pipe, opts, 8).await.unwrap().unwrap();
            assert_eq!(fds.len(), 1);
            let (_, fds) = try_read_message(&mut pipe, opts, 8).await.unwrap().unwrap();
            assert!(fds.is_empty());
        });
    }

    #[test]
    fn a_write_of_nothing_fails() {
        struct Stuck;
        impl AsyncFdWrite for Stuck {
            fn poll_write_with_fds(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &[u8],
                _: &[BorrowedFd<'_>],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(Ok(0))
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        assert!(block_on(write_message(Stuck, message("x"), &[])).is_err());
    }
}
