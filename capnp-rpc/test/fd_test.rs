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

//! File descriptors passed with capabilities, over `async_byte_channel`, which passes them as a
//! Unix socket does.

use std::cell::RefCell;
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::rc::Rc;

use capnp::Error;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::channel::oneshot;
use futures::Future;

use crate::test_capnp::{test_fd, test_handle};

struct Handle;

impl test_handle::Server for Handle {}

#[derive(Default)]
struct TestFd {
    /// The server's end of the socket pair behind `make` and `later`.
    peer: RefCell<Option<UnixStream>>,
    /// Resolves `later`'s promise.
    resolver: RefCell<Option<oneshot::Sender<()>>>,
}

impl TestFd {
    fn make(&self) -> Result<test_handle::Client, Error> {
        let (ours, theirs) = UnixStream::pair()?;
        *self.peer.borrow_mut() = Some(ours);
        Ok(capnp_rpc::new_client_with_fd(Handle, theirs))
    }
}

impl test_fd::Server for TestFd {
    async fn echo(
        self: Rc<Self>,
        params: test_fd::EchoParams,
        mut results: test_fd::EchoResults,
    ) -> Result<(), Error> {
        let cap = params.get()?.get_cap()?;
        let fd = capnp_rpc::get_fd(&cap).await?;
        if let Some(fd) = &fd {
            UnixStream::from(fd.as_fd().try_clone_to_owned()?).write_all(b"x")?;
        }
        results.get().set_had_fd(fd.is_some());
        results.get().set_cap(cap);
        Ok(())
    }

    async fn make(
        self: Rc<Self>,
        _: test_fd::MakeParams,
        mut results: test_fd::MakeResults,
    ) -> Result<(), Error> {
        results.get().set_cap(TestFd::make(&self)?);
        Ok(())
    }

    async fn later(
        self: Rc<Self>,
        _: test_fd::LaterParams,
        mut results: test_fd::LaterResults,
    ) -> Result<(), Error> {
        let (resolver, resolved) = oneshot::channel();
        *self.resolver.borrow_mut() = Some(resolver);
        let this = self.clone();
        results
            .get()
            .set_cap(capnp_rpc::new_future_client(async move {
                resolved
                    .await
                    .map_err(|_| Error::failed("never resolved".into()))?;
                TestFd::make(&this)
            }));
        Ok(())
    }

    async fn resolve(
        self: Rc<Self>,
        _: test_fd::ResolveParams,
        _: test_fd::ResolveResults,
    ) -> Result<(), Error> {
        if let Some(resolver) = self.resolver.borrow_mut().take() {
            let _ = resolver.send(());
        }
        Ok(())
    }

    async fn check(
        self: Rc<Self>,
        _: test_fd::CheckParams,
        mut results: test_fd::CheckResults,
    ) -> Result<(), Error> {
        let mut byte = [0u8];
        let mut peer = self.peer.borrow_mut();
        let peer = peer
            .as_mut()
            .ok_or_else(|| Error::failed("nothing made".into()))?;
        peer.read_exact(&mut byte)?;
        results.get().set_byte(byte[0]);
        Ok(())
    }
}

/// A client and a server over a pair of channels, each side passing descriptors or not
/// (`Some(max per message)` or `None`).
fn fd_top_level<F, G>(client_fds: Option<usize>, server_fds: Option<usize>, main: F)
where
    F: FnOnce(test_fd::Client) -> G,
    G: Future<Output = Result<(), Error>> + 'static,
{
    fn network(
        reader: async_byte_channel::Receiver,
        writer: async_byte_channel::Sender,
        fds: Option<usize>,
        side: rpc_twoparty_capnp::Side,
    ) -> Box<dyn capnp_rpc::VatNetwork<rpc_twoparty_capnp::Side>> {
        match fds {
            Some(max) => Box::new(twoparty::VatNetwork::new_with_fds(
                reader,
                writer,
                max,
                side,
                Default::default(),
            )),
            None => Box::new(twoparty::VatNetwork::new(
                reader,
                writer,
                side,
                Default::default(),
            )),
        }
    }

    let (client_writer, server_reader) = async_byte_channel::channel();
    let (server_writer, client_reader) = async_byte_channel::channel();
    let server = std::thread::spawn(move || {
        let net = network(
            server_reader,
            server_writer,
            server_fds,
            rpc_twoparty_capnp::Side::Server,
        );
        let bootstrap: test_fd::Client = capnp_rpc::new_client(TestFd::default());
        futures::executor::block_on(RpcSystem::new(net, Some(bootstrap.client))).unwrap();
    });

    let mut pool = futures::executor::LocalPool::new();
    let mut spawner = pool.spawner();
    let net = network(
        client_reader,
        client_writer,
        client_fds,
        rpc_twoparty_capnp::Side::Client,
    );
    let mut rpc_system = RpcSystem::new(net, None);
    let client: test_fd::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    let disconnector = rpc_system.get_disconnector();
    crate::spawn(&mut spawner, rpc_system);
    pool.run_until(main(client)).unwrap();
    pool.run_until(disconnector).unwrap();
    server.join().unwrap();
}

#[test]
fn a_capability_goes_with_its_descriptor() {
    fd_top_level(Some(4), Some(4), |client| async move {
        let (ours, mut peer) = UnixStream::pair()?;
        let handle: test_handle::Client = capnp_rpc::new_client_with_fd(Handle, ours);
        let mut request = client.echo_request();
        request.get().set_cap(handle);
        let response = request.send().promise.await?;
        assert!(response.get()?.get_had_fd());
        let mut byte = [0u8];
        peer.read_exact(&mut byte)?;
        assert_eq!(&byte, b"x");
        // Back home it is our own capability again, with its descriptor.
        let back = response.get()?.get_cap()?;
        assert!(capnp_rpc::get_fd(&back).await?.is_some());
        Ok(())
    });
}

#[test]
fn a_capability_comes_with_its_descriptor() {
    fd_top_level(Some(4), Some(4), |client| async move {
        let response = client.make_request().send().promise.await?;
        let cap = response.get()?.get_cap()?;
        let fd = capnp_rpc::get_fd(&cap).await?.expect("a descriptor");
        UnixStream::from(fd.as_fd().try_clone_to_owned()?).write_all(b"y")?;
        let checked = client.check_request().send().promise.await?;
        assert_eq!(checked.get()?.get_byte(), b'y');
        Ok(())
    });
}

#[test]
fn a_promise_resolves_with_its_descriptor() {
    fd_top_level(Some(4), Some(4), |client| async move {
        let response = client.later_request().send().promise.await?;
        let cap = response.get()?.get_cap()?;
        // Not resolved yet: the descriptor comes with the `Resolve` message.
        let fd = capnp_rpc::get_fd(&cap);
        client.resolve_request().send().promise.await?;
        let fd = fd.await?.expect("a descriptor");
        UnixStream::from(fd.as_fd().try_clone_to_owned()?).write_all(b"z")?;
        let checked = client.check_request().send().promise.await?;
        assert_eq!(checked.get()?.get_byte(), b'z');
        Ok(())
    });
}

#[test]
fn receiving_none_still_receives_the_capability() {
    fd_top_level(Some(0), Some(4), |client| async move {
        let response = client.make_request().send().promise.await?;
        let cap = response.get()?.get_cap()?;
        assert!(capnp_rpc::get_fd(&cap).await?.is_none());
        Ok(())
    });
}

#[test]
fn a_connection_without_descriptors_sends_the_capability_alone() {
    fd_top_level(Some(4), None, |client| async move {
        let (ours, _peer) = UnixStream::pair()?;
        let handle: test_handle::Client = capnp_rpc::new_client_with_fd(Handle, ours);
        let mut request = client.echo_request();
        request.get().set_cap(handle);
        let response = request.send().promise.await?;
        assert!(!response.get()?.get_had_fd());
        Ok(())
    });
}
