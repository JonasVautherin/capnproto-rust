@0xb40445b76d5d6883;

interface Files {
  open @0 (name :Text) -> (file :File);
  # The file `name`, opened for reading by the server.
}

interface File {
  # Its file descriptor comes with it (capnp_rpc::get_fd()).
}
