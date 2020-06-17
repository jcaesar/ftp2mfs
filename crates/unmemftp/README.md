# Ummm... unmemFTP!

A quick implementation of a `StorageBackend` for [unFTP](https://crates.io/crates/libunftp) that returns static files from memory — Is intended for testing things that access FTP, and code-quality-wise, you probably don't want to use it outside of your `dev-dependencies`.

See the [tests](./tests/test.rs) for simple usage examples.

Currently only supports reads. Implementing writes may require implementing some sharing mechanism between different instances of the storage backend, but shouldn't be too difficult.
