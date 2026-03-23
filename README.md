# lsp-server-tokio

An async-first Rust crate for building Language Server Protocol (LSP) servers with Tokio.

`lsp-server-tokio` provides transport-agnostic infrastructure for JSON-RPC/LSP servers, including framed I/O, request tracking, lifecycle enforcement, cancellation, and routing helpers.

## Features

- Async LSP transport built on Tokio
- `Connection` API with split sender and receiver halves
- `StdioConnection` alias for ergonomic stdio-based servers
- `LspCodec` for `Content-Length` framed messages
- `RequestQueue` utilities for incoming and outgoing request tracking
- Lifecycle helpers for initialize, shutdown, and exit handling
- In-memory duplex transport for tests and examples

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
lsp-server-tokio = "0.1.0"
```

## Example

```rust
use futures::{SinkExt, StreamExt};
use lsp_server_tokio::{duplex_transport, Message, Request, Response};

# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
let (mut client, mut server) = duplex_transport(4096);

client
    .send(Message::Request(Request::new(1, "textDocument/hover", None)))
    .await
    .unwrap();

if let Some(Ok(Message::Request(request))) = server.next().await {
    server
        .send(Message::Response(Response::ok(
            request.id,
            serde_json::json!({"contents": "Hello from lsp-server-tokio"}),
        )))
        .await
        .unwrap();
}
# });
```

## License

Licensed under either of:

- MIT license
- Apache License, Version 2.0
