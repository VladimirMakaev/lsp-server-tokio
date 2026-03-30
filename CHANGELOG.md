# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-03-30

### Added
- `ClientSender` for cloneable, non-blocking server→client communication
- `Connection::client_sender()` to create a `ClientSender` handle from an initialized connection

### Changed
- `Connection.sender` access now goes through the `Connection::sender()` method

### Deprecated
- `Connection::send_request()` in favor of `ClientSender::request()`

## [0.1.0] - 2026-03-23

### Added
- Async LSP server infrastructure using Tokio
- `Connection` type with split sender/receiver for concurrent I/O
- `StdioConnection` type alias for ergonomic stdio-based servers
- `LspCodec` for Content-Length message framing
- `Transport` and `duplex_transport()` for transport-agnostic I/O
- Full LSP lifecycle state machine (Uninitialized → Running → ShuttingDown → Exited)
- `RequestQueue` with `IncomingRequests` and `OutgoingRequests` tracking
- Cooperative cancellation via `CancellationToken` per request
- `IncomingMessage` routing with auto-registration
- 60-second timeout on `initialize_finish` for robustness
- Complete set of LSP and JSON-RPC error codes
- Example formatter server with E2E tests
